"""
API for Tuya Local devices.
"""

import asyncio
import logging
from asyncio.exceptions import CancelledError
from selectors import EVENT_READ, DefaultSelector
from threading import Lock, RLock
from time import time
from weakref import WeakKeyDictionary

import tinytuya
from homeassistant.const import (
    CONF_HOST,
    CONF_NAME,
    EVENT_HOMEASSISTANT_STARTED,
    EVENT_HOMEASSISTANT_STOP,
)
from homeassistant.core import HomeAssistant, callback
from tinytuya.core import command_types as CT

from .const import (
    API_PROTOCOL_VERSIONS,
    CONF_DEVICE_CID,
    CONF_DEVICE_ID,
    CONF_LOCAL_KEY,
    CONF_MANUFACTURER,
    CONF_MODEL,
    CONF_POLL_ONLY,
    CONF_PROTOCOL_VERSION,
    DOMAIN,
)
from .helpers.config import get_device_id
from .helpers.device_config import possible_matches
from .helpers.log import log_json

_LOGGER = logging.getLogger(__name__)
_ROUTING_LOGGER = logging.getLogger(f"{__name__}.routing")
_GATEWAY_SETUP_LOCKS = WeakKeyDictionary()
_GATEWAY_SETUP_LOCKS_LOCK = Lock()


def _gateway_setup_lock(hass, dev_id):
    """Return the setup lock for one gateway on one HA instance."""
    with _GATEWAY_SETUP_LOCKS_LOCK:
        return _GATEWAY_SETUP_LOCKS.setdefault(hass, {}).setdefault(dev_id, RLock())


# Extra context for tinytuya error codes whose message does not fully describe
# the possible causes.  Error 914 in particular is reported for any failure to
# negotiate a session, which includes a device that is refusing connections
# until it is power cycled, not just a misconfigured key or protocol version.
_ERROR_HINTS = {
    "914": "  If previously running OK, likely the device needs to be power cycled.",
}


def _collect_possible_matches(cached_state, product_ids):
    """Collect possible matches from generator into an array."""
    return list(possible_matches(cached_state, product_ids))


def _receive_available(api):
    """Read available data without shortening TinyTuya's frame timeout."""
    wifi_api = api.parent or api
    if not wifi_api.socket:
        return None
    with DefaultSelector() as selector:
        selector.register(wifi_api.socket, EVENT_READ)
        ready = selector.select(timeout=0)
    return api.receive() if ready else None


class _GatewayPacket(dict):
    """One decoded frame, retaining its origin through TinyTuya's CID queue."""

    def __init__(self, data, command, sequence, unsolicited):
        super().__init__(data)
        self.command = command
        self.sequence = sequence
        self.unsolicited = unsolicited
        self.delivered = False


class _GatewayReceiver:
    """Own unsolicited reads from a gateway shared by several devices."""

    def __init__(self, parent, lock, hass):
        self.parent = parent
        self.lock = lock
        self.hass = hass
        self.devices = {}
        self.members = set()
        self.task = None
        self._stopping = False
        self._wake = asyncio.Event()

    def start(self, device):
        self.devices[device.dev_cid] = device
        if not self.task and not device._poll_only:
            self._stopping = False
            self.task = self.hass.async_create_task(self.receive_loop())
        elif self._stopping:
            self._wake.set()

    async def stop(self, device):
        if self.devices.get(device.dev_cid) is device:
            self.devices.pop(device.dev_cid)
        if not any(not d._poll_only for d in self.devices.values()):
            if self.task:
                task = self.task
                self._stopping = True
                self._wake.set()
                await task
            await self.disconnect_if_idle()

    async def set_persistent(self, device, persist):
        """Change the parent socket only while no operation is using it."""
        async with self.lock:
            other_active = any(
                member is not device and member._running and not member.should_poll
                for member in self.devices.values()
            )
            if persist or not other_active:
                self.parent.set_socketPersistent(persist)

    async def disconnect(self):
        """Close a failed connection after any in-flight read has finished."""
        async with self.lock:
            self.parent.set_socketPersistent(False)

    async def disconnect_if_idle(self):
        """Close the socket only if no push-capable child joined meanwhile."""
        async with self.lock:
            if not any(not d._poll_only for d in self.devices.values()):
                self.parent.set_socketPersistent(False)

    def read(self, func, *args):
        """Keep frame metadata before TinyTuya can queue a wrong-CID result.

        Called in the executor with the gateway lock held. TinyTuya 1.20
        exposes raw_recv, but overwrites it on recursive wrong-CID reads.
        Capture it at decode time, using attributes rather than DPS/JSON keys.
        The original decoder is restored even when a read fails.
        """
        decode = self.parent._decode_payload

        def decode_packet(payload):
            packet = decode(payload)
            if not isinstance(packet, dict) or not self.parent.raw_recv:
                return packet
            frame = self.parent.raw_recv[-1]
            sent = self.parent.raw_sent
            # Some replies echo the request sequence (3.5 does not). Do not
            # use sequence equality to override the observed command-8,
            # protocol-4 child report: genuine reports have nonzero sequences.
            poll_reply = (
                self.parent.version < 3.5
                and sent is not None
                and sent.cmd in {CT.DP_QUERY, CT.DP_QUERY_NEW, CT.UPDATEDPS}
                and frame.seqno != 0
                and frame.seqno == sent.seqno
                and not self._is_child_report(packet, frame.cmd)
            )
            return _GatewayPacket(
                packet,
                frame.cmd,
                frame.seqno,
                frame.cmd == CT.STATUS and not poll_reply,
            )

        self.parent._decode_payload = decode_packet
        try:
            return func(*args)
        finally:
            self.parent._decode_payload = decode

    @staticmethod
    def _is_child_report(packet, command):
        """Require a STATUS frame with the captured child-report JSON shape."""
        if command != CT.STATUS or not isinstance(packet, dict):
            return False
        data = packet.get("data")
        return (
            packet.get("protocol") == 4
            and isinstance(data, dict)
            and isinstance(data.get("dps"), dict)
            and bool(data.get("cid"))
        )

    def is_child_push(self, packet):
        """Require a report command as well as the child-report JSON shape."""
        return (
            isinstance(packet, _GatewayPacket)
            and packet.unsolicited
            and self._is_child_report(packet, packet.command)
            and packet["data"]["cid"] in self.devices
        )

    async def _wait(self, delay):
        if not delay:
            await asyncio.sleep(0)
            return
        try:
            await asyncio.wait_for(self._wake.wait(), delay)
        except TimeoutError:
            pass
        self._wake.clear()

    def dispatch(self, packet, path="receive", from_poll=None):
        if not isinstance(packet, dict):
            return
        if "Err" in packet:
            if packet["Err"] in {"901", "902", "905", "906", "914"}:
                self.parent.set_socketPersistent(False)
            return
        data = packet.get("data", packet)
        if not isinstance(data, dict) or not isinstance(data.get("dps"), dict):
            return
        cid = data.get("cid", packet.get("cid"))
        device = self.devices.get(cid)
        if device:
            if isinstance(packet, _GatewayPacket):
                if packet.delivered:
                    _ROUTING_LOGGER.debug(
                        "Gateway CID %s path=%s cmd=%s seq=%s already delivered",
                        cid,
                        path,
                        packet.command,
                        packet.sequence,
                    )
                    return
                packet.delivered = True
            if from_poll is None:
                # Unknown-origin packets can refresh state, but cannot prove
                # a press. In particular, protocol=4 alone is insufficient.
                from_poll = not (
                    isinstance(packet, _GatewayPacket)
                    and packet.command == CT.STATUS
                    and packet.unsolicited
                )
            _ROUTING_LOGGER.debug(
                "Gateway CID %s path=%s source=%s cmd=%s seq=%s protocol=%s dp_ids=%s",
                cid,
                path,
                "poll" if from_poll else "push",
                getattr(packet, "command", None),
                getattr(packet, "sequence", None),
                packet.get("protocol"),
                list(data["dps"]),
            )
            device._handle_poll(data["dps"].copy(), False, from_poll=from_poll)
        else:
            _LOGGER.debug("Unsolicited update for unregistered gateway CID %s", cid)

    async def receive_loop(self):
        try:
            while not self._stopping and any(
                not d._poll_only for d in self.devices.values()
            ):
                try:
                    async with self.lock:
                        # A child status call can queue another child's response.
                        while self.parent.received_wrong_cid_queue:
                            _, packet = self.parent.received_wrong_cid_queue.pop(0)
                            self.dispatch(packet, "cid-queue")
                        packet = None
                        if self.parent.socket and self.parent.socketPersistent:
                            read = asyncio.ensure_future(
                                self.hass.async_add_executor_job(
                                    self.read, _receive_available, self.parent
                                )
                            )
                            try:
                                packet = await asyncio.shield(read)
                            except CancelledError:
                                # Cancellation must not release the lock while
                                # the executor thread still owns the socket.
                                await read
                                raise
                        self.dispatch(packet)
                    await self._wait(0 if packet else 0.1)
                except Exception:
                    _LOGGER.exception("Gateway receive error")
                    await self.disconnect()
                    await self._wait(5)
        except CancelledError:
            raise
        finally:
            if self.task is asyncio.current_task():
                self.task = None
                if not self.hass.is_stopping and any(
                    not d._poll_only for d in self.devices.values()
                ):
                    self._stopping = False
                    self.task = self.hass.async_create_task(self.receive_loop())


class TuyaLocalDevice(object):
    def __init__(
        self,
        name,
        dev_id,
        address,
        local_key,
        protocol_version,
        dev_cid,
        hass: HomeAssistant,
        poll_only=False,
        manufacturer=None,
        model=None,
    ):
        """
        Represents a Tuya-based device.

        Args:
            name (str): The device name.
            dev_id (str): The device id.
            address (str): The network address.
            local_key (str): The encryption key.
            protocol_version (str | number): The protocol version.
            dev_cid (str): The sub device id.
            hass (HomeAssistant): The Home Assistant instance.
            poll_only (bool): True if the device should be polled only.
            manufacturer (str | None): The device manufacturer, if known.
            model (str | None): The device model, if known.
        """
        self._name = name
        self._manufacturer = manufacturer
        self._model = model
        self._children = []
        self._force_dps = []
        self._product_ids = []
        self._running = False
        self._shutdown_listener = None
        self._startup_listener = None
        self._api_protocol_version_index = None
        self._api_protocol_working = False
        self._api_working_protocol_failures = 0
        self.dev_id = dev_id
        self.dev_cid = dev_cid
        self._gateway_receiver = None
        try:
            if dev_cid:
                if hass.data[DOMAIN].get(dev_id) and name != "Test":
                    parent = hass.data[DOMAIN][dev_id]["tuyadevice"]
                    parent_lock = hass.data[DOMAIN][dev_id].get(
                        "tuyadevicelock", asyncio.Lock()
                    )
                else:
                    parent = tinytuya.Device(dev_id, address, local_key)
                    parent_lock = asyncio.Lock()
                    if name != "Test":
                        hass.data[DOMAIN][dev_id] = {
                            "tuyadevice": parent,
                            "tuyadevicelock": parent_lock,
                        }
                self._api = tinytuya.Device(
                    dev_cid,
                    cid=dev_cid,
                    parent=parent,
                )
                self._api_lock = parent_lock
                if name != "Test":
                    parent_entry = hass.data[DOMAIN][dev_id]
                    receiver = parent_entry.get("gateway_receiver")
                    if receiver is None:
                        receiver = _GatewayReceiver(parent, parent_lock, hass)
                        parent_entry["gateway_receiver"] = receiver
                        gateway_device = parent_entry.get("device")
                        if gateway_device:
                            gateway_device._gateway_receiver = receiver
                            receiver.members.add(gateway_device)
                    self._gateway_receiver = receiver
            else:
                if hass.data[DOMAIN].get(dev_id) and name != "Test":
                    self._api = hass.data[DOMAIN][dev_id]["tuyadevice"]
                    self._api_lock = hass.data[DOMAIN][dev_id].get(
                        "tuyadevicelock", asyncio.Lock()
                    )
                    self._gateway_receiver = hass.data[DOMAIN][dev_id].get(
                        "gateway_receiver"
                    )
                else:
                    self._api = tinytuya.Device(dev_id, address, local_key)
                    self._api_lock = asyncio.Lock()
                    if name != "Test":
                        hass.data[DOMAIN][dev_id] = {
                            "tuyadevice": self._api,
                            "tuyadevicelock": self._api_lock,
                        }
        except Exception as e:
            _LOGGER.error(
                "%s: %s while initialising device %s",
                type(e).__name__,
                e,
                dev_id,
            )
            raise e

        # we handle retries at a higher level so we can rotate protocol version
        self._api.set_socketRetryLimit(1)
        if self._api.parent:
            # Retries cause problems for other children of the parent device
            self._api.parent.set_socketRetryLimit(1)

        self._refresh_task = None
        self._protocol_configured = protocol_version
        self._poll_only = poll_only
        self._temporary_poll = False
        self._reset_cached_state()

        self._hass = hass

        # API calls to update Tuya devices are asynchronous and non-blocking.
        # This means you can send a change and immediately request an updated
        # state (like HA does), but because it has not yet finished processing
        # you will be returned the old state.
        # The solution is to keep a temporary list of changed properties that
        # we can overlay onto the state while we wait for the board to update
        # its switches.
        self._FAKE_IT_TIMEOUT = 5
        self._POLLING_INTERVAL = 30
        self._HEARTBEAT_INTERVAL = 5
        # More attempts are needed in auto mode so we can cycle through all
        # the possibilities a couple of times
        self._AUTO_CONNECTION_ATTEMPTS = len(API_PROTOCOL_VERSIONS) * 2 + 1
        self._SINGLE_PROTO_CONNECTION_ATTEMPTS = 3
        # The number of failures from a working protocol before retrying other protocols.
        self._AUTO_FAILURE_RESET_COUNT = 10

    @property
    def name(self):
        return self._name

    @property
    def unique_id(self):
        """Return the unique ID for this device."""
        if self.dev_cid:
            return get_device_id(
                {CONF_DEVICE_ID: self.dev_id, CONF_DEVICE_CID: self.dev_cid}
            )
        return self._api.id

    @property
    def device_info(self):
        """Return the device information for this device."""
        info = {
            "identifiers": {(DOMAIN, self.unique_id)},
            "name": self.name,
            "manufacturer": self._manufacturer or "Tuya",
        }
        if self._model:
            info["model"] = self._model
        return info

    @property
    def has_returned_state(self):
        """Return True if the device has returned some state."""
        cached = self._get_cached_state()
        return len(cached) > 1 or cached.get("updated_at", 0) > 0

    @callback
    def actually_start(self, event=None):
        _LOGGER.debug("Starting monitor loop for %s", self.name)
        self._running = True
        if self._gateway_receiver:
            gateway_device = self._hass.data[DOMAIN].get(self.dev_id, {}).get("device")
            if (
                gateway_device
                and gateway_device is not self
                and gateway_device._running
            ):
                self._gateway_receiver.start(gateway_device)
            self._gateway_receiver.start(self)
        self._shutdown_listener = self._hass.bus.async_listen_once(
            EVENT_HOMEASSISTANT_STOP, self.async_stop
        )
        if not self._refresh_task:
            self._refresh_task = self._hass.async_create_task(self.receive_loop())

    def start(self):
        if self._hass.is_stopping:
            return
        elif self._hass.is_running:
            if self._startup_listener:
                self._startup_listener()
                self._startup_listener = None
            self.actually_start()
        else:
            self._startup_listener = self._hass.bus.async_listen_once(
                EVENT_HOMEASSISTANT_STARTED, self.actually_start
            )

    async def async_stop(self, event=None):
        _LOGGER.debug("Stopping monitor loop for %s", self.name)
        self._running = False
        self._children.clear()
        self._force_dps.clear()
        if self._gateway_receiver:
            await self._gateway_receiver.stop(self)
        if self._refresh_task:
            await self._async_set_socket_persistent(False)
            await self._refresh_task
        _LOGGER.debug("Monitor loop for %s stopped", self.name)
        self._refresh_task = None

    def register_entity(self, entity):
        # If this is the first child entity to register, and HA is still
        # starting, refresh the device state so it shows as available without
        # waiting for startup to complete.
        should_poll = len(self._children) == 0 and not self._hass.is_running

        self._children.append(entity)
        for dp in entity._config.dps():
            if dp.force and dp.id not in self._force_dps:
                self._force_dps.append(int(dp.id))

        if not self._running and not self._startup_listener:
            self.start()
        if self.has_returned_state:
            entity.async_schedule_update_ha_state()
        elif should_poll:
            entity.async_schedule_update_ha_state(True)

    async def async_unregister_entity(self, entity):
        self._children.remove(entity)
        if not self._children:
            try:
                await self.async_stop()
            except CancelledError:
                pass

    async def receive_loop(self):
        """Coroutine wrapper for async_receive generator."""
        try:
            async for poll in self.async_receive():
                if isinstance(poll, dict):
                    full_poll = poll.pop("full_poll", False)
                    from_poll = poll.pop("from_poll", full_poll)
                    self._handle_poll(poll, full_poll, from_poll=from_poll)
                else:
                    _LOGGER.debug(
                        "%s received non data %s",
                        self.name,
                        log_json(poll),
                    )
            _LOGGER.warning("%s receive loop has terminated", self.name)

        except Exception as t:
            _LOGGER.exception(
                "%s receive loop terminated by exception %s", self.name, t
            )
        finally:
            # Ensure the persistent connection is closed when the loop exits
            # and device appears as unavailable
            await self._async_set_socket_persistent(False)
            self._reset_cached_state()

    def _set_socket_persistent(self, persist):
        """Set persistence on a device without a shared gateway receiver."""
        if self._api.parent:
            self._api.set_socketPersistent(persist)
            self._api.parent.set_socketPersistent(persist)
        else:
            self._api.set_socketPersistent(persist)

    async def _async_set_socket_persistent(self, persist):
        """Set gateway persistence after any in-flight socket operation."""
        if self._gateway_receiver:
            if self._api.parent:
                self._api.set_socketPersistent(persist)
            await self._gateway_receiver.set_persistent(self, persist)
        else:
            self._set_socket_persistent(persist)

    async def _disconnect_socket(self):
        """Reset a failed connection without racing the shared reader."""
        if self._gateway_receiver:
            if self._api.parent:
                self._api.set_socketPersistent(False)
            await self._gateway_receiver.disconnect()
        else:
            self._set_socket_persistent(False)

    def _handle_poll(self, poll, full_poll, *, from_poll=None):
        """Apply a poll or routed unsolicited update to this device."""
        if from_poll is None:
            from_poll = full_poll
        _ROUTING_LOGGER.debug(
            "%s received source=%s full_poll=%s dp_ids=%s",
            self.name,
            "poll" if from_poll else "push",
            full_poll,
            list(poll),
        )
        self._cached_state = self._cached_state | poll
        self._cached_state["updated_at"] = time()
        self._remove_properties_from_pending_updates(poll)

        for entity in self._children:
            try:
                # A partial updatedps reply is still a poll for event entities.
                # Keep full_poll separate for nonpersistent-DP cache cleanup.
                entity.on_receive(poll, from_poll)
            except Exception as e:
                _LOGGER.exception(
                    "%s on_receive error for entity %s: %s",
                    self.name,
                    entity.entity_id,
                    e,
                )
            if full_poll:
                for dp in entity._config.dps():
                    if not dp.persist and dp.id not in poll:
                        self._cached_state.pop(dp.id, None)
            entity.schedule_update_ha_state()

    @property
    def should_poll(self):
        return self._poll_only or self._temporary_poll or not self.has_returned_state

    def pause(self):
        self._temporary_poll = True
        _LOGGER.debug("%s pausing connection temporarily", self.name, False)
        if self._gateway_receiver:
            self._hass.async_create_task(self._disconnect_socket())
        else:
            self._set_socket_persistent(False)

    async def async_pause(self):
        """Pause a connection and wait for its shared reader to release it."""
        self._temporary_poll = True
        await self._disconnect_socket()

    def resume(self):
        self._temporary_poll = False

    async def async_receive(self):
        """Receive messages from a persistent connection asynchronously."""
        # If we didn't yet get any state from the device, we may need to
        # negotiate the protocol before making the connection persistent
        persist = not self.should_poll
        # flag to alternate updatedps and status calls to ensure we get
        # all dps updated
        dps_updated = False

        await self._async_set_socket_persistent(persist)

        last_heartbeat = self._cached_state.get("updated_at", 0)
        wifi_api = self._api.parent or self._api
        while self._running:
            error_count = self._api_working_protocol_failures
            force_backoff = False
            try:
                last_cache = self._cached_state.get("updated_at", 0)
                now = time()
                full_poll = False
                from_poll = False
                if (persist == self.should_poll) or (
                    persist and (wifi_api.socket is None)
                ):
                    # use persistent connections after initial communication
                    # has been established.  Until then, we need to rotate
                    # the protocol version, which seems to require a fresh
                    # connection.
                    persist = not self.should_poll
                    _LOGGER.debug(
                        "%s persistant connection set to %s", self.name, persist
                    )
                    await self._async_set_socket_persistent(persist)
                    self._last_full_poll = 0  # ensure we start with a full poll

                needs_full_poll = now - self._last_full_poll > self._POLLING_INTERVAL
                if now - last_cache > self._POLLING_INTERVAL or (
                    persist and needs_full_poll
                ):
                    from_poll = True
                    if (
                        self._force_dps
                        and not dps_updated
                        and self._api_protocol_working
                    ):
                        poll = await self._poll_with_gateway_pushes(
                            lambda: self._api.updatedps(self._force_dps),
                            f"Failed to update device dps for {self.name}",
                        )
                        dps_updated = True
                    else:
                        poll = await self._poll_with_gateway_pushes(
                            lambda: self._api.status(),
                            f"Failed to fetch device status for {self.name}",
                        )
                        dps_updated = False
                        full_poll = True
                    self._last_full_poll = now
                    last_heartbeat = now  # reset heartbeat timer on full poll
                elif persist:
                    if now - last_heartbeat > self._HEARTBEAT_INTERVAL:
                        if self._gateway_receiver:
                            async with self._api_lock:
                                await self._hass.async_add_executor_job(
                                    self._api.heartbeat, True
                                )
                        else:
                            await self._hass.async_add_executor_job(
                                self._api.heartbeat, True
                            )
                        last_heartbeat = now
                    if self._gateway_receiver:
                        poll = None
                    else:
                        async with self._api_lock:
                            read = asyncio.ensure_future(
                                self._hass.async_add_executor_job(
                                    _receive_available, self._api
                                )
                            )
                            try:
                                poll = await asyncio.shield(read)
                            except CancelledError:
                                await read
                                raise
                            # A gateway child may have joined during this read.
                            if self._gateway_receiver:
                                self._gateway_receiver.dispatch(poll)
                                poll = None
                    # Ignore Payload error 904, as 3.4 protocol devices seem to return
                    # this when there is no new data, instead of just returning nothing.
                    if poll and "Err" in poll and poll["Err"] == "904":
                        poll = None
                else:
                    force_backoff = True
                    poll = None

                if poll:
                    if "Err" in poll:
                        # Limit disconnects to the errors that are caused low level
                        # communication problems
                        if poll["Err"] in {"901", "902", "905", "906", "914"}:
                            force_backoff = True
                            persist = False
                            await self._disconnect_socket()
                        # increment the error count if not done already
                        if error_count == self._api_working_protocol_failures:
                            self._api_working_protocol_failures += 1
                        if self._api_working_protocol_failures == 1:
                            _LOGGER.warning(
                                "%s error reading: %s", self.name, poll["Error"]
                            )
                        else:
                            _LOGGER.debug(
                                "%s error reading: %s", self.name, poll["Error"]
                            )
                        if "Payload" in poll and poll["Payload"]:
                            _LOGGER.debug(
                                "%s err payload: %s",
                                self.name,
                                poll["Payload"],
                            )
                    else:
                        if "dps" in poll:
                            poll = poll["dps"]
                        if isinstance(poll, dict):
                            poll["full_poll"] = full_poll
                            poll["from_poll"] = from_poll
                            yield poll

            except CancelledError:
                self._running = False
                # Close the persistent connection when exiting the loop
                persist = False
                _LOGGER.debug("%s receive loop interrupted", self.name)
                await self._async_set_socket_persistent(False)
                raise
            except Exception as t:
                _LOGGER.exception(
                    "%s receive loop error %s:%s",
                    self.name,
                    type(t).__name__,
                    t,
                )
                persist = False
                await self._disconnect_socket()
                force_backoff = True

            if not self.has_returned_state or wifi_api.socket is None:
                force_backoff = True
            await asyncio.sleep(5 if force_backoff else 0.1)

        # Close the persistent connection when exiting the loop
        await self._async_set_socket_persistent(False)

    def set_detected_product_id(self, product_id):
        self._product_ids.append(product_id)

    async def async_possible_types(self):
        cached_state = self._get_cached_state()
        if len(cached_state) <= 1:
            # in case of device22 devices, we need to poll them with a dp
            # that exists on the device to get anything back.  Most switch-like
            # devices have dp 1. Lights generally start from 20.  101 is where
            # vendor specific dps start.  Between them, these three should cover
            # most devices.  148 covers a doorbell device that didn't have these
            # 201 covers remote controllers and 2 and 9 cover others without 1
            self._api.set_dpsUsed(
                {
                    "1": None,
                    "2": None,
                    "9": None,
                    "20": None,
                    "60": None,
                    "101": None,
                    "148": None,
                    "201": None,
                }
            )
            await self.async_refresh()
            cached_state = self._get_cached_state()

        return await self._hass.async_add_executor_job(
            _collect_possible_matches,
            cached_state,
            self._product_ids,
        )

    async def async_inferred_type(self):
        best_match = None
        best_quality = 0
        cached_state = self._get_cached_state()
        possible = await self.async_possible_types()
        for config in possible:
            quality = config.match_quality(cached_state, self._product_ids)
            _LOGGER.info(
                "%s considering %s with quality %s",
                self.name,
                config.name,
                quality,
            )
            if quality > best_quality:
                best_quality = quality
                best_match = config

        if best_match:
            return best_match.config_type

        _LOGGER.warning(
            "Detection for %s with dps %s failed",
            self.name,
            log_json(cached_state),
        )

    async def async_refresh(self):
        _LOGGER.debug("Refreshing device state for %s", self.name)
        if not self._running:
            error_message = f"Failed to refresh device state for {self.name}."
            if self._gateway_receiver:
                new_state = await self._poll_with_gateway_pushes(
                    self._api.status, error_message
                )
                self._cache_refreshed_state(new_state)
            else:
                await self._retry_on_failed_connection(
                    self._refresh_cached_state, error_message
                )

    def get_property(self, dps_id):
        cached_state = self._get_cached_state()
        return cached_state.get(dps_id)

    async def async_set_property(self, dps_id, value):
        await self.async_set_properties({dps_id: value})

    def anticipate_property_value(self, dps_id, value):
        """
        Update a value in the cached state only. This is good for when you
        know the device will reflect a new state in the next update, but
        don't want to wait for that update for the device to represent
        this state.

        The anticipated value will be cleared with the next update.
        """
        self._cached_state[dps_id] = value

    def _reset_cached_state(self):
        self._cached_state = {"updated_at": 0}
        self._pending_updates = {}
        self._last_connection = 0
        self._last_full_poll = 0

    def _refresh_cached_state(self):
        return self._cache_refreshed_state(self._api.status())

    def _cache_refreshed_state(self, new_state):
        """Apply an initial status reply after the gateway transaction completes."""
        if new_state:
            if "Err" not in new_state:
                self._cached_state = self._cached_state | new_state.get("dps", {})
                self._cached_state["updated_at"] = time()
                for entity in self._children:
                    for dp in entity._config.dps():
                        # Clear non-persistant dps that were not in the poll
                        if not dp.persist and dp.id not in new_state.get("dps", {}):
                            self._cached_state.pop(dp.id, None)
                    entity.schedule_update_ha_state()
            elif self._api_working_protocol_failures == 1:
                _LOGGER.warning(
                    "%s protocol error %s: %s",
                    self.name,
                    new_state.get("Err"),
                    new_state.get("Error", "message not provided"),
                )
            else:
                _LOGGER.debug(
                    "%s protocol error %s: %s",
                    self.name,
                    new_state.get("Err"),
                    new_state.get("Error", "message not provided"),
                )
        _LOGGER.debug(
            "%s refreshed device state: %s",
            self.name,
            log_json(new_state),
        )
        _LOGGER.debug(
            "new state (incl pending): %s",
            log_json(self._get_cached_state()),
        )
        return new_state

    async def async_set_properties(self, properties):
        if len(properties) == 0:
            return

        self._add_properties_to_pending_updates(properties)
        await self._debounce_sending_updates()

    def _add_properties_to_pending_updates(self, properties):
        now = time()

        pending_updates = self._get_pending_updates()
        for key, value in properties.items():
            pending_updates[key] = {
                "value": value,
                "updated_at": now,
                "sent": False,
            }

        _LOGGER.debug(
            "%s new pending updates: %s",
            self.name,
            log_json(pending_updates),
        )

    def _remove_properties_from_pending_updates(self, data):
        self._pending_updates = {
            key: value
            for key, value in self._pending_updates.items()
            if key not in data or not value["sent"] or data[key] != value["value"]
        }

    async def _debounce_sending_updates(self):
        now = time()
        since = now - self._last_connection
        # set this now to avoid a race condition, it will be updated later
        # when the data is actally sent
        self._last_connection = now
        # Only delay a second if there was recently another command.
        # Otherwise delay 1ms, to keep things simple by reusing the
        # same send mechanism.
        waittime = 1 if since < 1.1 and self.should_poll else 0.001

        await asyncio.sleep(waittime)
        await self._send_pending_updates()

    async def _send_pending_updates(self):
        pending_properties = self._get_unsent_properties()

        _LOGGER.debug(
            "%s sending dps update: %s",
            self.name,
            log_json(pending_properties),
        )

        await self._retry_on_failed_connection(
            lambda: self._set_values(pending_properties),
            "Failed to update device state.",
        )

    def _set_values(self, properties):
        self._api.set_multiple_values(properties, nowait=True)
        now = time()
        self._last_connection = now
        pending_updates = self._get_pending_updates()
        for key in properties.keys():
            pending_updates[key]["updated_at"] = now
            pending_updates[key]["sent"] = True

    async def _poll_with_gateway_pushes(self, func, error_message):
        """Complete one poll transaction before releasing the gateway lock."""
        receiver = self._gateway_receiver
        if receiver is None:
            return await self._retry_on_failed_connection(func, error_message)

        deliveries = []

        def receive_reply():
            # receive() can return an older matching-CID packet from TinyTuya's
            # queue, even after socket replacement. Drain it before sending a
            # new request so an old snapshot cannot complete that transaction.
            while receiver.parent.received_wrong_cid_queue:
                _, packet = receiver.parent.received_wrong_cid_queue.pop(0)
                deliveries.append((packet, "cid-queue"))
            # TinyTuya closes a nonpersistent socket before returning each
            # packet, including a push that arrived ahead of the poll reply.
            persist = receiver.parent.socketPersistent
            receiver.parent.set_socketPersistent(True)
            try:
                reply = receiver.read(func)
                while receiver.is_child_push(reply):
                    deliveries.append((reply, "poll-interleaved"))
                    # Child receive() reads again without sending another request.
                    reply = receiver.read(self._api.receive)
                    if reply is None:
                        raise TimeoutError("Gateway poll reply missing after push")
                return reply
            except Exception:
                # Retire this stream before releasing the shared lock: a late
                # solicited reply must never reach the unsolicited receiver.
                receiver.parent.set_socketPersistent(False)
                raise
            finally:
                if not persist:
                    receiver.parent.set_socketPersistent(False)

        try:
            reply = await self._retry_on_failed_connection(receive_reply, error_message)
        finally:
            for packet, path in deliveries:
                receiver.dispatch(packet, path)

        if isinstance(reply, dict):
            data = reply.get("data", reply)
            cid = data.get("cid", reply.get("cid")) if isinstance(data, dict) else None
            _ROUTING_LOGGER.debug(
                "%s poll reply cid=%s cmd=%s seq=%s protocol=%s",
                self.name,
                cid,
                getattr(reply, "command", None),
                getattr(reply, "sequence", None),
                reply.get("protocol"),
            )
            if cid and cid != self.dev_cid:
                # A gateway-wide query can return a child's state. Do not
                # apply it to the gateway entity or turn it into a press.
                receiver.dispatch(reply, "poll-reply", from_poll=True)
                return None
            if isinstance(reply, _GatewayPacket):
                if reply.delivered:
                    return None
                reply.delivered = True
        return reply

    async def _retry_on_failed_connection(self, func, error_message):
        if self._api_protocol_version_index is None:
            await self._rotate_api_protocol_version()
        auto = (self._protocol_configured == "auto") and (
            not self._api_protocol_working
        )
        dev22 = self._protocol_configured in (3.22, 3.42, 3.52)
        connections = (
            self._AUTO_CONNECTION_ATTEMPTS
            if auto
            else (
                self._SINGLE_PROTO_CONNECTION_ATTEMPTS * 2
                if dev22
                else self._SINGLE_PROTO_CONNECTION_ATTEMPTS
            )
        )

        last_err_code = None
        last_err_msg = None
        for i in range(connections):
            try:
                if not self._hass.is_stopping:
                    async with self._api_lock:
                        if self._gateway_receiver:
                            read = asyncio.ensure_future(
                                self._hass.async_add_executor_job(func)
                            )
                            try:
                                retval = await asyncio.shield(read)
                            except CancelledError:
                                # The executor must finish using the shared socket
                                # before the receiver can acquire its lock.
                                try:
                                    await read
                                finally:
                                    raise
                        else:
                            retval = await self._hass.async_add_executor_job(func)
                    if isinstance(retval, dict) and "Error" in retval:
                        last_err_code = retval.get("Err")
                        last_err_msg = retval.get("Error")
                        if last_err_code == "900":
                            # Some devices (e.g. IR/RF remotes) never return
                            # status data; error 900 is their normal response
                            # to a status query. Treat as reachable with no
                            # data so commands can still be sent.
                            self._cached_state["updated_at"] = time()
                            retval = None
                        else:
                            raise AttributeError(retval["Error"])
                    self._api_protocol_working = True
                    self._api_working_protocol_failures = 0
                    return retval
            except Exception as e:
                _LOGGER.debug(
                    "Retrying after exception %s %s (%d/%d)",
                    type(e).__name__,
                    e,
                    i,
                    connections,
                )
                # Ensure we have a fresh connection for the next attempt
                await self._disconnect_socket()

                if i + 1 == connections:
                    self._reset_cached_state()
                    self._api_working_protocol_failures += 1
                    if (
                        self._api_working_protocol_failures
                        > self._AUTO_FAILURE_RESET_COUNT
                    ):
                        self._api_protocol_working = False
                        for entity in self._children:
                            entity.async_schedule_update_ha_state()
                    if last_err_code:
                        log_format = "%s Device reported error %s: %s%s"
                        log_args = (
                            error_message,
                            last_err_code,
                            last_err_msg,
                            _ERROR_HINTS.get(last_err_code, ""),
                        )
                    else:
                        log_format = "%s"
                        log_args = (error_message,)

                    if self._api_working_protocol_failures == 1 and not (
                        last_err_code == "914" and self._protocol_configured == "auto"
                    ):
                        _LOGGER.error(log_format, *log_args)
                    else:
                        _LOGGER.debug(log_format, *log_args)

                if not self._api_protocol_working:
                    await self._rotate_api_protocol_version()

    def _get_cached_state(self):
        cached_state = self._cached_state.copy()
        return {**cached_state, **self._get_pending_properties()}

    def _get_pending_properties(self):
        return {key: prop["value"] for key, prop in self._get_pending_updates().items()}

    def _get_unsent_properties(self):
        return {
            key: info["value"]
            for key, info in self._get_pending_updates().items()
            if not info["sent"]
        }

    def _get_pending_updates(self):
        now = time()
        # sort pending updates according to their API identifier
        pending_updates_sorted = sorted(
            self._pending_updates.items(), key=lambda x: int(x[0])
        )
        self._pending_updates = {
            key: value
            for key, value in pending_updates_sorted
            if not value["sent"]
            or now - value.get("updated_at", 0) < self._FAKE_IT_TIMEOUT
        }
        return self._pending_updates

    async def _rotate_api_protocol_version(self):
        if self._api_protocol_version_index is None:
            try:
                self._api_protocol_version_index = API_PROTOCOL_VERSIONS.index(
                    self._protocol_configured
                )
            except ValueError:
                self._api_protocol_version_index = 0

        # only rotate if configured as auto
        elif self._protocol_configured == "auto":
            self._api_protocol_version_index += 1

        if self._api_protocol_version_index >= len(API_PROTOCOL_VERSIONS):
            self._api_protocol_version_index = 0

        new_version = API_PROTOCOL_VERSIONS[self._api_protocol_version_index]
        _LOGGER.debug(
            "Setting protocol version for %s to %s",
            self.name,
            new_version,
        )
        # Only enable tinytuya's "device22" auto-detect when exlpicitly requested
        # as 3.22, 3.42, or 3.52
        # Enabling this on other devices can cause them to stop responding to commands,
        # as once tinytuya decides to switch to it, it never switches back.
        # 3.2 always uses the "device22" protocol variant.
        # 3.22 is a fake version that actually means 3.3 with auto-detect enabled
        # likewise 3.42 and 3.52 actually mean 3.4 and 3.5 with auto-detect enabled.
        #
        # Note: "device22" is a misnomer for historical reasons. Not all devices with
        # 22 character device ids use this protocol variant.
        if new_version == 3.22:
            new_version = 3.3
            self._api.disabledetect = False
        elif new_version == 3.42:
            new_version = 3.4
            self._api.disabledetect = False
        elif new_version == 3.52:
            new_version = 3.5
            self._api.disabledetect = False
        else:
            self._api.disabledetect = True

        async with self._api_lock:
            await self._hass.async_add_executor_job(
                self._api.set_version,
                new_version,
            )
            if self._api.parent:
                await self._hass.async_add_executor_job(
                    self._api.parent.set_version,
                    new_version,
                )

    @staticmethod
    def get_key_for_value(obj, value, fallback=None):
        keys = list(obj.keys())
        values = list(obj.values())
        return keys[values.index(value)] if value in values else fallback


def setup_device(hass: HomeAssistant, config: dict):
    """Setup a tuya device based on passed in config."""
    # Config entries are set up in executor threads, possibly concurrently.
    # The parent, its lock, and receiver must be published as one unit.
    with _gateway_setup_lock(hass, config[CONF_DEVICE_ID]):
        return _setup_device(hass, config)


def _setup_device(hass: HomeAssistant, config: dict):
    """Construct and cache a device with the gateway setup lock held."""

    _LOGGER.info("Creating device: %s", get_device_id(config))
    hass.data.setdefault(DOMAIN, {})
    try:
        device = TuyaLocalDevice(
            config[CONF_NAME],
            config[CONF_DEVICE_ID],
            config[CONF_HOST],
            config[CONF_LOCAL_KEY],
            config[CONF_PROTOCOL_VERSION],
            config.get(CONF_DEVICE_CID),
            hass,
            config[CONF_POLL_ONLY],
            manufacturer=config.get(CONF_MANUFACTURER),
            model=config.get(CONF_MODEL),
        )
    except Exception:
        parent_entry = hass.data[DOMAIN].get(config[CONF_DEVICE_ID])
        if parent_entry and not parent_entry.get("device"):
            receiver = parent_entry.get("gateway_receiver")
            if not receiver or not receiver.members:
                parent_entry["tuyadevice"].set_socketPersistent(False)
                hass.data[DOMAIN].pop(config[CONF_DEVICE_ID], None)
        raise
    hass.data[DOMAIN].setdefault(get_device_id(config), {}).update(
        {
            "device": device,
            "tuyadevice": device._api,
            "tuyadevicelock": device._api_lock,
        }
    )
    if device._gateway_receiver:
        device._gateway_receiver.members.add(device)

    return device


def release_gateway_member(hass: HomeAssistant, device):
    """Remove the raw gateway cache after its final configured device leaves."""
    receiver = device._gateway_receiver
    if receiver is None:
        return
    with _gateway_setup_lock(hass, device.dev_id):
        receiver.members.discard(device)
        domain_data = hass.data.get(DOMAIN, {})
        gateway_entry = domain_data.get(device.dev_id)
        if (
            gateway_entry
            and gateway_entry.get("gateway_receiver") is receiver
            and not receiver.members
            and not gateway_entry.get("device")
        ):
            domain_data.pop(device.dev_id, None)


def cleanup_failed_device(hass: HomeAssistant, device_id: str, gateway_id: str):
    """Remove a failed setup without disconnecting other gateway children."""
    with _gateway_setup_lock(hass, gateway_id):
        domain_data = hass.data.get(DOMAIN, {})
        stale = domain_data.get(device_id)
        if not stale:
            return
        device = stale.get("device")
        receiver = device._gateway_receiver if device else None
        if receiver:
            release_gateway_member(hass, device)
            if receiver.members and device_id == device.dev_id:
                stale.pop("device", None)
                return
        domain_data.pop(device_id, None)
        api = stale.get("tuyadevice")
        if api:
            api.set_socketPersistent(False)
            if api.parent and (not receiver or not receiver.members):
                api.parent.set_socketPersistent(False)


async def async_delete_device(hass: HomeAssistant, config: dict):
    device_id = get_device_id(config)
    _LOGGER.info("Deleting device: %s", device_id)
    domain_data = hass.data.get(DOMAIN, {})
    device_entry = domain_data.get(device_id)
    if device_entry is None:
        return

    device = device_entry.get("device")
    if device is not None:
        await device.async_stop()
        device_entry.pop("device", None)
        await hass.async_add_executor_job(release_gateway_member, hass, device)
    receiver = device_entry.get("gateway_receiver")
    if not receiver or not receiver.members:
        device_entry.pop("tuyadevice", None)
        device_entry.pop("tuyadevicelock", None)
        device_entry.pop("gateway_receiver", None)
    # Platform setup may cache entity instances in this bucket by config_id.
    # Only drop empty buckets here; async_unload_entry removes the whole bucket
    # after forwarded platform unloads complete.
    if not device_entry:
        domain_data.pop(device_id, None)
