import asyncio
import json
import logging
import socket
from threading import Event as ThreadEvent
from time import sleep as blocking_sleep
from time import time
from types import SimpleNamespace

import pytest
import tinytuya
from homeassistant.const import CONF_HOST, CONF_NAME
from tinytuya.core import command_types as CT
from tinytuya.core.crypto_helper import AESCipher
from tinytuya.core.message_helper import TuyaMessage, pack_message

# from homeassistant.const import EVENT_HOMEASSISTANT_STARTED, EVENT_HOMEASSISTANT_STOP
from custom_components.tuya_local.const import (
    CONF_DEVICE_CID,
    CONF_DEVICE_ID,
    CONF_LOCAL_KEY,
    CONF_POLL_ONLY,
    CONF_PROTOCOL_VERSION,
    DOMAIN,
)
from custom_components.tuya_local.device import (
    TuyaLocalDevice,
    _GatewayPacket,
    async_delete_device,
    setup_device,
)
from custom_components.tuya_local.event import TuyaLocalEvent
from custom_components.tuya_local.helpers.device_config import get_config

from .const import EUROM_600_HEATER_PAYLOAD


@pytest.fixture
def mock_api(mocker):
    mock = mocker.patch("tinytuya.Device")
    mock.parent = None
    yield mock


@pytest.fixture
def patched_hass(hass, mocker):
    hass.is_running = True
    hass.is_stopping = False
    hass.data = {"tuya_local": {}}

    async def job(func, *args):
        print(f"{args}")
        return func(*args)

    mocker.patch.object(hass, "async_add_executor_job", side_effect=job)
    mocker.patch.object(hass, "async_create_task")
    return hass


@pytest.fixture
def subject(patched_hass, mock_api, mocker):
    subject = TuyaLocalDevice(
        "Some name",
        "some_dev_id",
        "some.ip.address",
        "some_local_key",
        "auto",
        None,
        patched_hass,
    )
    # For most tests we want the protocol working
    subject._api_protocol_version_index = 0
    subject._api_protocol_working = True
    subject._protocol_configured = "auto"
    return subject


def test_name(subject):
    """Returns the name given at instantiation."""
    assert subject.name == "Some name"


def test_unique_id(subject, mock_api):
    """Returns the unique ID presented by the API class."""
    assert subject.unique_id is mock_api().id


def test_subdevice_unique_id_is_scoped_by_gateway(patched_hass, mock_api):
    """Returns a gateway-scoped ID for a child device."""
    subject = TuyaLocalDevice(
        "Some name",
        "gateway_id",
        "some.ip.address",
        "some_local_key",
        "3.3",
        "child_id",
        patched_hass,
    )

    assert subject.unique_id == "gateway_id/child_id"


def test_temporary_subdevice_does_not_register_gateway_receiver(patched_hass, mock_api):
    subject = TuyaLocalDevice(
        "Test", "gateway_id", "host", "key", 3.4, "child_id", patched_hass
    )

    assert subject._gateway_receiver is None
    assert "gateway_id" not in patched_hass.data[DOMAIN]


def test_device_info(subject, mock_api):
    """Returns generic info plus the unique ID for categorisation."""
    assert subject.device_info == {
        "identifiers": {("tuya_local", mock_api().id)},
        "name": "Some name",
        "manufacturer": "Tuya",
    }


@pytest.mark.asyncio
async def test_delete_keeps_device_entry_when_stop_fails(hass, mocker):
    """Device cache should not be removed before stop succeeds."""
    device = mocker.MagicMock()
    device.async_stop = mocker.AsyncMock(side_effect=RuntimeError("stop failed"))
    hass.data[DOMAIN] = {
        "deviceid": {
            "device": device,
            "tuyadevice": mocker.MagicMock(),
            "tuyadevicelock": mocker.MagicMock(),
        }
    }

    with pytest.raises(RuntimeError, match="stop failed"):
        await async_delete_device(hass, {CONF_DEVICE_ID: "deviceid"})

    assert hass.data[DOMAIN]["deviceid"]["device"] is device


def test_has_returned_state(subject):
    """Returns True if the device has returned its state."""
    subject._cached_state = EUROM_600_HEATER_PAYLOAD
    assert subject.has_returned_state

    subject._cached_state = {"updated_at": 0}
    assert not subject.has_returned_state


@pytest.mark.asyncio
async def test_refreshes_state_if_no_cached_state_exists(subject, mocker):
    subject._cached_state = {}
    subject.async_refresh = mocker.AsyncMock()

    await subject.async_inferred_type()

    subject.async_refresh.assert_awaited()


@pytest.mark.asyncio
async def test_detection_returns_none_when_device_type_not_detected(subject):
    subject._cached_state = {"192": False, "updated_at": time()}
    assert await subject.async_inferred_type() is None


@pytest.mark.asyncio
async def test_refresh_retries_up_to_eleven_times(subject, mock_api):
    subject._api_protocol_working = False
    mock_api().status.side_effect = [
        Exception("Error"),
        Exception("Error"),
        Exception("Error"),
        Exception("Error"),
        Exception("Error"),
        Exception("Error"),
        Exception("Error"),
        Exception("Error"),
        Exception("Error"),
        Exception("Error"),
        {"dps": {"1": False}},
    ]

    await subject.async_refresh()

    assert mock_api().status.call_count == 11
    assert subject._cached_state["1"] is False


@pytest.mark.asyncio
async def test_refresh_clears_cache_after_allowed_failures(subject, mock_api):
    subject._cached_state = {"1": True}
    subject._pending_updates = {
        "1": {"value": False, "updated_at": time(), "sent": True}
    }
    mock_api().status.side_effect = [
        Exception("Error"),
        Exception("Error"),
        Exception("Error"),
    ]

    await subject.async_refresh()

    assert mock_api().status.call_count == 3
    assert subject._cached_state == {"updated_at": 0}
    assert subject._pending_updates == {}


@pytest.mark.asyncio
async def test_api_protocol_version_is_rotated_with_each_failure(
    subject, mock_api, mocker
):
    subject._api_protocol_version_index = None
    subject._api_protocol_working = False
    mock_api().status.side_effect = [
        Exception("Error"),
        Exception("Error"),
        Exception("Error"),
        Exception("Error"),
        Exception("Error"),
        Exception("Error"),
        Exception("Error"),
    ]
    await subject.async_refresh()

    mock_api().set_version.assert_has_calls(
        [
            mocker.call(3.3),
            mocker.call(3.1),
            mocker.call(3.2),
            mocker.call(3.4),
            mocker.call(3.5),
            mocker.call(3.3),
            mocker.call(3.4),
            mocker.call(3.5),
            mocker.call(3.3),
            mocker.call(3.1),
            mocker.call(3.2),
            mocker.call(3.4),
            mocker.call(3.5),
            mocker.call(3.3),
            mocker.call(3.4),
            mocker.call(3.5),
            mocker.call(3.3),
            mocker.call(3.1),
        ]
    )


@pytest.mark.asyncio
async def test_api_protocol_version_is_stable_once_successful(
    subject, mock_api, mocker
):
    subject._api_protocol_version_index = None
    subject._api_protocol_working = False
    mock_api().status.side_effect = [
        Exception("Error"),
        Exception("Error"),
        Exception("Error"),
        {"dps": {"1": False}},
        {"dps": {"1": False}},
        Exception("Error"),
        Exception("Error"),
        {"dps": {"1": False}},
    ]

    await subject.async_refresh()
    assert subject._api_protocol_version_index == 3
    assert subject._api_protocol_working
    await subject.async_refresh()
    assert subject._api_protocol_version_index == 3
    await subject.async_refresh()
    assert subject._api_protocol_version_index == 3

    mock_api().set_version.assert_has_calls(
        [
            mocker.call(3.1),
            mocker.call(3.2),
            mocker.call(3.4),
        ]
    )


@pytest.mark.asyncio
async def test_api_protocol_version_is_not_rotated_when_not_auto(subject, mock_api):
    # Set up preconditions for the test

    subject._protocol_configured = 3.4
    subject._api_protocol_version_index = None
    subject._api_protocol_working = False
    mock_api().status.side_effect = [
        Exception("Error"),
        Exception("Error"),
        Exception("Error"),
        {"dps": {"1": False}},
        {"dps": {"1": False}},
        Exception("Error"),
        Exception("Error"),
        Exception("Error"),
        Exception("Error"),
        Exception("Error"),
        Exception("Error"),
        Exception("Error"),
        {"dps": {"1": False}},
    ]
    await subject._rotate_api_protocol_version()
    mock_api().set_version.assert_called_once_with(3.4)
    mock_api().set_version.reset_mock()

    await subject.async_refresh()
    assert subject._api_protocol_version_index == 3
    await subject.async_refresh()
    assert subject._api_protocol_version_index == 3
    await subject.async_refresh()
    assert subject._api_protocol_version_index == 3


def test_reset_cached_state_clears_cached_state_and_pending_updates(subject):
    subject._cached_state = {"1": True, "updated_at": time()}
    subject._pending_updates = {
        "1": {"value": False, "updated_at": time(), "sent": True}
    }

    subject._reset_cached_state()

    assert subject._cached_state == {"updated_at": 0}
    assert subject._pending_updates == {}


def test_get_property_returns_value_from_cached_state(subject):
    subject._cached_state = {"1": True}
    assert subject.get_property("1") is True


def test_get_property_returns_pending_update_value(subject):
    subject._pending_updates = {
        "1": {"value": False, "updated_at": time() - 4, "sent": True}
    }
    assert subject.get_property("1") is False


def test_pending_update_value_overrides_cached_value(subject):
    subject._cached_state = {"1": True}
    subject._pending_updates = {
        "1": {"value": False, "updated_at": time() - 4, "sent": True}
    }

    assert subject.get_property("1") is False


def test_expired_pending_update_value_does_not_override_cached_value(subject):
    subject._cached_state = {"1": True}
    subject._pending_updates = {
        "1": {"value": False, "updated_at": time() - 5, "sent": True}
    }

    assert subject.get_property("1") is True


def test_get_property_returns_none_when_value_does_not_exist(subject):
    subject._cached_state = {"1": True}
    assert subject.get_property("2") is None


@pytest.mark.asyncio
async def test_async_set_property_sends_to_api(subject, mock_api):
    await subject.async_set_property("1", False)

    mock_api().set_multiple_values.assert_called_once()


@pytest.mark.asyncio
async def test_set_property_immediately_stores_pending_updates(subject):
    subject._cached_state = {"1": True}
    await subject.async_set_property("1", False)
    assert not subject.get_property("1")


@pytest.mark.asyncio
async def test_set_properties_takes_no_action_when_nothing_provided(subject, mocker):
    mock = mocker.patch("asyncio.sleep")
    await subject.async_set_properties({})
    mock.assert_not_called()


def test_anticipate_property_value_updates_cached_state(subject):
    subject._cached_state = {"1": True}
    subject.anticipate_property_value("1", False)
    assert subject._cached_state["1"] is False


def test_get_key_for_value_returns_key_from_object_matching_value(subject):
    obj = {"key1": "value1", "key2": "value2"}

    assert TuyaLocalDevice.get_key_for_value(obj, "value1") == "key1"
    assert TuyaLocalDevice.get_key_for_value(obj, "value2") == "key2"


def test_get_key_for_value_returns_fallback_when_value_not_found(subject):
    obj = {"key1": "value1", "key2": "value2"}
    assert TuyaLocalDevice.get_key_for_value(obj, "value3", fallback="fb") == "fb"


def test_refresh_cached_state(subject, mock_api):
    # set up preconditions
    mock_api().status.return_value = {"dps": {"1": "CHANGED"}}
    subject._cached_state = {"1": "UNCHANGED", "updated_at": 123}

    # call the function under test
    subject._refresh_cached_state()

    # Did it call the API as expected?
    mock_api().status.assert_called_once()
    # Did it update the cached state?
    assert subject._cached_state == {"1": "CHANGED"} | subject._cached_state
    # Did it update the timestamp on the cached state?
    assert subject._cached_state["updated_at"] == pytest.approx(time(), abs=2)


def test_set_values(subject, mock_api):
    # set up preconditions
    subject._pending_updates = {
        "1": {"value": "sample", "updated_at": time() - 2, "sent": False},
    }

    # call the function under test
    subject._set_values({"1": "sample"})

    # did it send what it was asked?
    mock_api().set_multiple_values.assert_called_once_with({"1": "sample"}, nowait=True)
    # did it mark the pending updates as sent?
    assert subject._pending_updates["1"]["sent"]
    # did it update the time on the pending updates?
    assert subject._pending_updates["1"]["updated_at"] == pytest.approx(time(), abs=2)
    # did it lock and unlock when sending
    # subject._lock.acquire.assert_called_once()
    # subject._lock.release.assert_called_once()


def test_pending_updates_cleared_on_receipt(subject):
    # Set up the preconditions
    now = time()
    subject._pending_updates = {
        "1": {"value": True, "updated_at": now, "sent": True},
        "2": {"value": True, "updated_at": now, "sent": False},  # unsent
        "3": {"value": True, "updated_at": now, "sent": True},  # unmatched
        "4": {"value": True, "updated_at": now, "sent": True},  # not received
    }
    subject._remove_properties_from_pending_updates({"1": True, "2": True, "3": False})
    assert subject._pending_updates == {
        "2": {"value": True, "updated_at": now, "sent": False},
        "3": {"value": True, "updated_at": now, "sent": True},
        "4": {"value": True, "updated_at": now, "sent": True},
    }


def test_actually_start(subject, mocker, patched_hass):
    # Set up the preconditions
    mocker.patch.object(subject, "receive_loop", return_value="LOOP")
    mocker.patch.object(subject, "_refresh_task", new=mocker.AsyncMock)
    subject._running = False
    mocker.patch.object(patched_hass, "async_create_task")
    # patched_hass.async_create_task = mocker.MagicMock()
    # patched_hass.bus.async_listen_once = mocker.AsyncMock()
    # patched_hass.bus.async_listen_once.return_value = "LISTENER"
    # run the function under test
    subject.actually_start()

    # did it register a listener for EVENT_HOMEASSISTANT_STOP?
    # patched_hass.bus.async_listen_once.assert_called_once_with(
    #     EVENT_HOMEASSISTANT_STOP, subject.async_stop
    # )
    # assert subject._shutdown_listener == "LISTENER"
    # did it set the running flag?
    assert subject._running
    # did it schedule the loop?
    # task.assert_called_once()


def test_start_starts_when_ha_running(subject, patched_hass, mocker):
    # Set up preconditions
    patched_hass.is_running = True
    listener = mocker.MagicMock()
    subject._startup_listener = listener
    subject.actually_start = mocker.MagicMock()

    # Call the function under test
    subject.start()

    # Did it actually start?
    subject.actually_start.assert_called_once()
    # Did it cancel the startup listener?
    assert subject._startup_listener is None
    listener.assert_called_once()


def test_start_schedules_for_later_when_ha_starting(subject, patched_hass, mocker):
    # Set up preconditions
    patched_hass.is_running = False
    subject.actually_start = mocker.MagicMock()

    # Call the function under test
    subject.start()

    # Did it avoid actually starting?
    subject.actually_start.assert_not_called()
    # Did it register a listener?
    # assert subject._startup_listener == "LISTENER"
    # patched_hass.bus.async_listen_once.assert_called_once_with(
    #     EVENT_HOMEASSISTANT_STARTED, subject.actually_start
    # )


def test_start_does_nothing_when_ha_stopping(subject, patched_hass, mocker):
    # Set up preconditions
    patched_hass.is_running = True
    patched_hass.is_stopping = True
    subject.actually_start = mocker.MagicMock()

    # Call the function under test
    subject.start()

    # Did it avoid actually starting?
    subject.actually_start.assert_not_called()
    # Did it avoid registering a listener?
    # patched_hass.bus.async_listen_once.assert_not_called()
    assert subject._startup_listener is None


@pytest.mark.asyncio
async def test_async_stop(subject, mocker):
    # Set up preconditions
    listener = mocker.MagicMock()
    subject._refresh_task = None
    subject._shutdown_listener = listener
    subject._children = [1, 2, 3]

    # Call the function under test
    await subject.async_stop()

    # Shutdown listener doesn't get cancelled as HA does that
    listener.assert_not_called()
    # Were the child entities cleared?
    assert subject._children == []
    # Did it wait for the refresh task to finish then clear it?
    # This doesn't work because AsyncMock only mocks awaitable method calls
    # but we want an awaitable object
    # refresh.assert_awaited_once()
    assert subject._refresh_task is None


@pytest.mark.asyncio
async def test_async_stop_when_not_running(subject):
    # Set up preconditions
    _refresh_task = None
    subject._shutdown_listener = None
    subject._children = []

    # Call the function under test
    await subject.async_stop()

    # Was the shutdown listener left empty?
    assert subject._shutdown_listener is None
    # Were the child entities cleared?
    assert subject._children == []
    # Was the refresh task left empty?
    assert subject._refresh_task is None


def test_register_first_entity_ha_running(subject, mocker):
    # Set up preconditions
    subject._children = []
    subject._running = False
    subject._startup_listener = None
    subject.start = mocker.MagicMock()
    entity = mocker.AsyncMock()
    entity._config = mocker.MagicMock()
    entity._config.dps.return_value = []
    # despite the name, the below HA function is not async and does not need to be awaited
    entity.async_schedule_update_ha_state = mocker.MagicMock()

    # Call the function under test
    subject.register_entity(entity)

    # Was the entity added to the list?
    assert subject._children == [entity]

    # Did we start the loop?
    subject.start.assert_called_once()


def test_register_subsequent_entity_ha_running(subject, mocker):
    # Set up preconditions
    first = mocker.AsyncMock()
    second = mocker.AsyncMock()
    second._config = mocker.MagicMock()
    second._config.dps.return_value = []
    subject._children = [first]
    subject._running = True

    subject._startup_listener = None
    subject.start = mocker.MagicMock()

    # Call the function under test
    subject.register_entity(second)

    # Was the entity added to the list?
    assert set(subject._children) == set([first, second])

    # Did we avoid restarting the loop?
    subject.start.assert_not_called()


def test_register_subsequent_entity_ha_starting(subject, mocker):
    # Set up preconditions
    first = mocker.AsyncMock()
    second = mocker.AsyncMock()
    second._config = mocker.MagicMock()
    second._config.dps.return_value = []
    subject._children = [first]
    subject._running = False
    subject._startup_listener = mocker.MagicMock()
    subject.start = mocker.MagicMock()

    # Call the function under test
    subject.register_entity(second)

    # Was the entity added to the list?
    assert set(subject._children) == set([first, second])
    # Did we avoid restarting the loop?
    subject.start.assert_not_called()


@pytest.mark.asyncio
async def test_unregister_one_of_many_entities(subject, mocker):
    # Set up preconditions
    subject._children = ["First", "Second"]
    subject.async_stop = mocker.AsyncMock()

    # Call the function under test
    await subject.async_unregister_entity("First")

    # Was the entity removed from the list?
    assert set(subject._children) == set(["Second"])
    # Is the loop still running?
    subject.async_stop.assert_not_called()


@pytest.mark.asyncio
async def test_unregister_last_entity(subject, mocker):
    # Set up preconditions
    subject._children = ["Last"]
    subject.async_stop = mocker.AsyncMock()

    # Call the function under test
    await subject.async_unregister_entity("Last")

    # Was the entity removed from the list?
    assert subject._children == []
    # Was the loop stopped?
    subject.async_stop.assert_called_once()


@pytest.mark.asyncio
async def test_async_receive(subject, mock_api, mocker):
    # Set up preconditions
    selector = mocker.patch("custom_components.tuya_local.device.DefaultSelector")
    selector.return_value.__enter__.return_value.select.return_value = [True]
    mock_api().status.return_value = {"dps": {"1": "INIT", "2": 2}}
    mock_api().receive.return_value = {"1": "UPDATED"}
    subject._running = True
    subject._cached_state = {"updated_at": 0}
    # Call the function under test
    print("starting test loop...")
    loop = subject.async_receive()
    print("getting first iteration...")
    result = await loop.__anext__()

    # Check that the loop was started, but without persistent connection
    # since there was no state returned yet and it might need to negotiate
    # version.
    mock_api().set_socketPersistent.assert_called_once_with(False)
    # Check that a full poll was done
    mock_api().status.assert_called_once()
    assert result == {"1": "INIT", "2": 2, "full_poll": True, "from_poll": True}
    # Prepare for next round
    subject._cached_state = subject._cached_state | result
    mock_api().status.reset_mock()
    mock_api().set_socketPersistent.reset_mock()
    print("getting second iteration...")
    result = await loop.__anext__()
    # Check that the connection was made persistent now that data has been
    # returned
    mock_api().set_socketPersistent.assert_called_once_with(True)
    mock_api().status.reset_mock()
    # Wait long enough to force a heartbeat poll on the next iteration
    subject._cached_state = subject._cached_state | {"updated_at": time()}
    await asyncio.sleep(10.1)
    print("getting third iteration...")
    # Call the function under test
    result = await loop.__anext__()

    # Check that a heartbeat poll was done
    mock_api().status.assert_not_called()
    mock_api().heartbeat.assert_called_once()
    mock_api().receive.assert_called_once()
    assert result == {"1": "UPDATED", "full_poll": False, "from_poll": False}
    # Prepare for next iteration
    subject._running = False
    mock_api().set_socketPersistent.reset_mock()

    # Call the function under test
    print("getting last iteration...")
    try:
        result = await loop.__anext__()
        pytest.fail("Should have raised an exception to quit the loop")
    # Check that the loop terminated
    except StopAsyncIteration:
        pass
    mock_api().set_socketPersistent.assert_called_once_with(False)


@pytest.mark.asyncio
async def test_direct_idle_receive_does_not_block_commands(patched_hass, mocker):
    api = tinytuya.Device("direct", "127.0.0.1", "0123456789abcdef")
    api.set_version(3.4)
    patched_hass.data[DOMAIN]["direct"] = {
        "tuyadevice": api,
        "tuyadevicelock": asyncio.Lock(),
    }
    device = TuyaLocalDevice("Direct", "direct", "host", "key", 3.4, None, patched_hass)
    reader, peer = socket.socketpair()
    api.socket = reader
    api.set_socketTimeout(5)
    device._running = True
    device._api_protocol_version_index = 0
    device._api_protocol_working = True
    device._cached_state = {"1": False, "updated_at": time()}
    device._last_full_poll = time()
    read_started = asyncio.Event()

    async def executor_job(func, *args):
        read_started.set()
        return await asyncio.to_thread(func, *args)

    mocker.patch.object(
        patched_hass, "async_add_executor_job", side_effect=executor_job
    )
    loop = device.async_receive()
    read = asyncio.create_task(loop.__anext__())
    try:
        await asyncio.wait_for(read_started.wait(), 1)
        await asyncio.wait_for(
            device._retry_on_failed_connection(
                lambda: api.set_multiple_values({"1": True}, nowait=True),
                "write failed",
            ),
            0.5,
        )
        peer.settimeout(0.5)
        assert peer.recv(4096)
        assert api.connection_timeout == 5
    finally:
        device._running = False
        peer.close()
        read.cancel()
        with pytest.raises(asyncio.CancelledError):
            await read
        reader.close()


@pytest.fixture
def transport_gateway(patched_hass, mocker):
    """Real TinyTuya children, with executor jobs allowed to run concurrently."""
    parent = tinytuya.Device("gateway", "127.0.0.1", "0123456789abcdef")
    parent.set_version(3.4)
    patched_hass.data[DOMAIN]["gateway"] = {
        "tuyadevice": parent,
        "tuyadevicelock": asyncio.Lock(),
    }
    a = TuyaLocalDevice("A", "gateway", "host", "key", 3.4, "cid-a", patched_hass)
    b = TuyaLocalDevice("B", "gateway", "host", "key", 3.4, "cid-b", patched_hass)
    b._api_protocol_version_index = 0
    b._api_protocol_working = True
    b._gateway_receiver.devices.update({"cid-a": a, "cid-b": b})

    async def executor_job(func, *args):
        return await asyncio.to_thread(func, *args)

    mocker.patch.object(
        patched_hass, "async_add_executor_job", side_effect=executor_job
    )
    mocker.patch.object(
        patched_hass, "async_create_task", side_effect=asyncio.create_task
    )
    return parent, a, b


def gateway_frame(
    parent, dps, protocol=None, *, cid="cid-b", command=None, sequence=0, nested=True
):
    values = {"dps": dps}
    if cid is not None:
        values["cid"] = cid
    data = {"data": values} if nested else values
    if command is None:
        command = CT.STATUS if protocol == 4 else CT.DP_QUERY_NEW
    if protocol is not None:
        data["protocol"] = protocol
    payload = AESCipher(parent.local_key).encrypt(json.dumps(data).encode(), False)
    return pack_message(
        TuyaMessage(sequence, command, 0, b"\x00\x00\x00\x00" + payload, 0),
        hmac_key=parent.local_key,
    )


@pytest.mark.asyncio
async def test_gateway_receives_fragmented_frames_without_shortening_timeout(
    transport_gateway, mocker
):
    parent, a, b = transport_gateway
    reader, peer = socket.socketpair()
    parent.socket = reader
    parent.set_socketPersistent(True)
    parent.set_socketTimeout(5)
    first = gateway_frame(parent, {"1": "first"}, 4)
    second = gateway_frame(parent, {"1": "second"}, 4)
    delivered = asyncio.Event()
    entity = mocker.MagicMock()
    entity.on_receive.side_effect = lambda *_: (
        delivered.set() if entity.on_receive.call_count == 2 else None
    )
    b._children = [entity]
    reading = ThreadEvent()
    original_receive = parent._receive

    def receive_frame():
        reading.set()
        return original_receive()

    mocker.patch.object(parent, "_receive", side_effect=receive_frame)
    peer.sendall(first[:20])
    receiver = b._gateway_receiver
    receiver.start(a)
    receiver.start(b)
    try:
        assert await asyncio.to_thread(reading.wait, 1)
        await asyncio.sleep(0.3)
        peer.sendall(first[20:] + second)
        await asyncio.wait_for(delivered.wait(), 1)
        assert entity.on_receive.call_args_list == [
            mocker.call({"1": "first"}, False),
            mocker.call({"1": "second"}, False),
        ]
        assert reader.gettimeout() == parent.connection_timeout == 5
    finally:
        peer.close()
        await receiver.stop(a)
        await receiver.stop(b)
        reader.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["status", "updatedps"])
@pytest.mark.parametrize("persistent", [False, True])
async def test_gateway_poll_preserves_socket_until_solicited_reply(
    transport_gateway, mocker, method, persistent
):
    parent, _, b = transport_gateway
    reader, peer = socket.socketpair()
    parent.socket = reader
    parent.socketPersistent = persistent
    parent.set_socketTimeout(5)
    b._poll_only = not persistent
    mocker.patch.object(b._api, "generate_payload", return_value=b"query")
    entity = mocker.MagicMock()
    b._children = [entity]
    push = gateway_frame(parent, {"1": "scene"}, 4)
    peer.sendall(push + push + gateway_frame(parent, {"2": True}))
    original_receive = parent._receive

    def receive_frame():
        assert parent.socket is reader
        assert parent.socketPersistent
        return original_receive()

    receive = mocker.patch.object(parent, "_receive", side_effect=receive_frame)
    poll = b._api.status if method == "status" else lambda: b._api.updatedps([1])
    try:
        result = await b._poll_with_gateway_pushes(poll, "poll failed")
        assert result["dps"] == {"2": True}
        assert receive.call_count == 3
        peer.settimeout(0.5)
        assert peer.recv(4096) == b"query"
        assert entity.on_receive.call_args_list == [
            mocker.call({"1": "scene"}, False),
            mocker.call({"1": "scene"}, False),
        ]
        assert parent.socketPersistent is persistent
        assert (parent.socket is reader) is persistent
        assert b._api_working_protocol_failures == 0
    finally:
        parent.set_socketPersistent(False)
        reader.close()
        peer.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["status", "updatedps"])
@pytest.mark.parametrize("attempts", [1, 2])
async def test_gateway_poll_timeout_retires_stream_before_unlocking(
    transport_gateway, mocker, method, attempts
):
    parent, a, b = transport_gateway
    reader, peer = socket.socketpair()
    retry_reader, retry_peer = socket.socketpair()
    parent.socket = reader
    parent.set_socketPersistent(True)
    parent.set_socketTimeout(0.05)
    retry_reader.settimeout(0.5)
    b._SINGLE_PROTO_CONNECTION_ATTEMPTS = attempts
    mocker.patch.object(b._api, "generate_payload", return_value=b"query")
    peer.sendall(gateway_frame(parent, {"1": "scene"}, 4))
    retry_peer.sendall(gateway_frame(parent, {"2": "fresh"}))
    entity = mocker.MagicMock()
    b._children = [entity]
    receiver = b._gateway_receiver
    original_receive = b._api.receive

    def timed_out_receive():
        result = original_receive()
        assert result is None
        # This late reply must be discarded with the failed connection.
        peer.sendall(gateway_frame(parent, {"1": "scene", "2": "stale"}))
        return result

    mocker.patch.object(b._api, "receive", side_effect=timed_out_receive)

    def get_socket(_):
        if parent.socket is None:
            parent.socket = retry_reader
        return True

    mocker.patch.object(parent, "_get_socket", side_effect=get_socket)
    closed_under_lock = []
    original_persistence = parent.set_socketPersistent

    def set_persistence(value):
        if not value and parent.socket is reader:
            closed_under_lock.append(receiver.lock.locked())
        original_persistence(value)

    mocker.patch.object(parent, "set_socketPersistent", side_effect=set_persistence)
    gateway_receive = mocker.spy(parent, "receive")
    poll = b._api.status if method == "status" else lambda: b._api.updatedps([1])
    try:
        result = await b._poll_with_gateway_pushes(poll, "poll failed")
        assert closed_under_lock == [True]
        assert reader.fileno() == -1
        assert parent.socket is None
        if attempts == 1:
            assert result is None
            assert b._api_working_protocol_failures == 1
        else:
            assert result["dps"] == {"2": "fresh"}
            assert b._api_working_protocol_failures == 0
            retry_peer.settimeout(0.5)
            assert retry_peer.recv(4096) == b"query"
        entity.on_receive.assert_called_once_with({"1": "scene"}, False)
        receiver.start(a)
        receiver.start(b)
        await asyncio.sleep(0.12)
        gateway_receive.assert_not_called()
    finally:
        await receiver.stop(a)
        await receiver.stop(b)
        reader.close()
        peer.close()
        retry_reader.close()
        retry_peer.close()


@pytest.mark.asyncio
async def test_gateway_routes_sequential_updates_to_matching_child(
    patched_hass, mocker
):
    """One gateway reader delivers transient updates without child socket reads."""
    selector = mocker.patch("custom_components.tuya_local.device.DefaultSelector")
    selector.return_value.__enter__.return_value.select.return_value = [True]
    parent = mocker.MagicMock()
    parent.parent = None
    parent.socket = object()
    parent.socketPersistent = True
    parent.connection_timeout = 5
    parent.received_wrong_cid_queue = []
    child_apis = {}

    def make_api(dev_id, *args, **kwargs):
        if "parent" not in kwargs:
            return parent
        child = mocker.MagicMock()
        child.parent = parent
        child_apis[dev_id] = child
        return child

    mocker.patch("tinytuya.Device", side_effect=make_api)
    mocker.patch.object(
        patched_hass, "async_create_task", side_effect=asyncio.create_task
    )
    a = TuyaLocalDevice("A", "gateway", "host", "key", 3.4, "cid-a", patched_hass)
    b = TuyaLocalDevice("B", "gateway", "host", "key", 3.4, "cid-b", patched_hass)
    assert a._gateway_receiver is b._gateway_receiver
    receiver = a._gateway_receiver
    entity_a = mocker.MagicMock()
    entity_b = mocker.MagicMock()
    second_event = asyncio.Event()
    entity_b.on_receive.side_effect = lambda *_: (
        second_event.set() if entity_b.on_receive.call_count == 2 else None
    )
    a._children = [entity_a]
    b._children = [entity_b]
    packets = iter(
        [
            _GatewayPacket(
                {"protocol": 4, "data": {"dps": {"1": "scene"}, "cid": "cid-b"}},
                CT.STATUS,
                0,
                True,
            )
            for _ in range(2)
        ]
    )
    reads = 0

    def receive():
        nonlocal reads
        reads += 1
        packet = next(packets)
        if reads == 2:
            parent.socket = None
        return packet

    parent.receive.side_effect = receive
    receiver.start(a)
    receiver.start(b)
    try:
        await asyncio.wait_for(second_event.wait(), 1)
        assert parent.receive.call_count == 2
        assert entity_a.on_receive.call_count == 0
        assert entity_b.on_receive.call_args_list == [
            mocker.call({"1": "scene"}, False),
            mocker.call({"1": "scene"}, False),
        ]
        assert b.get_property("1") == "scene"
        child_apis["cid-a"].receive.assert_not_called()
        child_apis["cid-b"].receive.assert_not_called()
        await receiver.stop(a)
        assert receiver.task is not None
    finally:
        await receiver.stop(b)
    assert receiver.task is None


@pytest.mark.asyncio
async def test_gateway_delivers_other_child_response_queued_during_poll(
    patched_hass, mocker
):
    """A response TinyTuya queued during a child poll reaches its target."""
    selector = mocker.patch("custom_components.tuya_local.device.DefaultSelector")
    selector.return_value.__enter__.return_value.select.return_value = [True]
    parent = mocker.MagicMock()
    parent.parent = None
    parent.socket = object()
    parent.socketPersistent = True
    parent.connection_timeout = 5
    parent.received_wrong_cid_queue = [
        (
            None,
            _GatewayPacket(
                {"protocol": 4, "data": {"dps": {"1": "press"}, "cid": "cid-b"}},
                CT.STATUS,
                0,
                True,
            ),
        )
    ]
    mocker.patch("tinytuya.Device", side_effect=lambda *args, **kwargs: parent)
    mocker.patch.object(
        patched_hass, "async_create_task", side_effect=asyncio.create_task
    )
    a = TuyaLocalDevice("A", "gateway", "host", "key", 3.4, "cid-a", patched_hass)
    b = TuyaLocalDevice("B", "gateway", "host", "key", 3.4, "cid-b", patched_hass)
    entity_b = mocker.MagicMock()
    delivered = asyncio.Event()
    entity_b.on_receive.side_effect = lambda *_: delivered.set()
    b._children = [entity_b]
    parent.receive.side_effect = lambda: setattr(parent, "socket", None)
    receiver = a._gateway_receiver
    receiver.start(a)
    receiver.start(b)
    try:
        await asyncio.wait_for(delivered.wait(), 1)
        entity_b.on_receive.assert_called_once_with({"1": "press"}, False)
        assert parent.received_wrong_cid_queue == []
    finally:
        await receiver.stop(a)
        await receiver.stop(b)


@pytest.mark.asyncio
async def test_tinytuya_wrong_cid_response_is_routed_to_other_child(
    patched_hass, mocker
):
    """Exercise TinyTuya's real wrong-CID queue before gateway dispatch."""
    parent = tinytuya.Device("gateway", "127.0.0.1", "0123456789abcdef")
    patched_hass.data[DOMAIN]["gateway"] = {
        "tuyadevice": parent,
        "tuyadevicelock": asyncio.Lock(),
    }
    a = TuyaLocalDevice("A", "gateway", "host", "key", 3.4, "cid-a", patched_hass)
    b = TuyaLocalDevice("B", "gateway", "host", "key", 3.4, "cid-b", patched_hass)
    parent.set_socketPersistent(True)
    mocker.patch.object(
        parent,
        "_decode_payload",
        return_value={"protocol": 4, "data": {"dps": {"1": "scene"}, "cid": "cid-b"}},
    )
    mocker.patch.object(parent, "_send_receive", return_value={"dps": {"1": "a"}})

    parent.raw_recv = [SimpleNamespace(cmd=CT.STATUS, seqno=0)]
    a._gateway_receiver.read(
        lambda: parent._process_message(
            SimpleNamespace(payload=b"packet"), from_child=a._api
        )
    )

    assert len(parent.received_wrong_cid_queue) == 1
    assert parent.received_wrong_cid_queue[0][0] is b._api
    entity_b = mocker.MagicMock()
    delivered = asyncio.Event()
    entity_b.on_receive.side_effect = lambda *_: delivered.set()
    b._children = [entity_b]
    parent.socket = None
    mocker.patch.object(
        patched_hass, "async_create_task", side_effect=asyncio.create_task
    )
    receiver = a._gateway_receiver
    receiver.start(a)
    receiver.start(b)
    try:
        await asyncio.wait_for(delivered.wait(), 1)
        entity_b.on_receive.assert_called_once_with({"1": "scene"}, False)
    finally:
        await receiver.stop(a)
        await receiver.stop(b)


@pytest.mark.asyncio
@pytest.mark.parametrize("force_dps", [False, True])
async def test_same_cid_push_consumed_by_poll_triggers_repeated_event(
    patched_hass, mocker, force_dps
):
    """Polls drain intervening pushes before releasing the shared socket."""
    parent = tinytuya.Device("gateway", "127.0.0.1", "0123456789abcdef")
    parent.set_version(3.4)
    patched_hass.data[DOMAIN]["gateway"] = {
        "tuyadevice": parent,
        "tuyadevicelock": asyncio.Lock(),
    }
    a = TuyaLocalDevice("A", "gateway", "host", "key", 3.4, "cid-a", patched_hass)
    b = TuyaLocalDevice("B", "gateway", "host", "key", 3.4, "cid-b", patched_hass)
    receiver = b._gateway_receiver

    config = get_config("linkoze_dual_button")
    event_config = next(e for e in config.all_entities() if e.entity == "event")
    event_config._config["dps"][0]["mapping"][0]["dps_val"] = "scene"
    event = TuyaLocalEvent(b, event_config)
    trigger = mocker.patch.object(event, "_trigger_event")
    mocker.patch.object(event, "schedule_update_ha_state")
    polled = asyncio.Event()
    state_entity = mocker.MagicMock()
    state_entity.on_receive.side_effect = lambda dps, _: (
        polled.set() if "2" in dps else None
    )
    b._children = [event, state_entity]
    b._cached_state = {"1": "scene", "updated_at": time()}
    event.on_receive({"1": "scene"}, False)
    trigger.reset_mock()

    cipher = AESCipher(parent.local_key)

    def response(data):
        return SimpleNamespace(
            payload=cipher.encrypt(json.dumps(data).encode(), False),
            cmd=CT.STATUS if data.get("protocol") == 4 else CT.DP_QUERY_NEW,
            seqno=0,
            retcode=0,
        )

    push = {"protocol": 4, "data": {"dps": {"1": "scene"}, "cid": "cid-b"}}
    reply = response({"data": {"dps": {"2": True}, "cid": "cid-b"}})
    frames = iter([response(push), response(push), reply])
    reply_pending = ThreadEvent()
    release_reply = ThreadEvent()

    def receive_frame():
        frame = next(frames)
        if frame is reply:
            reply_pending.set()
            if not release_reply.wait(2):
                raise TimeoutError("test did not release the solicited reply")
            # Leave the receiver with no socket to read after the poll completes.
            parent.socketPersistent = False
        return frame

    mocker.patch.object(parent, "_get_socket", return_value=True)
    mocker.patch.object(parent, "_receive", side_effect=receive_frame)
    mocker.patch.object(b._api, "generate_payload", return_value=b"query")
    status = mocker.spy(b._api, "status")
    updatedps = mocker.spy(b._api, "updatedps")
    child_receive = mocker.spy(b._api, "receive")
    gateway_receive = mocker.spy(parent, "receive")
    handle_poll = mocker.spy(b, "_handle_poll")

    async def executor_job(func, *args):
        return await asyncio.to_thread(func, *args)

    mocker.patch.object(
        patched_hass, "async_add_executor_job", side_effect=executor_job
    )
    mocker.patch.object(
        patched_hass, "async_create_task", side_effect=asyncio.create_task
    )
    parent.socket = None
    parent.socketPersistent = True
    receiver.start(a)
    receiver.start(b)
    await asyncio.sleep(0)
    socket = mocker.MagicMock()
    parent.socket = socket
    b._running = True
    b._api_protocol_version_index = 0
    b._api_protocol_working = True
    b._last_full_poll = 0
    if force_dps:
        b._force_dps = [1]
    loop_task = asyncio.create_task(b.receive_loop())
    try:
        assert await asyncio.to_thread(reply_pending.wait, 1)
        receiver._wake.set()
        await asyncio.sleep(0.15)
        assert receiver.task is not None
        assert receiver.lock.locked()
        assert receiver.lock._waiters  # The gateway reader is waiting for the poll.
        gateway_receive.assert_not_called()
        assert child_receive.call_count == 2
        socket.sendall.assert_called_once_with(b"query")

        release_reply.set()
        await asyncio.wait_for(polled.wait(), 1)
        assert handle_poll.call_args_list == [
            mocker.call({"1": "scene"}, False, from_poll=False),
            mocker.call({"1": "scene"}, False, from_poll=False),
            mocker.call({"2": True}, not force_dps, from_poll=True),
        ]
        assert trigger.call_count == 2
        assert parent._receive.call_count == 3
        assert a.get_property("1") is None
        assert status.call_count == (0 if force_dps else 1)
        assert updatedps.call_count == (1 if force_dps else 0)
        socket.sendall.assert_called_once_with(b"query")
    finally:
        release_reply.set()
        b._running = False
        loop_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await loop_task
        await receiver.stop(a)
        await receiver.stop(b)


@pytest.mark.asyncio
async def test_gateway_child_poll_loop_never_reads_shared_socket(patched_hass, mocker):
    parent = mocker.MagicMock()
    parent.parent = None
    parent.socket = object()
    parent.socketPersistent = True

    def make_api(dev_id, *args, **kwargs):
        if "parent" not in kwargs:
            return parent
        child = mocker.MagicMock()
        child.parent = parent
        return child

    mocker.patch("tinytuya.Device", side_effect=make_api)
    TuyaLocalDevice("A", "gateway", "host", "key", 3.4, "cid-a", patched_hass)
    b = TuyaLocalDevice("B", "gateway", "host", "key", 3.4, "cid-b", patched_hass)
    b._running = True
    b._cached_state = {"1": "initial", "updated_at": time()}
    b._last_full_poll = time()
    loop = b.async_receive()
    read = asyncio.create_task(loop.__anext__())
    await asyncio.sleep(0.12)
    b._running = False
    with pytest.raises(StopAsyncIteration):
        await read
    b._api.receive.assert_not_called()
    parent.receive.assert_not_called()


@pytest.mark.asyncio
async def test_gateway_receiver_stops_only_after_last_child(patched_hass, mocker):
    parent = mocker.MagicMock()
    parent.parent = None
    parent.socket = None
    mocker.patch("tinytuya.Device", side_effect=lambda *args, **kwargs: parent)
    mocker.patch.object(
        patched_hass, "async_create_task", side_effect=asyncio.create_task
    )
    a = TuyaLocalDevice("A", "gateway", "host", "key", 3.4, "cid-a", patched_hass)
    b = TuyaLocalDevice("B", "gateway", "host", "key", 3.4, "cid-b", patched_hass)
    receiver = a._gateway_receiver
    receiver.start(a)
    receiver.start(b)

    await a.async_stop()
    assert receiver.devices == {"cid-b": b}
    assert receiver.task is not None
    parent.set_socketPersistent.assert_not_called()

    await b.async_stop()
    assert receiver.devices == {}
    assert receiver.task is None
    parent.set_socketPersistent.assert_called_once_with(False)


@pytest.mark.asyncio
async def test_gateway_receiver_does_not_restart_on_hass_shutdown(patched_hass, mocker):
    parent = mocker.MagicMock()
    parent.parent = None
    parent.socket = None
    parent.received_wrong_cid_queue = []
    mocker.patch("tinytuya.Device", side_effect=lambda *args, **kwargs: parent)
    mocker.patch.object(
        patched_hass, "async_create_task", side_effect=asyncio.create_task
    )
    child = TuyaLocalDevice("B", "gateway", "host", "key", 3.4, "cid-b", patched_hass)
    receiver = child._gateway_receiver
    receiver.start(child)
    task = receiver.task
    await asyncio.sleep(0)
    patched_hass.is_stopping = True
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert receiver.task is None
    assert patched_hass.async_create_task.call_count == 1
    await receiver.stop(child)
    parent.set_socketPersistent.assert_called_once_with(False)


@pytest.mark.asyncio
async def test_gateway_receiver_disconnects_with_only_poll_only_child(
    patched_hass, mocker
):
    parent = mocker.MagicMock()
    parent.parent = None
    parent.socket = None
    mocker.patch("tinytuya.Device", side_effect=lambda *args, **kwargs: parent)
    mocker.patch.object(
        patched_hass, "async_create_task", side_effect=asyncio.create_task
    )
    active = TuyaLocalDevice("A", "gateway", "host", "key", 3.4, "cid-a", patched_hass)
    poll_only = TuyaLocalDevice(
        "B", "gateway", "host", "key", 3.4, "cid-b", patched_hass, poll_only=True
    )
    receiver = active._gateway_receiver
    receiver.start(active)
    receiver.start(poll_only)

    await receiver.stop(active)

    assert receiver.devices == {"cid-b": poll_only}
    assert receiver.task is None
    parent.set_socketPersistent.assert_called_once_with(False)


@pytest.mark.asyncio
async def test_gateway_child_joining_during_idle_disconnect_keeps_socket(
    patched_hass, mocker
):
    parent = mocker.MagicMock()
    parent.parent = None
    parent.socket = None
    parent.received_wrong_cid_queue = []
    mocker.patch("tinytuya.Device", side_effect=lambda *args, **kwargs: parent)
    mocker.patch.object(
        patched_hass, "async_create_task", side_effect=asyncio.create_task
    )
    a = TuyaLocalDevice("A", "gateway", "host", "key", 3.4, "cid-a", patched_hass)
    b = TuyaLocalDevice("B", "gateway", "host", "key", 3.4, "cid-b", patched_hass)
    receiver = a._gateway_receiver
    receiver.devices["cid-a"] = a

    await receiver.lock.acquire()
    stop_a = asyncio.create_task(receiver.stop(a))
    try:
        await asyncio.sleep(0)
        assert receiver.devices == {}
        assert not stop_a.done()
        receiver.start(b)
    finally:
        receiver.lock.release()

    await stop_a
    assert receiver.devices == {"cid-b": b}
    parent.set_socketPersistent.assert_not_called()
    await receiver.stop(b)
    parent.set_socketPersistent.assert_called_once_with(False)


@pytest.mark.asyncio
async def test_gateway_stop_waits_for_read_and_restarts_for_new_child(
    patched_hass, mocker
):
    """A replacement reader cannot overlap a still-running executor read."""
    selector = mocker.patch("custom_components.tuya_local.device.DefaultSelector")
    selector.return_value.__enter__.return_value.select.return_value = [True]
    parent = mocker.MagicMock()
    parent.parent = None
    parent.socket = object()
    parent.socketPersistent = True
    parent.connection_timeout = 5
    parent.received_wrong_cid_queue = []
    reading = ThreadEvent()
    release = ThreadEvent()

    def receive():
        reading.set()
        release.wait(2)
        parent.socket = None

    parent.receive.side_effect = receive
    mocker.patch("tinytuya.Device", side_effect=lambda *args, **kwargs: parent)
    mocker.patch.object(
        patched_hass, "async_create_task", side_effect=asyncio.create_task
    )

    async def executor_job(func, *args):
        return await asyncio.to_thread(func, *args)

    mocker.patch.object(
        patched_hass, "async_add_executor_job", side_effect=executor_job
    )
    a = TuyaLocalDevice("A", "gateway", "host", "key", 3.4, "cid-a", patched_hass)
    b = TuyaLocalDevice("B", "gateway", "host", "key", 3.4, "cid-b", patched_hass)
    receiver = a._gateway_receiver
    receiver.start(a)
    stop_a = None
    try:
        assert await asyncio.to_thread(reading.wait, 1)
        stop_a = asyncio.create_task(receiver.stop(a))
        await asyncio.sleep(0)
        assert not stop_a.done()
        assert receiver.lock.locked()
        receiver.start(b)
        assert parent.receive.call_count == 1
        release.set()
        await asyncio.wait_for(stop_a, 1)
        assert receiver.task is not None
        parent.set_socketPersistent.assert_not_called()
    finally:
        release.set()
        if stop_a:
            await stop_a
        await receiver.stop(b)

    assert receiver.task is None
    parent.set_socketPersistent.assert_called_once_with(False)


@pytest.mark.asyncio
async def test_gateway_pause_waits_for_in_flight_receive(patched_hass, mocker):
    selector = mocker.patch("custom_components.tuya_local.device.DefaultSelector")
    selector.return_value.__enter__.return_value.select.return_value = [True]
    parent = mocker.MagicMock()
    parent.parent = None
    parent.socket = object()
    parent.socketPersistent = True
    parent.connection_timeout = 5
    parent.received_wrong_cid_queue = []
    reading = ThreadEvent()
    release = ThreadEvent()

    def receive():
        reading.set()
        release.wait(2)
        parent.socket = None

    parent.receive.side_effect = receive
    mocker.patch("tinytuya.Device", side_effect=lambda *args, **kwargs: parent)
    mocker.patch.object(
        patched_hass, "async_create_task", side_effect=asyncio.create_task
    )

    async def executor_job(func, *args):
        return await asyncio.to_thread(func, *args)

    mocker.patch.object(
        patched_hass, "async_add_executor_job", side_effect=executor_job
    )
    child = TuyaLocalDevice("A", "gateway", "host", "key", 3.4, "cid-a", patched_hass)
    receiver = child._gateway_receiver
    receiver.start(child)
    pause = None
    try:
        assert await asyncio.to_thread(reading.wait, 1)
        pause = asyncio.create_task(child.async_pause())
        await asyncio.sleep(0)
        assert not pause.done()
        parent.set_socketPersistent.assert_not_called()
        release.set()
        await asyncio.wait_for(pause, 1)
        parent.set_socketPersistent.assert_called_once_with(False)
    finally:
        release.set()
        if pause:
            await pause
        await receiver.stop(child)


@pytest.mark.asyncio
async def test_concurrent_children_share_one_gateway_and_release_last_cache(
    patched_hass, mocker
):
    """Concurrent executor setup cannot publish separate parents or locks."""
    parents = []

    def make_api(dev_id, *args, **kwargs):
        api = mocker.MagicMock()
        if "parent" in kwargs:
            api.parent = kwargs["parent"]
        else:
            api.parent = None
            parents.append(api)
            blocking_sleep(0.02)
        return api

    mocker.patch("tinytuya.Device", side_effect=make_api)

    def config(cid):
        return {
            CONF_NAME: cid,
            CONF_DEVICE_ID: "gateway",
            CONF_DEVICE_CID: cid,
            CONF_HOST: "host",
            CONF_LOCAL_KEY: "key",
            CONF_PROTOCOL_VERSION: 3.4,
            CONF_POLL_ONLY: False,
        }

    a_config, b_config = config("cid-a"), config("cid-b")
    a, b = await asyncio.gather(
        asyncio.to_thread(setup_device, patched_hass, a_config),
        asyncio.to_thread(setup_device, patched_hass, b_config),
    )

    assert len(parents) == 1
    assert a._api.parent is b._api.parent is parents[0]
    assert a._api_lock is b._api_lock
    assert a._gateway_receiver is b._gateway_receiver
    assert a._gateway_receiver.members == {a, b}

    await async_delete_device(patched_hass, a_config)
    assert "gateway" in patched_hass.data[DOMAIN]
    await async_delete_device(patched_hass, b_config)
    assert "gateway" not in patched_hass.data[DOMAIN]


def test_failed_child_construction_removes_orphan_gateway(patched_hass, mocker):
    parent = mocker.MagicMock()
    parent.parent = None

    def make_api(dev_id, *args, **kwargs):
        if "parent" in kwargs:
            raise RuntimeError("child construction failed")
        return parent

    mocker.patch("tinytuya.Device", side_effect=make_api)
    config = {
        CONF_NAME: "Child",
        CONF_DEVICE_ID: "gateway",
        CONF_DEVICE_CID: "cid-a",
        CONF_HOST: "host",
        CONF_LOCAL_KEY: "key",
        CONF_PROTOCOL_VERSION: 3.4,
        CONF_POLL_ONLY: False,
    }

    with pytest.raises(RuntimeError, match="child construction failed"):
        setup_device(patched_hass, config)

    assert "gateway" not in patched_hass.data[DOMAIN]


def test_gateway_receiver_registers_running_parent_on_child_start(
    patched_hass, mock_api, mocker
):
    gateway = TuyaLocalDevice(
        "Gateway", "gateway", "host", "key", 3.4, None, patched_hass
    )
    patched_hass.data[DOMAIN]["gateway"]["device"] = gateway
    gateway._running = True
    child = TuyaLocalDevice(
        "Child", "gateway", "host", "key", 3.4, "cid-a", patched_hass
    )
    receiver = child._gateway_receiver
    assert receiver.devices == {}

    def close_task(coro):
        coro.close()
        return mocker.MagicMock()

    patched_hass.async_create_task.side_effect = close_task
    child.actually_start()

    assert receiver.devices == {None: gateway, "cid-a": child}
    assert patched_hass.async_create_task.call_count == 2
    child._shutdown_listener()
    child._refresh_task = None
    receiver.task = None


def test_should_poll(subject):
    subject._cached_state = {"1": "sample", "updated_at": time()}
    subject._poll_only = False
    subject._temporary_poll = False

    # Test temporary poll via pause/resume
    assert not subject.should_poll
    subject.pause()
    assert subject.should_poll
    subject.resume()
    assert not subject.should_poll

    # Test configured polling
    subject._poll_only = True
    assert subject.should_poll
    subject._poll_only = False

    # Test initial polling
    subject._cached_state = {}
    assert subject.should_poll


@pytest.mark.asyncio
async def test_refresh_error_reports_device_error_code(subject, mock_api, caplog):
    """The error code and message returned by the device are logged."""
    subject._api_protocol_working = False
    subject._protocol_configured = "3.3"
    mock_api().status.return_value = {
        "Error": "Check device key or version",
        "Err": "914",
        "Payload": None,
    }

    with caplog.at_level(logging.ERROR):
        await subject.async_refresh()

    assert "914" in caplog.text
    assert "Check device key or version" in caplog.text
    # 914 is ambiguous, so the possible causes are spelled out
    assert "power cycled" in caplog.text


@pytest.mark.asyncio
async def test_refresh_error_without_error_code_is_unchanged(subject, mock_api, caplog):
    """A failure that is not a device error logs the bare message."""
    subject._api_protocol_working = False
    subject._protocol_configured = "3.3"
    mock_api().status.side_effect = Exception("connection refused")

    with caplog.at_level(logging.ERROR):
        await subject.async_refresh()

    assert "Failed to refresh device state for Some name." in caplog.text
    assert "Device reported error" not in caplog.text


@pytest.mark.asyncio
async def test_refresh_error_914_stays_quiet_when_rotating_protocols(
    subject, mock_api, caplog
):
    """914 is expected while auto-detecting, so it must not log at error."""
    subject._api_protocol_working = False
    subject._protocol_configured = "auto"
    mock_api().status.return_value = {
        "Error": "Check device key or version",
        "Err": "914",
        "Payload": None,
    }

    with caplog.at_level(logging.ERROR):
        await subject.async_refresh()

    assert caplog.text == ""


@pytest.fixture
def gateway_scene_events(transport_gateway, mocker):
    """Use the custom BSEED event semantics, without a hardware-specific fix."""
    _, a, b = transport_gateway
    triggers = []
    for device in (a, b):
        config = get_config("linkoze_dual_button")
        event_config = next(e for e in config.all_entities() if e.entity == "event")
        dp = event_config._config["dps"][0]
        dp.update(persist=False, optional=True)
        dp["mapping"] = [{"dps_val": "scene", "value": "pressed"}]
        event = TuyaLocalEvent(device, event_config)
        triggers.append(mocker.patch.object(event, "_trigger_event"))
        mocker.patch.object(event, "schedule_update_ha_state")
        device._children = [event]
        device._api_protocol_version_index = 0
        device._api_protocol_working = True
    return triggers


@pytest.mark.parametrize("method", ["status", "updatedps"])
@pytest.mark.parametrize("echoed_status", [False, True])
async def test_gateway_scene_pushes_and_stale_poll_reply_share_transaction(
    transport_gateway, gateway_scene_events, mocker, method, echoed_status, caplog
):
    """Nested protocol-4 poll replies cannot turn stale scene data into presses."""
    parent, a, b = transport_gateway
    trigger_a, trigger_b = gateway_scene_events
    receiver = b._gateway_receiver
    reader, peer = socket.socketpair()
    parent.socket = reader
    parent.set_socketPersistent(True)
    parent.set_socketTimeout(2)
    b._running = True
    b._cached_state = {"1": "scene", "updated_at": time()}
    b._last_full_poll = 0
    if method == "updatedps":
        b._force_dps = [1, 2]
    original_decode = parent._decode_payload
    original_receive = parent._receive
    reply_pending = ThreadEvent()
    reads = 0

    def receive_frame():
        nonlocal reads
        reads += 1
        if reads == 3:
            reply_pending.set()
        return original_receive()

    mocker.patch.object(parent, "_receive", side_effect=receive_frame)
    query = mocker.spy(b._api, method)
    gateway_read = mocker.spy(parent, "receive")
    polled = asyncio.Event()
    state = mocker.MagicMock()
    state.on_receive.side_effect = lambda dps, _: polled.set() if "2" in dps else None
    b._children.append(state)
    caplog.set_level(logging.DEBUG, logger="custom_components.tuya_local.device")
    loop_task = asyncio.create_task(b.receive_loop())
    try:
        # The real TinyTuya request has been encoded and sent before either push.
        peer.settimeout(1)
        assert await asyncio.to_thread(peer.recv, 4096)
        push = gateway_frame(parent, {"1": "scene"}, 4)
        peer.sendall(push + push)
        assert await asyncio.to_thread(reply_pending.wait, 1)
        receiver.start(a)
        receiver.start(b)
        await asyncio.sleep(0.05)
        assert receiver.lock.locked()
        assert receiver.lock._waiters
        gateway_read.assert_not_called()
        request = parent.raw_sent
        assert request.cmd == (
            CT.UPDATEDPS if method == "updatedps" else CT.DP_QUERY_NEW
        )
        if echoed_status:
            # Preserve the original nested STATUS/matching-sequence case. Its
            # wire markers also describe a genuine report, so it must survive
            # as a third press; the actual query reply is still pending.
            peer.sendall(
                gateway_frame(
                    parent,
                    {"1": "scene", "2": True},
                    4,
                    command=CT.STATUS,
                    sequence=request.seqno,
                )
            )
        peer.sendall(
            gateway_frame(
                parent,
                {"1": "scene", "2": True},
                4,
                command=request.cmd,
                sequence=request.seqno,
            )
        )
        await asyncio.wait_for(polled.wait(), 1)
        trigger_a.assert_not_called()
        assert trigger_b.call_count == (3 if echoed_status else 2)
        assert state.on_receive.call_args_list == [
            mocker.call({"1": "scene"}, False),
            mocker.call({"1": "scene"}, False),
            *([mocker.call({"1": "scene", "2": True}, False)] if echoed_status else []),
            mocker.call({"1": "scene", "2": True}, True),
        ]
        query.assert_called_once()
        assert reads == (4 if echoed_status else 3)
        assert b.get_property("2") is True
        assert b._api_working_protocol_failures == 0
        assert parent._decode_payload == original_decode
        assert "path=poll-interleaved source=push cmd=8 seq=0" in caplog.text
        assert "received source=poll" in caplog.text
    finally:
        b._running = False
        peer.close()
        loop_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await loop_task
        await receiver.stop(a)
        await receiver.stop(b)
        reader.close()


@pytest.mark.parametrize("method", ["status", "updatedps"])
@pytest.mark.parametrize("command", [CT.DP_QUERY_NEW, CT.STATUS])
async def test_gateway_flat_poll_reply_with_stale_scene_is_state_only(
    transport_gateway, gateway_scene_events, mocker, method, command
):
    """Flat command-16 snapshots and legacy echoed STATUS replies are polls."""
    parent, _, b = transport_gateway
    _, trigger = gateway_scene_events
    reader, peer = socket.socketpair()
    parent.socket = reader
    parent.set_socketPersistent(True)
    parent.set_socketTimeout(1)
    b._running = True
    b._cached_state = {"1": "scene", "updated_at": time()}
    b._last_full_poll = 0
    if method == "updatedps":
        b._force_dps = [1, 2]
    query = mocker.spy(b._api, method)
    state = mocker.MagicMock()
    b._children.append(state)
    receive = b.async_receive()
    try:
        peer.sendall(
            gateway_frame(
                parent,
                {"1": "scene", "2": True},
                command=command,
                sequence=parent.seqno,
                nested=False,
            )
        )
        result = await asyncio.wait_for(receive.__anext__(), 1)
        assert result == {
            "1": "scene",
            "2": True,
            "full_poll": method == "status",
            "from_poll": True,
        }
        full_poll = result.pop("full_poll")
        from_poll = result.pop("from_poll")
        b._handle_poll(result, full_poll, from_poll=from_poll)
        trigger.assert_not_called()
        state.on_receive.assert_called_once_with({"1": "scene", "2": True}, True)
        assert b.get_property("2") is True
        query.assert_called_once()
    finally:
        b._running = False
        await receive.aclose()
        parent.set_socketPersistent(False)
        reader.close()
        peer.close()


@pytest.mark.parametrize("method", ["status", "updatedps"])
async def test_scene_to_relay_mode_does_not_replay_polled_scene(
    transport_gateway, gateway_scene_events, mocker, method
):
    """Stale optional/nonpersistent DP1 cannot fire on any subsequent poll."""
    parent, _, b = transport_gateway
    _, trigger = gateway_scene_events
    b._handle_poll({"1": "scene", "18": "scene_1"}, False)
    b._handle_poll({"18": "switch_1"}, False)
    assert trigger.call_count == 1
    reader, peer = socket.socketpair()
    parent.socket = reader
    parent.set_socketPersistent(True)
    parent.set_socketTimeout(1)
    query = mocker.spy(b._api, method)
    poll = b._api.status if method == "status" else lambda: b._api.updatedps([1, 2])
    try:
        for n in range(3):
            peer.sendall(
                gateway_frame(
                    parent,
                    {"1": "scene", "18": "switch_1", "2": n},
                    4,
                    command=CT.UPDATEDPS if method == "updatedps" else CT.DP_QUERY_NEW,
                    sequence=parent.seqno,
                )
            )
            result = await b._poll_with_gateway_pushes(poll, "poll failed")
            b._handle_poll(result["dps"], method == "status", from_poll=True)
            assert trigger.call_count == 1
            assert b.get_property("18") == "switch_1"
            assert b.get_property("2") == n
        assert query.call_count == 3
        assert b._api_working_protocol_failures == 0
    finally:
        parent.set_socketPersistent(False)
        reader.close()
        peer.close()


async def test_gateway_wrong_cid_queue_keeps_packet_origin_and_delivers_once(
    transport_gateway, gateway_scene_events, mocker, caplog
):
    """TinyTuya's recursive reads must not replace queued frame metadata."""
    parent, a, b = transport_gateway
    trigger_a, trigger_b = gateway_scene_events
    receiver = a._gateway_receiver
    reader, peer = socket.socketpair()
    parent.socket = reader
    parent.set_socketPersistent(True)
    parent.set_socketTimeout(1)
    push = gateway_frame(parent, {"1": "scene"}, 4)
    peer.sendall(
        push
        + gateway_frame(parent, {"1": "scene", "2": True}, 4, command=CT.DP_QUERY_NEW)
        + push
        + gateway_frame(parent, {"2": False}, cid="cid-a")
    )
    caplog.set_level(logging.DEBUG, logger="custom_components.tuya_local.device")
    delivered = asyncio.Event()
    trigger_b.side_effect = lambda *_: (
        delivered.set() if trigger_b.call_count == 2 else None
    )
    try:
        reply = await a._poll_with_gateway_pushes(a._api.status, "poll failed")
        assert reply["dps"] == {"2": False}
        queued = list(parent.received_wrong_cid_queue)
        assert len(queued) == 3
        assert [p.unsolicited for _, p in queued] == [True, False, True]
        assert queued[0][1] is not queued[2][1]  # Identical bytes are separate frames.
        # Simulate the same decoded frame also reaching the immediate route.
        receiver.dispatch(queued[0][1], "poll-interleaved")
        receiver.start(a)
        receiver.start(b)
        await asyncio.wait_for(delivered.wait(), 1)
        assert parent.received_wrong_cid_queue == []
        assert trigger_b.call_count == 2
        trigger_a.assert_not_called()
        assert b.get_property("2") is True
        receiver.dispatch(reply, "receive")  # Poll results are already claimed.
        assert "already delivered" in caplog.text
        assert "path=cid-queue source=poll cmd=16" in caplog.text
    finally:
        peer.close()
        await receiver.stop(a)
        await receiver.stop(b)
        reader.close()


async def test_gateway_parent_poll_routes_child_state_without_event(
    transport_gateway, gateway_scene_events, patched_hass, mocker
):
    """A gateway-wide query must not apply child DPS to the gateway entity."""
    parent, a, b = transport_gateway
    _, trigger = gateway_scene_events
    gateway = TuyaLocalDevice(
        "Gateway", "gateway", "host", "key", 3.4, None, patched_hass
    )
    gateway._api_protocol_working = True
    gateway._api_protocol_version_index = 0
    receiver = gateway._gateway_receiver
    reader, peer = socket.socketpair()
    parent.socket = reader
    parent.set_socketPersistent(True)
    parent.set_socketTimeout(1)
    peer.sendall(
        gateway_frame(parent, {"1": "scene"}, 4)
        + gateway_frame(parent, {"1": "scene", "2": True}, 4, command=CT.DP_QUERY_NEW)
    )
    query = mocker.spy(parent, "status")
    try:
        result = await gateway._poll_with_gateway_pushes(parent.status, "poll failed")
        assert result is None
        query.assert_called_once()
        assert trigger.call_count == 1
        assert b.get_property("2") is True
        assert gateway.get_property("1") is None
        assert a.get_property("1") is None
        receiver.start(a)
        receiver.start(b)
        # All packets were consumed by the transaction. The reader has none to replay.
        await asyncio.sleep(0.12)
        assert trigger.call_count == 1
    finally:
        peer.close()
        await receiver.stop(a)
        await receiver.stop(b)
        reader.close()


async def test_gateway_late_poll_reply_and_unknown_packet_are_state_only(
    transport_gateway, gateway_scene_events
):
    parent, a, b = transport_gateway
    _, trigger = gateway_scene_events
    receiver = b._gateway_receiver
    reader, peer = socket.socketpair()
    parent.socket = reader
    parent.set_socketPersistent(True)
    parent.set_socketTimeout(1)
    try:
        # Metadata-free protocol-4 JSON is insufficient evidence of a fresh press.
        receiver.dispatch(
            {"protocol": 4, "data": {"cid": "cid-b", "dps": {"1": "scene"}}}
        )
        trigger.assert_not_called()
        peer.sendall(
            gateway_frame(parent, {"1": "scene", "2": True}, 4, command=CT.UPDATEDPS)
        )
        async with receiver.lock:
            packet = await asyncio.to_thread(receiver.read, parent.receive)
        receiver.dispatch(packet)
        trigger.assert_not_called()
        assert b.get_property("2") is True
        # A subsequent identical DP1 from a genuine STATUS frame remains a press.
        peer.sendall(gateway_frame(parent, {"1": "scene"}, 4))
        async with receiver.lock:
            packet = await asyncio.to_thread(receiver.read, parent.receive)
        receiver.dispatch(packet)
        receiver.dispatch(packet, "cid-queue")
        assert trigger.call_count == 1
    finally:
        parent.set_socketPersistent(False)
        reader.close()
        peer.close()


@pytest.mark.parametrize("method", ["status", "updatedps"])
async def test_gateway_protocol4_poll_reply_is_not_retried_as_scene(
    transport_gateway, gateway_scene_events, mocker, method
):
    """Previously three retries accumulated three stale 'pushes' for one poll."""
    parent, _, b = transport_gateway
    _, trigger = gateway_scene_events
    pairs = [socket.socketpair() for _ in range(3)]
    available = iter(reader for reader, _ in pairs)
    for reader, peer in pairs:
        reader.settimeout(0.01)
        peer.sendall(
            gateway_frame(
                parent,
                {"1": "scene"},
                4,
                command=CT.UPDATEDPS if method == "updatedps" else CT.DP_QUERY_NEW,
            )
        )
    parent.set_socketPersistent(True)

    def get_socket(_):
        if parent.socket is None:
            parent.socket = next(available)
        return True

    mocker.patch.object(parent, "_get_socket", side_effect=get_socket)
    query = mocker.spy(b._api, method)
    poll = b._api.status if method == "status" else lambda: b._api.updatedps([1])
    try:
        reply = await b._poll_with_gateway_pushes(poll, "poll failed")
        trigger.assert_not_called()
        assert reply["dps"] == {"1": "scene"}
        query.assert_called_once()
        assert b._api_working_protocol_failures == 0
    finally:
        parent.set_socketPersistent(False)
        for reader, peer in pairs:
            reader.close()
            peer.close()


def test_partial_poll_suppresses_events_without_clearing_other_dps(
    transport_gateway, gateway_scene_events
):
    _, _, b = transport_gateway
    _, trigger = gateway_scene_events
    b._handle_poll({"1": "scene"}, False)
    b._handle_poll({"2": True}, False, from_poll=True)
    assert b.get_property("1") == "scene"
    assert trigger.call_count == 1
    b._handle_poll({"1": "scene"}, False, from_poll=True)
    assert trigger.call_count == 1
    b._handle_poll({"2": False}, True)
    assert b.get_property("1") is None
    assert b.get_property("2") is False


async def test_bseed_capture_queued_snapshot_and_two_identical_presses(
    transport_gateway, gateway_scene_events, mocker, caplog
):
    """Replay the 2026-10-08 BSEED trace; only the two command-8 frames fire.

    CIDs are anonymized. Command numbers, sequences, JSON shapes and DPS
    are from the physical trace. A's reply completes the simulated other-child
    transaction; the supplied trace did not contain that reply.
    """
    parent, a, b = transport_gateway
    trigger_a, trigger_b = gateway_scene_events
    receiver = b._gateway_receiver
    snapshot = {
        "1": "scene",
        "18": "scene_1",
        "24": False,
        "30": 0,
        "36": True,
        "37": "relay",
        "38": "memory",
    }

    def frame(sequence, command, payload):
        encrypted = AESCipher(parent.local_key).encrypt(
            json.dumps(payload).encode(), False
        )
        return pack_message(
            TuyaMessage(sequence, command, 0, b"\x00\x00\x00\x00" + encrypted, 0),
            hmac_key=parent.local_key,
        )

    # Remember a prior scene press, as in the trace's same-value log message.
    b._handle_poll({"1": "scene"}, False)
    trigger_b.reset_mock()
    handle = mocker.spy(b, "_handle_poll")
    state_received = asyncio.Event()
    first_press = asyncio.Event()
    second_press = asyncio.Event()
    state = mocker.MagicMock()
    state.on_receive.side_effect = lambda *_: state_received.set()
    b._children.append(state)
    trigger_b.side_effect = lambda *_: (
        first_press.set() if trigger_b.call_count == 1 else second_press.set()
    )
    caplog.set_level(logging.DEBUG, logger="custom_components.tuya_local.device")
    reader, peer = socket.socketpair()
    parent.socket = reader
    parent.set_socketPersistent(True)
    parent.set_socketTimeout(2)
    peer.settimeout(1)
    loop_task = None
    try:
        # 16:34:39: B's flat command-16 snapshot is consumed by A's status().
        peer.sendall(
            frame(75729, CT.DP_QUERY_NEW, {"dps": snapshot, "cid": b.dev_cid})
            + frame(75730, CT.DP_QUERY_NEW, {"dps": {"2": False}, "cid": a.dev_cid})
        )
        result = await a._poll_with_gateway_pushes(a._api.status, "poll failed")
        assert result["dps"] == {"2": False}
        assert peer.recv(4096)  # Drain A's outgoing query.
        assert len(parent.received_wrong_cid_queue) == 1
        assert parent.received_wrong_cid_queue[0][0] is b._api
        receiver.start(a)
        receiver.start(b)
        await asyncio.wait_for(state_received.wait(), 1)
        trigger_b.assert_not_called()
        state.on_receive.assert_called_once_with(snapshot, True)
        assert handle.call_args_list == [mocker.call(snapshot, False, from_poll=True)]
        assert parent.received_wrong_cid_queue == []
        assert b.get_property("36") is True
        assert "path=cid-queue source=poll cmd=16 seq=75729" in caplog.text

        # 16:37:13: B's own status reply reaches the child receive_loop.
        b._running = True
        b._last_full_poll = 0
        loop_task = asyncio.create_task(b.receive_loop())
        assert await asyncio.to_thread(peer.recv, 4096)
        peer.sendall(
            frame(76170, CT.DP_QUERY_NEW, {"dps": snapshot, "cid": b.dev_cid})
            + frame(
                76171,
                CT.STATUS,
                {
                    "protocol": 4,
                    "t": 1791470233,
                    "data": {"dps": {"1": "scene"}, "cid": b.dev_cid},
                },
            )
        )
        await asyncio.wait_for(first_press.wait(), 1)
        assert trigger_b.call_count == 1

        # 16:37:16: identical DP1 in a distinct command-8 frame is another press.
        peer.sendall(
            frame(
                76174,
                CT.STATUS,
                {
                    "protocol": 4,
                    "t": 1791470235,
                    "data": {"dps": {"1": "scene"}, "cid": b.dev_cid},
                },
            )
        )
        await asyncio.wait_for(second_press.wait(), 1)
        assert trigger_b.call_count == 2
        trigger_a.assert_not_called()
        assert state.on_receive.call_args_list == [
            mocker.call(snapshot, True),
            mocker.call(snapshot, True),
            mocker.call({"1": "scene"}, False),
            mocker.call({"1": "scene"}, False),
        ]
        assert handle.call_args_list[1] == mocker.call(snapshot, True, from_poll=True)
        assert "source=push cmd=8 seq=76171 protocol=4" in caplog.text
        assert "source=push cmd=8 seq=76174 protocol=4" in caplog.text
    finally:
        b._running = False
        if loop_task:
            loop_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await loop_task
        await receiver.stop(a)
        await receiver.stop(b)
        reader.close()
        peer.close()


@pytest.mark.parametrize("method", ["status", "updatedps"])
@pytest.mark.parametrize("push_dps", [{"1": "scene"}, {"1": "scene", "2": True}])
async def test_bseed_nonzero_push_sequence_cannot_be_mistaken_for_poll_reply(
    transport_gateway, gateway_scene_events, mocker, method, push_dps
):
    """A sequence coincidence cannot override a command-8 protocol-4 report."""
    parent, _, b = transport_gateway
    _, trigger = gateway_scene_events
    reader, peer = socket.socketpair()
    parent.socket = reader
    parent.set_socketPersistent(True)
    parent.set_socketTimeout(1)
    parent.seqno = 76171
    peer.sendall(
        gateway_frame(parent, push_dps, 4, sequence=76171)
        + gateway_frame(
            parent,
            {"2": True},
            command=(CT.DP_QUERY_NEW if method == "status" else CT.UPDATEDPS),
            sequence=76171,
        )
    )
    query = mocker.spy(b._api, method)
    poll = b._api.status if method == "status" else lambda: b._api.updatedps([1])
    try:
        reply = await b._poll_with_gateway_pushes(poll, "poll failed")
        assert parent.raw_sent.seqno == 76171
        assert trigger.call_count == 1
        assert reply["dps"] == {"2": True}
        query.assert_called_once()
    finally:
        parent.set_socketPersistent(False)
        reader.close()
        peer.close()


@pytest.mark.parametrize("method", ["status", "updatedps"])
@pytest.mark.parametrize("replace_socket", [False, True])
async def test_gateway_queued_packets_cannot_complete_next_child_poll(
    transport_gateway, gateway_scene_events, mocker, method, replace_socket
):
    """An older queued reply cannot satisfy a new request after a scene push.

    A's poll queues a stale B snapshot and a genuine B report. B polls before
    the receiver drains that queue, then gets another report ahead of its own
    reply. TinyTuya receive() must not return the old queued snapshot as B's
    new reply, including after the shared socket has been replaced.
    """
    parent, a, b = transport_gateway
    trigger_a, trigger_b = gateway_scene_events
    receiver = b._gateway_receiver
    reader, peer = socket.socketpair()
    replacement_reader, replacement_peer = socket.socketpair()
    parent.socket = reader
    parent.set_socketPersistent(True)
    parent.set_socketTimeout(1)
    snapshot = {"1": "scene", "18": "scene_1", "24": False}
    peer.sendall(
        gateway_frame(
            parent, snapshot, command=CT.DP_QUERY_NEW, sequence=75729, nested=False
        )
        + gateway_frame(parent, {"1": "scene"}, 4, sequence=76171)
        + gateway_frame(parent, {"24": False}, cid=a.dev_cid)
    )
    active_peer = peer
    polling = None
    reading = ThreadEvent()
    release = ThreadEvent()
    state = mocker.MagicMock()
    b._children.append(state)
    try:
        a_reply = await a._poll_with_gateway_pushes(a._api.status, "poll failed")
        assert a_reply["dps"] == {"24": False}
        assert len(parent.received_wrong_cid_queue) == 2
        if replace_socket:
            await receiver.disconnect()
            assert reader.fileno() == -1
            # TinyTuya retains decoded CID packets when closing a socket.
            assert len(parent.received_wrong_cid_queue) == 2
            parent.socket = replacement_reader
            active_peer = replacement_peer
            parent.set_socketPersistent(True)
            parent.set_socketTimeout(1)
        active_peer.sendall(
            gateway_frame(parent, {"1": "scene"}, 4, sequence=76174)
            + gateway_frame(
                parent,
                {"1": "scene", "24": True},
                command=CT.DP_QUERY_NEW if method == "status" else CT.UPDATEDPS,
                sequence=parent.seqno,
                nested=False,
            )
        )
        original_receive = parent._receive
        reads = 0

        def receive_frame():
            nonlocal reads
            reads += 1
            if reads == 1:
                reading.set()
                assert release.wait(1)
            return original_receive()

        mocker.patch.object(parent, "_receive", side_effect=receive_frame)
        query = mocker.spy(b._api, method)
        gateway_read = mocker.spy(parent, "receive")
        poll = (
            b._api.status if method == "status" else lambda: b._api.updatedps([1, 24])
        )
        polling = asyncio.create_task(b._poll_with_gateway_pushes(poll, "poll failed"))
        assert await asyncio.to_thread(reading.wait, 1)
        receiver.start(a)
        receiver.start(b)
        await asyncio.sleep(0.02)
        assert receiver.lock.locked()
        assert receiver.lock._waiters
        gateway_read.assert_not_called()
        release.set()
        reply = await asyncio.wait_for(polling, 1)
        assert reply["dps"] == {"1": "scene", "24": True}
        assert reads == 2  # New push and the actual reply, with no second query.
        query.assert_called_once()
        b._handle_poll(reply["dps"], method == "status", from_poll=True)
        assert state.on_receive.call_args_list == [
            mocker.call(snapshot, True),
            mocker.call({"1": "scene"}, False),
            mocker.call({"1": "scene"}, False),
            mocker.call({"1": "scene", "24": True}, True),
        ]
        assert trigger_b.call_count == 2
        trigger_a.assert_not_called()
        assert parent.received_wrong_cid_queue == []
        assert b.get_property("24") is True
        gateway_read.assert_not_called()
    finally:
        release.set()
        if polling:
            await polling
        await receiver.stop(a)
        await receiver.stop(b)
        parent.set_socketPersistent(False)
        reader.close()
        peer.close()
        replacement_reader.close()
        replacement_peer.close()


async def test_concurrent_gateway_child_polls_preserve_ten_rapid_presses(
    transport_gateway, gateway_scene_events, mocker
):
    """Two waiting child polls plus the receiver deliver every report once."""
    parent, a, b = transport_gateway
    trigger_a, trigger_b = gateway_scene_events
    receiver = b._gateway_receiver
    reader, peer = socket.socketpair()
    parent.socket = reader
    parent.set_socketPersistent(True)
    parent.set_socketTimeout(1)
    peer.settimeout(1)
    query_a = mocker.spy(a._api, "status")
    query_b = mocker.spy(b._api, "status")
    state = mocker.MagicMock()
    b._children.append(state)
    tasks = [
        asyncio.create_task(a._poll_with_gateway_pushes(a._api.status, "poll failed")),
        asyncio.create_task(b._poll_with_gateway_pushes(b._api.status, "poll failed")),
    ]
    try:
        assert await asyncio.to_thread(peer.recv, 4096)  # A's request.
        receiver.start(a)
        receiver.start(b)
        await asyncio.sleep(0)
        assert receiver.lock.locked()
        assert len(receiver.lock._waiters) == 2  # B and the receiver.
        peer.sendall(
            gateway_frame(
                parent,
                {"1": "scene", "24": False},
                command=CT.DP_QUERY_NEW,
                sequence=75729,
                nested=False,
            )
            + b"".join(
                gateway_frame(parent, {"1": "scene"}, 4, sequence=76171 + n)
                for n in range(5)
            )
            + gateway_frame(parent, {"24": True}, cid=a.dev_cid)
        )
        assert await asyncio.to_thread(peer.recv, 4096)  # B's request.
        peer.sendall(
            b"".join(
                gateway_frame(parent, {"1": "scene"}, 4, sequence=76176 + n)
                for n in range(5)
            )
            + gateway_frame(
                parent,
                {"1": "scene", "24": True},
                4,
                command=CT.DP_QUERY_NEW,
                sequence=parent.raw_sent.seqno,
            )
        )
        replies = await asyncio.wait_for(asyncio.gather(*tasks), 1)
        for device, reply in zip((a, b), replies, strict=True):
            device._handle_poll(reply["dps"], True, from_poll=True)
        query_a.assert_called_once()
        query_b.assert_called_once()
        trigger_a.assert_not_called()
        assert trigger_b.call_count == 10
        assert [call.args[1] for call in state.on_receive.call_args_list] == (
            [True] + [False] * 10 + [True]
        )
        assert a.get_property("24") is True
        assert b.get_property("24") is True
        assert parent.received_wrong_cid_queue == []
    finally:
        peer.close()
        await asyncio.gather(*tasks, return_exceptions=True)
        await receiver.stop(a)
        await receiver.stop(b)
        reader.close()
