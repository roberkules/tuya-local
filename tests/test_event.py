"""Tests for the event entity."""

from unittest.mock import AsyncMock, Mock

import pytest
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.tuya_local.const import (
    CONF_DEVICE_ID,
    CONF_PROTOCOL_VERSION,
    CONF_TYPE,
    DOMAIN,
)
from custom_components.tuya_local.event import (
    TuyaLocalEvent,
    async_setup_entry,
)
from custom_components.tuya_local.helpers.device_config import get_config


@pytest.mark.asyncio
async def test_init_entry(hass):
    """Test the initialisation."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={
            CONF_TYPE: "linkoze_dual_button",
            CONF_DEVICE_ID: "dummy",
            CONF_PROTOCOL_VERSION: "auto",
        },
    )
    m_add_entities = Mock()
    m_device = AsyncMock()

    hass.data[DOMAIN] = {
        "dummy": {"device": m_device},
    }

    await async_setup_entry(hass, entry, m_add_entities)
    assert type(hass.data[DOMAIN]["dummy"]["event_button_1"]) is TuyaLocalEvent
    m_add_entities.assert_called_once()


@pytest.mark.asyncio
async def test_init_entry_fails_if_device_has_no_event(hass):
    """Test initialisation when device has no matching entity"""
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={
            CONF_TYPE: "mirabella_genio_usb",
            CONF_DEVICE_ID: "dummy",
            CONF_PROTOCOL_VERSION: "auto",
        },
    )
    m_add_entities = Mock()
    m_device = AsyncMock()

    hass.data[DOMAIN] = {
        "dummy": {"device": m_device},
    }
    try:
        await async_setup_entry(hass, entry, m_add_entities)
        assert False
    except ValueError:
        pass
    m_add_entities.assert_not_called()


@pytest.mark.asyncio
async def test_init_entry_fails_if_config_is_missing(hass):
    """Test initialisation when device has no matching entity"""
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={
            CONF_TYPE: "non_existing",
            CONF_DEVICE_ID: "dummy",
            CONF_PROTOCOL_VERSION: "auto",
        },
    )
    m_add_entities = Mock()
    m_device = AsyncMock()

    hass.data[DOMAIN] = {
        "dummy": {"device": m_device},
    }
    try:
        await async_setup_entry(hass, entry, m_add_entities)
        assert False
    except ValueError:
        pass
    m_add_entities.assert_not_called()


def test_repeated_identical_pushes_trigger_distinct_events(mocker):
    """Repeated button presses must not be deduplicated as state updates."""
    config = get_config("linkoze_dual_button")
    event_config = next(e for e in config.all_entities() if e.entity == "event")
    device = mocker.MagicMock()
    device.get_property.return_value = "single_click"
    event = TuyaLocalEvent(device, event_config)
    trigger = mocker.patch.object(event, "_trigger_event")

    event.on_receive({"1": "single_click"}, False)
    event.on_receive({"1": "single_click"}, False)
    event.on_receive({"1": "single_click"}, True)

    assert trigger.call_count == 2


def test_poll_values_never_trigger_events(mocker):
    """Neither an initial nor a changed polled event value is a new press."""
    config = get_config("linkoze_dual_button")
    event_config = next(e for e in config.all_entities() if e.entity == "event")
    device = mocker.MagicMock()
    event = TuyaLocalEvent(device, event_config)
    trigger = mocker.patch.object(event, "_trigger_event")

    for value in ("single_click", "double_click", None, "single_click"):
        device.get_property.return_value = value
        event.on_receive({"1": value}, True)
    trigger.assert_not_called()

    device.get_property.return_value = "single_click"
    event.on_receive({"1": "single_click"}, False)
    event.on_receive({"1": "single_click"}, False)
    assert trigger.call_count == 2
