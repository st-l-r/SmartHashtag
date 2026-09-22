"""Central locking support for Smart vehicles."""

from __future__ import annotations

import asyncio
import json
from datetime import timedelta
from time import monotonic
from typing import TYPE_CHECKING, Any, Callable

import httpx
from homeassistant.components.lock import LockEntity
from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.event import async_call_later
from pysmarthashtag.api import utils
from pysmarthashtag.api.client import SmartClient
from pysmarthashtag.const import API_TELEMATICS_URL
from pysmarthashtag.models import (
    SmartAPIError,
    SmartHumanCarConnectionError,
    SmartMainTokenExpiredError,
    SmartNoPermissionError,
    SmartNonceError,
    SmartTokenRefreshNecessary,
    SmartVehicleNotInUseError,
    SmartVehicleUnboundError,
)

from .const import CONF_VEHICLE, FAST_INTERVAL, LOGGER
from .coordinator import SmartHashtagDataUpdateCoordinator
from .entity import SmartHashtagEntity

if TYPE_CHECKING:
    from . import SmartHashtagConfigEntry


UPDATE_INTERVAL_KEY = "central_lock"
COMMAND_CONFIRM_TIMEOUT = 60
MAX_COMMAND_ATTEMPTS = 3

SERVICE_ID_LOCK = "RDL_2"
SERVICE_ID_UNLOCK = "RDU_2"


async def async_setup_entry(
    hass: HomeAssistant,
    entry: SmartHashtagConfigEntry,
    async_add_entities,
) -> None:
    """Set up the Smart central-lock entity."""
    coordinator = entry.runtime_data
    vehicle_vin = coordinator.config_entry.data.get(CONF_VEHICLE)

    if not vehicle_vin:
        LOGGER.error("No vehicle configured; skipping lock setup")
        return

    vehicles = coordinator.account.vehicles or {}
    if vehicle_vin not in vehicles:
        LOGGER.error("Vehicle %s not available; skipping lock setup", vehicle_vin)
        return

    async_add_entities(
        [SmartCentralLock(coordinator, vehicle_vin)],
        update_before_add=True,
    )


class SmartCentralLock(SmartHashtagEntity, LockEntity):
    """Representation of the vehicle central locking."""

    _attr_icon = "mdi:car-door-lock"

    def __init__(
        self,
        coordinator: SmartHashtagDataUpdateCoordinator,
        vehicle_vin: str,
    ) -> None:
        """Initialize the central-lock entity."""
        super().__init__(coordinator)

        self._vehicle_vin = vehicle_vin
        self._attr_unique_id = f"{self._attr_unique_id}_central_lock"
        self._attr_name = "Central locking"

        self._command_lock = asyncio.Lock()
        self._pending_target: bool | None = None
        self._pending_until = 0.0
        self._pending_timeout_cancel: Callable[[], None] | None = None

    def _get_lock_state(self) -> bool | None:
        """Return the cached lock state without performing API I/O."""
        vehicle = self.coordinator.account.vehicles.get(self._vehicle_vin)
        if vehicle is None or vehicle.safety is None:
            return None

        safety = vehicle.safety

        # Confirmed on Smart #1:
        # doorLockStatus* = 1 when locked, 0 when unlocked.
        #
        # Prefer the individual door states when all four are available so
        # a mixed/stale state is not incorrectly reported as safely locked.
        door_states = [
            safety.door_lock_status_driver,
            safety.door_lock_status_driver_rear,
            safety.door_lock_status_passenger,
            safety.door_lock_status_passenger_rear,
        ]

        if all(state in (0, 1) for state in door_states):
            if all(state == 1 for state in door_states):
                return True

            if all(state == 0 for state in door_states):
                return False

            LOGGER.debug("Mixed Smart door lock states: %s", door_states)
            return None

        # Fallback confirmed on Smart #1:
        # centralLockingStatus = 1 when locked, 0 when unlocked.
        central_state = safety.central_locking_status
        if central_state in (0, 1):
            return central_state == 1

        return None

    @property
    def is_locked(self) -> bool | None:
        """Return whether the vehicle is centrally locked."""
        return self._get_lock_state()

    @property
    def is_locking(self) -> bool:
        """Return whether a lock command is awaiting telemetry confirmation."""
        return (
            self._pending_target is True
            and monotonic() < self._pending_until
            and self._get_lock_state() is not True
        )

    @property
    def is_unlocking(self) -> bool:
        """Return whether an unlock command is awaiting telemetry confirmation."""
        return (
            self._pending_target is False
            and monotonic() < self._pending_until
            and self._get_lock_state() is not False
        )

    async def async_lock(self, **kwargs: Any) -> None:
        """Lock all vehicle doors."""
        await self._execute_command(lock=True)

    async def async_unlock(self, **kwargs: Any) -> None:
        """Unlock all vehicle doors."""
        await self._execute_command(lock=False)

    async def _execute_command(self, lock: bool) -> None:
        """Send a command and temporarily poll quickly for confirmation."""
        async with self._command_lock:
            if self._pending_target is not None:
                self._clear_pending_command()

            await self._send_lock_command(lock)

            self._pending_target = lock
            self._pending_until = monotonic() + COMMAND_CONFIRM_TIMEOUT

            self.coordinator.set_update_interval(
                UPDATE_INTERVAL_KEY,
                timedelta(seconds=FAST_INTERVAL),
            )

            # Ensure fast polling is always reset even if no successful
            # coordinator update arrives during the confirmation window.
            self._pending_timeout_cancel = async_call_later(
                self.hass,
                COMMAND_CONFIRM_TIMEOUT,
                self._handle_pending_timeout,
            )

            # Expose LOCKING / UNLOCKING immediately in Home Assistant.
            self.async_write_ha_state()

            # Ask for fresh telemetry immediately. A failed refresh must not
            # turn an already accepted remote command into a service failure;
            # the coordinator will retry on its normal/fast schedule.
            try:
                await self.coordinator.async_request_refresh()
            except Exception as err:
                LOGGER.debug(
                    "Immediate telemetry refresh after Smart lock command failed: %s",
                    err,
                )

    @callback
    def _handle_coordinator_update(self) -> None:
        """Handle fresh telemetry and stop fast polling after confirmation."""
        if self._pending_target is not None:
            current_state = self._get_lock_state()

            if current_state == self._pending_target:
                LOGGER.debug(
                    "Smart central-lock command confirmed by vehicle telemetry"
                )
                self._clear_pending_command()

        super()._handle_coordinator_update()

    @callback
    def _handle_pending_timeout(self, _now: Any) -> None:
        """Stop fast polling when telemetry did not confirm the command."""
        self._pending_timeout_cancel = None

        if self._pending_target is None:
            return

        LOGGER.warning(
            "Smart central-lock command was accepted by the cloud but "
            "not confirmed by vehicle telemetry within %s seconds",
            COMMAND_CONFIRM_TIMEOUT,
        )

        self._clear_pending_command()
        self.async_write_ha_state()

    def _clear_pending_command(self) -> None:
        """Clear pending command state and restore the normal poll interval."""
        if self._pending_timeout_cancel is not None:
            self._pending_timeout_cancel()
            self._pending_timeout_cancel = None

        self._pending_target = None
        self._pending_until = 0.0
        self.coordinator.reset_update_interval(UPDATE_INTERVAL_KEY)

    async def async_will_remove_from_hass(self) -> None:
        """Clean up timers and polling changes when the entity is removed."""
        if self._pending_target is not None or self._pending_timeout_cancel is not None:
            self._clear_pending_command()

        await super().async_will_remove_from_hass()

    @staticmethod
    def _build_payload(lock: bool) -> str:
        """Build the confirmed Smart telematics lock/unlock payload."""
        payload = {
            "creator": "tc",
            "operationScheduling": {
                "duration": 6,
                "interval": 0,
                "occurs": 1,
                "recurrentOperation": False,
            },
            "serviceId": SERVICE_ID_LOCK if lock else SERVICE_ID_UNLOCK,
            "command": "start",
            "timestamp": utils.create_correct_timestamp(),
            "serviceParameters": [
                {
                    "key": "door",
                    "value": "all",
                }
            ],
        }

        return json.dumps(payload, separators=(",", ":"))

    @staticmethod
    def _command_succeeded(result: dict[str, Any]) -> bool:
        """Return whether the Smart cloud accepted the remote command."""
        data = result.get("data")
        if not isinstance(data, dict):
            return False

        service_result = data.get("serviceResult")
        if not isinstance(service_result, dict):
            return False

        return (
            result.get("success") is True
            and str(result.get("code")) == "1000"
            and service_result.get("operationResult") == 1
            and service_result.get("error") is None
        )

    async def _refresh_authentication(self, action: str) -> None:
        """Refresh Smart authentication using pySmartHashtag's refresh ladder."""
        try:
            # In pySmartHashtag 0.12.3 refresh() performs:
            # API-session refresh -> refresh-token exchange -> full login.
            await self.coordinator.account.config.authentication.refresh()
        except (SmartAPIError, httpx.HTTPError) as err:
            raise HomeAssistantError(
                f"Failed to renew Smart authentication while trying to {action} "
                "the vehicle"
            ) from err

    async def _send_lock_command(self, lock: bool) -> None:
        """Send a lock/unlock command to the Smart cloud."""
        account = self.coordinator.account
        action = "lock" if lock else "unlock"
        service_id = SERVICE_ID_LOCK if lock else SERVICE_ID_UNLOCK
        last_error: Exception | None = None

        if self._vehicle_vin not in account.vehicles:
            raise HomeAssistantError(
                "The configured Smart vehicle is currently unavailable"
            )

        # SmartClient uses the account's pre-created SSL context so Home
        # Assistant does not perform certificate setup in the event loop.
        await account._ensure_ssl_context()

        for attempt in range(1, MAX_COMMAND_ATTEMPTS + 1):
            try:
                # Remote commands require the VIN to be selected/bound to the
                # current Smart API session. select_active_vehicle() also has
                # its own bounded retry handling for session/binding failures.
                await account.select_active_vehicle(self._vehicle_vin)

                vehicle = account.vehicles.get(self._vehicle_vin)
                if vehicle is None:
                    raise HomeAssistantError(
                        "The configured Smart vehicle is currently unavailable"
                    )

                params = self._build_payload(lock)

                async with SmartClient(account.config) as client:
                    response = await client.put(
                        vehicle.base_url
                        + API_TELEMATICS_URL
                        + self._vehicle_vin,
                        headers={
                            **utils.generate_default_header(
                                client.config.authentication.device_id,
                                client.config.authentication.api_access_token,
                                params={},
                                method="PUT",
                                url=API_TELEMATICS_URL + self._vehicle_vin,
                                body=params,
                            )
                        },
                        content=params.encode("utf-8"),
                    )

                result = response.json()
                if not isinstance(result, dict):
                    raise HomeAssistantError(
                        f"Smart cloud returned an invalid {action} response"
                    )

                if self._command_succeeded(result):
                    LOGGER.debug(
                        "Smart %s command accepted (serviceId=%s)",
                        action,
                        service_id,
                    )
                    return

                code = result.get("code")
                message = result.get("message") or "unknown cloud response"

                data = result.get("data")
                service_result = (
                    data.get("serviceResult")
                    if isinstance(data, dict)
                    else None
                )
                operation_result = (
                    service_result.get("operationResult")
                    if isinstance(service_result, dict)
                    else None
                )

                raise HomeAssistantError(
                    f"Smart cloud rejected {action} command "
                    f"(code={code}, operationResult={operation_result}): {message}"
                )

            except (
                SmartTokenRefreshNecessary,
                SmartMainTokenExpiredError,
            ) as err:
                last_error = err
                if attempt >= MAX_COMMAND_ATTEMPTS:
                    break

                LOGGER.debug(
                    "Smart authentication expired during %s; "
                    "refreshing before retry %d/%d",
                    action,
                    attempt + 1,
                    MAX_COMMAND_ATTEMPTS,
                )
                await self._refresh_authentication(action)

            except (
                SmartHumanCarConnectionError,
                SmartVehicleNotInUseError,
                SmartNonceError,
            ) as err:
                last_error = err
                if attempt >= MAX_COMMAND_ATTEMPTS:
                    break

                # The next iteration re-selects the VIN and creates a fresh
                # request timestamp/signature/nonce.
                LOGGER.debug(
                    "Transient Smart API error during %s (%s); retrying %d/%d",
                    action,
                    type(err).__name__,
                    attempt + 1,
                    MAX_COMMAND_ATTEMPTS,
                )

            except SmartVehicleUnboundError as err:
                last_error = err
                if attempt >= MAX_COMMAND_ATTEMPTS:
                    break

                # The integration itself treats an isolated 8040 as possibly
                # transient immediately after session renewal. Use the same
                # bounded approach here; the next iteration selects the VIN.
                LOGGER.debug(
                    "Smart vehicle temporarily reported as unbound during %s; "
                    "retrying %d/%d",
                    action,
                    attempt + 1,
                    MAX_COMMAND_ATTEMPTS,
                )

            except SmartNoPermissionError as err:
                raise HomeAssistantError(
                    f"The Smart account has no permission to {action} this vehicle"
                ) from err

            except HomeAssistantError:
                raise

            except SmartAPIError as err:
                raise HomeAssistantError(
                    f"Smart API error while trying to {action} the vehicle: {err}"
                ) from err

            except httpx.HTTPStatusError as err:
                raise HomeAssistantError(
                    f"Smart cloud rejected the {action} request: {err}"
                ) from err

            except httpx.RequestError as err:
                raise HomeAssistantError(
                    f"Network error while trying to {action} the vehicle"
                ) from err

            except (ValueError, TypeError, KeyError) as err:
                raise HomeAssistantError(
                    f"Invalid Smart cloud response while trying to {action} the vehicle"
                ) from err

        if isinstance(last_error, SmartVehicleUnboundError):
            raise HomeAssistantError(
                "The Smart vehicle is reported as unbound from this account"
            ) from last_error

        raise HomeAssistantError(
            f"Unable to {action} the Smart vehicle after "
            f"{MAX_COMMAND_ATTEMPTS} attempts"
        ) from last_error
