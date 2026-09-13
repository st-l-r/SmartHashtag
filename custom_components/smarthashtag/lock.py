"""Central locking support for Smart #1 / #3 / #5."""

from __future__ import annotations

import json
from datetime import timedelta
from time import monotonic
from typing import TYPE_CHECKING, Any

import httpx
from homeassistant.components.lock import LockEntity
from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import HomeAssistantError
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

        self._pending_target: bool | None = None
        self._pending_until = 0.0

    def _get_lock_state(self) -> bool | None:
        """Return the cached lock state without performing API I/O."""
        vehicle = self.coordinator.account.vehicles.get(self._vehicle_vin)
        if vehicle is None or vehicle.safety is None:
            return None

        safety = vehicle.safety

        # Confirmed on Smart #1:
        # centralLockingStatus 1 = locked, 0 = unlocked.
        central_state = safety.central_locking_status
        if central_state in (0, 1):
            return central_state == 1

        # Fallback for responses where the central state is not available.
        door_states = [
            safety.door_lock_status_driver,
            safety.door_lock_status_driver_rear,
            safety.door_lock_status_passenger,
            safety.door_lock_status_passenger_rear,
        ]
        known_states = [state for state in door_states if state in (0, 1)]

        if len(known_states) != len(door_states):
            return None

        return all(state == 1 for state in known_states)

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
        if self._pending_target is not None:
            self._clear_pending_command()

        await self._send_lock_command(lock)

        self._pending_target = lock
        self._pending_until = monotonic() + COMMAND_CONFIRM_TIMEOUT
        self.coordinator.set_update_interval(
            UPDATE_INTERVAL_KEY,
            timedelta(seconds=FAST_INTERVAL),
        )

        # Expose the locking/unlocking state immediately.
        self.async_write_ha_state()

        # Request fresh telemetry now; normal coordinator updates continue
        # at FAST_INTERVAL until the command is confirmed or times out.
        await self.coordinator.async_request_refresh()

    @callback
    def _handle_coordinator_update(self) -> None:
        """Handle fresh telemetry and stop fast polling when appropriate."""
        if self._pending_target is not None:
            current_state = self._get_lock_state()

            if current_state == self._pending_target:
                LOGGER.debug(
                    "Smart central-lock command confirmed by vehicle telemetry"
                )
                self._clear_pending_command()
            elif monotonic() >= self._pending_until:
                LOGGER.warning(
                    "Smart central-lock command was accepted by the cloud but "
                    "not confirmed by vehicle telemetry within %s seconds",
                    COMMAND_CONFIRM_TIMEOUT,
                )
                self._clear_pending_command()

        super()._handle_coordinator_update()

    def _clear_pending_command(self) -> None:
        """Clear pending state and restore the normal polling interval."""
        self._pending_target = None
        self._pending_until = 0.0
        self.coordinator.reset_update_interval(UPDATE_INTERVAL_KEY)

    @staticmethod
    def _build_payload(lock: bool) -> str:
        """Build the Smart telematics lock/unlock payload."""
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
            "serviceParameters": [{"key": "door", "value": "all"}],
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

        # Same SSL preparation used by pySmartHashtag's existing controls.
        await account._ensure_ssl_context()

        for attempt in range(1, MAX_COMMAND_ATTEMPTS + 1):
            try:
                # Remote-control requests require the VIN to be bound to the
                # current Smart API session.
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
                    LOGGER.info(
                        "Smart %s command accepted (serviceId=%s)",
                        action,
                        service_id,
                    )
                    return

                code = result.get("code")
                message = result.get("message") or "unknown cloud response"
                operation_result = (
                    result.get("data", {})
                    .get("serviceResult", {})
                    .get("operationResult")
                    if isinstance(result.get("data"), dict)
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
                    "Smart token expired during %s; refreshing before retry %d/%d",
                    action,
                    attempt + 1,
                    MAX_COMMAND_ATTEMPTS,
                )
                await account.config.authentication.refresh()

            except (
                SmartHumanCarConnectionError,
                SmartVehicleNotInUseError,
            ) as err:
                last_error = err
                if attempt >= MAX_COMMAND_ATTEMPTS:
                    break

                # The next iteration calls select_active_vehicle() again.
                LOGGER.debug(
                    "Smart VIN binding lost during %s; retrying %d/%d",
                    action,
                    attempt + 1,
                    MAX_COMMAND_ATTEMPTS,
                )

            except SmartNonceError as err:
                last_error = err
                if attempt >= MAX_COMMAND_ATTEMPTS:
                    break

                # The next iteration creates a fresh timestamp/signature.
                LOGGER.debug(
                    "Smart API nonce collision during %s; retrying %d/%d",
                    action,
                    attempt + 1,
                    MAX_COMMAND_ATTEMPTS,
                )

            except SmartVehicleUnboundError as err:
                raise HomeAssistantError(
                    "The Smart vehicle is no longer bound to this account"
                ) from err

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

            except httpx.HTTPError as err:
                raise HomeAssistantError(
                    f"Network error while trying to {action} the vehicle"
                ) from err

            except (ValueError, TypeError, KeyError) as err:
                raise HomeAssistantError(
                    f"Invalid Smart cloud response while trying to {action} the vehicle"
                ) from err

        raise HomeAssistantError(
            f"Unable to {action} the Smart vehicle after "
            f"{MAX_COMMAND_ATTEMPTS} attempts"
        ) from last_error
