"""Central locking support for Smart vehicles."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta
from time import monotonic
from typing import TYPE_CHECKING, Any, Callable
from urllib.parse import urlparse

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
        # Avoid putting a VIN into logs unnecessarily.
        LOGGER.error("Configured vehicle is not available; skipping lock setup")
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

        # Serialize commands issued through this entity. This avoids overlapping
        # lock/unlock requests from a double tap or competing HA automations.
        self._command_lock = asyncio.Lock()

        self._pending_target: bool | None = None
        self._pending_until = 0.0
        self._pending_timeout_cancel: Callable[[], None] | None = None

        # Security invariant:
        # after the cloud accepts a physical lock/unlock command, cached
        # pre-command telemetry must not be presented as a trustworthy state.
        # It becomes trustworthy again only after vehicle telemetry carries a
        # newer Safety timestamp than the snapshot observed before the command.
        self._telemetry_uncertain = False
        self._command_baseline_timestamp: datetime | None = None

    def _get_safety_timestamp(self) -> datetime | None:
        """Return the timestamp of the cached safety telemetry."""
        vehicle = self.coordinator.account.vehicles.get(self._vehicle_vin)
        if vehicle is None or vehicle.safety is None:
            return None

        timestamp = vehicle.safety.timestamp
        return timestamp if isinstance(timestamp, datetime) else None

    def _has_fresh_post_command_telemetry(self) -> bool:
        """Return whether safety telemetry is newer than the pre-command data."""
        current = self._get_safety_timestamp()
        if current is None:
            return False

        baseline = self._command_baseline_timestamp
        if baseline is None:
            # There was no timestamp before the command, so the first valid
            # timestamp seen afterwards is new information.
            return True

        return current > baseline

    def _get_raw_lock_state(self) -> bool | None:
        """Return cached lock telemetry without applying freshness rules."""
        vehicle = self.coordinator.account.vehicles.get(self._vehicle_vin)
        if vehicle is None or vehicle.safety is None:
            return None

        safety = vehicle.safety

        # Confirmed on Smart #1:
        # doorLockStatus* = 1 when locked, 0 when unlocked.
        #
        # Prefer all four individual door states. A mixed state is deliberately
        # reported as unknown rather than as unlocked or locked.
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

    def _get_lock_state(self) -> bool | None:
        """Return a trustworthy cached lock state, or unknown when stale."""
        if self._telemetry_uncertain:
            return None

        return self._get_raw_lock_state()

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
            and (
                self._telemetry_uncertain
                or self._get_raw_lock_state() is not True
            )
        )

    @property
    def is_unlocking(self) -> bool:
        """Return whether an unlock command is awaiting telemetry confirmation."""
        return (
            self._pending_target is False
            and monotonic() < self._pending_until
            and (
                self._telemetry_uncertain
                or self._get_raw_lock_state() is not False
            )
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

            # Capture the telemetry snapshot BEFORE the physical command is sent.
            # Any identical timestamp seen afterwards is still stale data.
            baseline_timestamp = self._get_safety_timestamp()

            await self._send_lock_command(lock)

            # The Smart cloud has accepted the command. From this point onward
            # the old cached state is no longer authoritative until a newer
            # Safety snapshot arrives.
            self._command_baseline_timestamp = baseline_timestamp
            self._telemetry_uncertain = True
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
            # the coordinator will continue retrying on its normal/fast schedule.
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
        if self._telemetry_uncertain and self._has_fresh_post_command_telemetry():
            # A newer vehicle Safety snapshot exists. Its lock state may or may
            # not match the requested target, but it is no longer stale.
            self._telemetry_uncertain = False
            LOGGER.debug("Fresh post-command Smart safety telemetry received")

        if self._pending_target is not None:
            current_state = self._get_lock_state()

            if current_state == self._pending_target:
                LOGGER.debug(
                    "Smart central-lock command confirmed by vehicle telemetry"
                )
                self._command_baseline_timestamp = None
                self._clear_pending_command()

        elif not self._telemetry_uncertain:
            # No command is pending and the state is trustworthy again.
            self._command_baseline_timestamp = None

        super()._handle_coordinator_update()

    @callback
    def _handle_pending_timeout(self, _now: Any) -> None:
        """Stop fast polling when telemetry did not confirm the command."""
        self._pending_timeout_cancel = None

        if self._pending_target is None:
            return

        if self._telemetry_uncertain:
            LOGGER.warning(
                "Smart central-lock command was accepted by the cloud but no "
                "fresh vehicle safety telemetry was received within %s seconds; "
                "lock state will remain unknown until fresh telemetry arrives",
                COMMAND_CONFIRM_TIMEOUT,
            )
        else:
            LOGGER.warning(
                "Smart central-lock command was accepted by the cloud but fresh "
                "vehicle telemetry did not confirm the requested state within "
                "%s seconds",
                COMMAND_CONFIRM_TIMEOUT,
            )
            # Fresh telemetry exists, so the current state can be trusted even
            # though the requested target was not reached.
            self._command_baseline_timestamp = None

        # Deliberately preserve _telemetry_uncertain when no fresh telemetry
        # arrived. That prevents an old pre-command "locked" value from
        # reappearing after an unconfirmed unlock operation.
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

    @staticmethod
    def _validate_remote_base_url(base_url: str) -> str:
        """Validate the endpoint before sending bearer credentials to it."""
        if not isinstance(base_url, str) or not base_url:
            raise HomeAssistantError("Smart remote-control API endpoint is unavailable")

        parsed = urlparse(base_url)

        # Access tokens used for physical vehicle control must never be sent
        # over cleartext HTTP or to a URL containing embedded user credentials.
        if (
            parsed.scheme.lower() != "https"
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
        ):
            raise HomeAssistantError(
                "Refusing Smart remote-control request to an insecure API endpoint"
            )

        return base_url.rstrip("/")

    @staticmethod
    def _get_model_code(vehicle: Any) -> str:
        """Return the vehicle matCode required for per-request VIN binding."""
        data = getattr(vehicle, "data", None)
        model_code = data.get("matCode") if isinstance(data, dict) else None

        if not isinstance(model_code, str) or not model_code.strip():
            # Fail closed: without the X-Vehicle-* headers the cloud may fall
            # back to the account-wide active-vehicle binding, which another
            # client can change between vehicle selection and command dispatch.
            raise HomeAssistantError(
                "Vehicle model code is unavailable; refusing remote lock command"
            )

        return model_code.strip()

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
                # The legacy active-vehicle binding is still performed because
                # the Smart cloud expects it for remote services.
                await account.select_active_vehicle(self._vehicle_vin)

                vehicle = account.vehicles.get(self._vehicle_vin)
                if vehicle is None:
                    raise HomeAssistantError(
                        "The configured Smart vehicle is currently unavailable"
                    )

                base_url = self._validate_remote_base_url(vehicle.base_url)
                model_code = self._get_model_code(vehicle)
                path = API_TELEMATICS_URL + self._vehicle_vin
                params = self._build_payload(lock)

                async with SmartClient(account.config) as client:
                    response = await client.put(
                        base_url + path,
                        headers={
                            **utils.generate_default_header(
                                client.config.authentication.device_id,
                                client.config.authentication.api_access_token,
                                params={},
                                method="PUT",
                                url=path,
                                body=params,
                                # Security hardening supplied by
                                # pySmartHashtag: bind this request explicitly
                                # to the intended VIN/model in addition to the
                                # mutable account-wide active-vehicle binding.
                                vin=self._vehicle_vin,
                                model_code=model_code,
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

                # Do not echo an arbitrary cloud-provided message into HA's
                # user-visible error path. The numeric result is sufficient for
                # diagnosis and avoids accidental disclosure of response data.
                raise HomeAssistantError(
                    f"Smart cloud rejected {action} command "
                    f"(code={code}, operationResult={operation_result})"
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
                # request timestamp/signature/nonce. The command itself also
                # carries explicit X-Vehicle-* binding headers.
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
                    f"Smart API error while trying to {action} the vehicle"
                ) from err

            except httpx.HTTPStatusError as err:
                raise HomeAssistantError(
                    f"Smart cloud rejected the {action} request"
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
