"""Enrolment. The one unauthenticated write on the platform.

================================================================================
THE ORDERING RULE: CREATE THE BROKER CLIENT FIRST, COMMIT SECOND.

Of the two possible half-failures only ONE is recoverable, and that asymmetry is
the entire justification:

  broker created, commit fails
      -> an orphan dynsec client with no device row. A reconciliation sweep
         deletes it. Harmless.

  committed, broker create fails
      -> a device that believes it is enrolled, holding a secret that
         authenticates nothing, with its one-time token consumed. On a
         keyboard-less box in a basement, that is a SITE VISIT.

So the broker call happens between two transactions, and the token is CLAIMED
with a short lease rather than consumed up front: a 504 between the broker
command and the response would otherwise burn a credential the member never
received.
================================================================================

AND THE BRANCH THAT MAKES THE LEASE WORTH HAVING.

`createClient` is NOT idempotent - Phase 0 measured it answering "Client already
exists". So a retry inside the lease, or a re-claim after a timeout on a command
that actually succeeded server-side, MUST fall through to `setClientPassword`.
Without that branch the retry fails permanently and burns exactly the credential
the lease exists to protect. The plan's original text only mentioned
`setClientPassword` being idempotent and missed this.

================================================================================
NOTHING IN THIS MODULE MAY READ THE REQUEST CONTEXT.

`current_user_id`, `current_community_id`, `current_internal_community_id` and
`current_user_role` are ATTACKER-CONTROLLED here: `auth: false` removes the
producer of the `x-user-*` headers, not the gateway's input-header allow-list,
and that list is global with no per-service override. nginx blanks them on
`= /api/live-public/enroll`; this module simply never asks.

Every tenant value is discovered from the DEVICE ROW THE TOKEN POINTS AT, and
passed explicitly from there on - including to the audit log, which would
otherwise read the unset ContextVar and file the row against nothing.
================================================================================
"""

from __future__ import annotations

import datetime
import logging
import secrets
from typing import TYPE_CHECKING

from core.audit_log.actions import AuditActions
from core.audit_log.dtos import AuditLogInput
from core.audit_log.service import AuditLogService
from core.errors.errors import ErrorException
from domain import tokens
from domain.topics import status_topic, telemetry_topic
from ports.broker import (
    BrokerClientExistsError,
    BrokerError,
    BrokerUnavailableError,
    DeviceCredentials,
)
from shared.const import ROLE_DEVICE, DeviceStatus, FeatureName
from shared.custom_errors import errors

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from api.live.repository import EnrolmentRepository
    from ports.broker import DeviceBrokerPort
    from ports.crm_read import CrmReadPort
    from shared.models.local_models import DeviceModel

logger = logging.getLogger(__name__)

AUDIT_SOURCE = "live-data"

# The device password. 32 bytes of urlsafe base64 (~43 characters): long enough
# that the broker's bcrypt is the only thing standing between an attacker and a
# credential they already cannot guess, and short enough to sit in an ESP32's
# NVS beside the host and topics.
_PASSWORD_BYTES = 32


class EnrolmentResult:
    """What the device is told, once."""

    __slots__ = ("device", "password", "status_topic", "telemetry_topic")

    def __init__(self, device: DeviceModel, password: str, telemetry: str, status: str) -> None:
        self.device = device
        self.password = password
        self.telemetry_topic = telemetry
        self.status_topic = status


class EnrolmentService:
    def __init__(
        self,
        *,
        local_session: AsyncSession,
        crm_session: AsyncSession,
        repository: EnrolmentRepository,
        crm_read: CrmReadPort,
        broker: DeviceBrokerPort,
        lease_seconds: int,
    ) -> None:
        self._local = local_session
        self._crm = crm_session
        self._repo = repository
        self._crm_read = crm_read
        self._broker = broker
        self._lease_seconds = lease_seconds
        self._audit = AuditLogService(crm_session)

    async def enrol(
        self, *, supplied_token: str, connector_name: str, connector_version: str
    ) -> EnrolmentResult:
        token_hash = tokens.hash_supplied(supplied_token)
        if token_hash is None:
            # Malformed. Answered exactly as unknown is answered: telling a
            # caller when they have the SHAPE right is the feedback a search
            # needs.
            raise self._not_found()

        now = datetime.datetime.now(datetime.UTC)

        # ---- T1: claim ---------------------------------------------------
        claimed = await self._repo.claim_token(
            token_hash=token_hash, now=now, lease_seconds=self._lease_seconds
        )
        if claimed is None:
            # Unclaimable. Distinguish ONLY the genuinely concurrent case -
            # where 409 + Retry-After is actionable and the caller demonstrably
            # already holds a valid token. Asking the broader question "does a
            # row exist" would answer True for an expired or consumed token too,
            # and the 409-vs-400 split would then tell a guesser that their
            # guess named a real one.
            if await self._repo.has_live_claim(token_hash=token_hash, now=now):
                raise ErrorException(errors.live.ENROLMENT_IN_PROGRESS, status_code=409)
            raise self._not_found()
        # The claim must be durable BEFORE the broker call: that is what makes a
        # crash in between expire a lease rather than lose a token.
        await self._local.commit()

        device = await self._repo.get_device(claimed.id_device)
        if device is None:  # pragma: no cover - FK makes this unreachable
            raise self._not_found()
        if device.status == int(DeviceStatus.REVOKED):
            raise ErrorException(errors.live.DEVICE_NOT_FOUND, status_code=404)

        # THE SUBSCRIPTION CHECK, from the device row's community and never from
        # a header. `require_feature` cannot run on this leg at all - see
        # ports/crm_read.py for the two ways a naive version fails silently.
        subscribed = await self._crm_read.is_feature_active_unscoped(
            id_community=device.id_community, feature=FeatureName.LIVE_DATA.value
        )
        if not subscribed:
            # A DISTINCT code, unlike the token errors. The caller already holds
            # a valid token for a real device, so there is nothing left to leak -
            # and the remediation is different and actionable.
            raise ErrorException(errors.live.COMMUNITY_NOT_SUBSCRIBED, status_code=403)

        # ---- the broker, BEFORE the second commit ------------------------
        password = secrets.token_urlsafe(_PASSWORD_BYTES)
        credentials = DeviceCredentials(
            username=str(device.public_id),
            password=password,
            # Pinned at the broker: connecting with any other id is refused,
            # which enforces protocol 4.3 there rather than by asking connector
            # authors nicely.
            client_id=str(device.public_id),
        )
        try:
            await self._broker.create_device(credentials, ROLE_DEVICE)
        except BrokerClientExistsError:
            # THE RECOVERY PATH. The client exists because a previous attempt
            # got far enough to create it - a 3000 ms gateway cut on a command
            # that succeeded server-side, or a retry after the lease expired.
            # `setClientPassword` is idempotent, so this re-issues a working
            # credential instead of failing and burning the token.
            logger.info(
                "broker client already exists; setting a fresh password",
                extra={"operation": "enrol:already_exists", "device": str(device.public_id)},
            )
            await self._broker.set_device_password(credentials.username, password)
        except BrokerUnavailableError as exc:
            # The token stays CLAIMED and expires on its own. Nothing is
            # consumed, so the member can try again in half a minute.
            raise ErrorException(errors.live.BROKER_UNAVAILABLE, status_code=503) from exc
        except BrokerError as exc:
            raise ErrorException(errors.live.BROKER_COMMAND_FAILED, status_code=502) from exc

        # ---- T2: consume -------------------------------------------------
        claimed.consumed_at = datetime.datetime.now(datetime.UTC)
        device.status = int(DeviceStatus.ACTIVE)
        device.enrolled_at = claimed.consumed_at
        device.connector_name = connector_name
        device.connector_version = connector_version
        await self._local.commit()
        await self._local.refresh(device)

        await self._write_audit(device)

        return EnrolmentResult(
            device=device,
            password=password,
            telemetry=telemetry_topic(device.id_community, device.public_id),
            status=status_topic(device.id_community, device.public_id),
        )

    def _not_found(self) -> ErrorException:
        """ONE opaque answer for unknown, expired, consumed and malformed.

        They share a remediation - ask for a new code - and distinguishing them
        turns this endpoint into a token oracle. The only useful signal is the
        GLOBAL counter: a per-row attempt counter is meaningless because a guess
        never finds a row to count against.
        """
        # TODO(step 11): increment a global `live_data.enrol.token_not_found`
        # counter here. Deliberately not a per-row column.
        return ErrorException(errors.live.TOKEN_NOT_FOUND, status_code=400)

    async def _write_audit(self, device: DeviceModel) -> None:
        """File the enrolment against the device's community, EXPLICITLY.

        `id_community` is passed rather than left to the ContextVar, which is
        unset on this leg - exactly as the worker handlers in the sibling
        services do it. Without it the row would be filed against nothing and
        the forged-header security probe would pass for the wrong reason.
        """
        await self._audit.log(
            AuditLogInput(
                action=AuditActions.DEVICE_ENROLLED,
                entity_type="live_device",
                entity_id=str(device.public_id),
                payload={
                    "connector": device.connector_name,
                    "version": device.connector_version,
                },
                source=AUDIT_SOURCE,
            ),
            id_community=device.id_community,
        )
        try:
            await self._crm.commit()
        except Exception:
            # LOGGED, never silent. The local write is already durable and a
            # CRM hiccup must not turn a successful operation into a 500 the
            # operator retries - but an audit trail that stops existing with
            # no trace at all is its own incident.
            logger.exception("audit commit failed for enrolled device %s", device.public_id)
