"""Orchestration for the device lifecycle: create, list, issue a token, revoke.

The service owns the commit. The repository stages; routes touch no session.

Two orderings here are load-bearing and neither is obvious:

  * CREATE stamps `capacity_kva` from the CRM at creation time, not at ingest
    time. A snapshot, because the worker must apply the `implausible_production`
    ceiling per message without a CRM round-trip - and because the value is what
    the meter declared when the device was set up, which is the thing an operator
    can reason about.

  * REVOKE is disable -> clear the retained status -> delete, in that order, and
    the MIDDLE step is the reason for the order. Phase 0 measured that
    `deleteClient` alone DOES cut a live connection (47 ms) and refuse
    reconnection, so the plan's stated justification - that only `disableClient`
    is documented to disconnect - was simply false. The order survives because
    the retained-status clear has to happen while we still have a device to
    clear it for, and doing it after the delete would race a reconnect.
"""

from __future__ import annotations

import datetime
import logging
import uuid
from typing import TYPE_CHECKING

from core.audit_log.actions import AuditActions
from core.audit_log.dtos import AuditLogInput
from core.audit_log.service import AuditLogService
from core.errors.errors import ErrorException
from domain import tokens
from domain.topics import status_topic
from ports.broker import BrokerClientNotFoundError, BrokerError, DeviceCredentials
from shared.const import ROLE_DEVICE, DeviceStatus, DeviceType, FeatureName
from shared.custom_errors import errors

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.ext.asyncio import AsyncSession

    from api.live.repository import LiveRepository
    from ports.broker import DeviceBrokerPort
    from ports.crm_read import CrmReadPort
    from shared.models.local_models import DeviceModel

logger = logging.getLogger(__name__)

AUDIT_SOURCE = "live-data"


class IssuedToken:
    """A token in plaintext, which exists for exactly one response.

    A tiny class rather than a tuple because the mapper reads two things off it
    and a positional pair would make a reordering silent.
    """

    __slots__ = ("expires_at", "token")

    def __init__(self, token: str, expires_at: datetime.datetime) -> None:
        self.token = token
        self.expires_at = expires_at


class LiveDataService:
    def __init__(
        self,
        *,
        local_session: AsyncSession,
        crm_session: AsyncSession,
        repository: LiveRepository,
        crm_read: CrmReadPort,
        broker: DeviceBrokerPort,
    ) -> None:
        self._local = local_session
        self._crm = crm_session
        self._repo = repository
        self._crm_read = crm_read
        self._broker = broker
        self._audit = AuditLogService(crm_session)

    # ---- reads ----------------------------------------------------------

    async def list_devices(self) -> Sequence[DeviceModel]:
        return await self._repo.list_devices()

    async def get_device_or_404(self, public_id: uuid.UUID) -> DeviceModel:
        device = await self._repo.get_device(public_id)
        if device is None:
            # 404 and not 403: the read was community-scoped, so another
            # community's device is indistinguishable from one that does not
            # exist - which is the point. A 403 would confirm it exists.
            raise ErrorException(errors.live.DEVICE_NOT_FOUND, status_code=404)
        return device

    # ---- create ---------------------------------------------------------

    async def create_device(
        self,
        *,
        id_community: int,
        name: str,
        ean: str,
        device_type: DeviceType,
        pure_injection: bool,
        ttl_hours: int,
    ) -> tuple[DeviceModel, IssuedToken]:
        """Create a PENDING device and its first enrolment token.

        Creating a device does NOT enrol it: no broker client exists until the
        device presents the token on the public leg. That split is what lets the
        token go to the MEMBER for a consumption device, through their own
        account, rather than to the administrator.
        """
        if await self._repo.live_device_exists_for_ean(ean):
            raise ErrorException(errors.live.DUPLICATE_EAN, status_code=409)

        # The ONLY thing that will ever validate this EAN. `device.ean` is a
        # plain column in another database and never a foreign key, so a typo
        # otherwise produces a device that ingests happily and is attributed to
        # nobody - found months later with real data stored against it.
        meter = await self._crm_read.find_active_meter(
            ean=ean, id_community=id_community, now=datetime.datetime.now(datetime.UTC)
        )
        if meter is None:
            raise ErrorException(errors.live.EAN_NOT_FOUND, status_code=422)

        device = self._repo.add_device(
            public_id=uuid.uuid4(),
            device_type=int(device_type),
            ean=ean,
            name=name,
            pure_injection=pure_injection,
            # Snapshot, named with its unit. kVA is the AC injection ceiling,
            # not the DC panel peak - clip against it, never trust it.
            capacity_kva=meter.capacity_kva,
            # The same snapshot argument, for the forecast seam (build step 9):
            # a method declares which production chains it supports, and a job
            # running per device must not pay a CRM round trip each to find out.
            # NULL when the CRM has not classified the meter, and the registry
            # treats NULL as matching no method rather than assuming solar.
            production_chain=meter.production_chain,
        )
        # Flush to get the SERIAL id the token row references.
        await self._local.flush()
        issued = await self._issue_token(device, ttl_hours=ttl_hours)
        await self._local.commit()
        await self._local.refresh(device)

        await self._audit_and_commit(
            AuditActions.DEVICE_CREATED,
            entity_id=str(device.public_id),
            payload={"ean": ean, "type": int(device_type), "pure_injection": pure_injection},
        )
        return device, issued

    async def reissue_token(self, public_id: uuid.UUID, *, ttl_hours: int) -> IssuedToken:
        """A fresh token, invalidating any unconsumed one.

        Regenerating is a NORMAL operation, not an incident: the password is
        shown once and is never recoverable, so a lost secret is re-enrolled
        rather than recovered.
        """
        device = await self.get_device_or_404(public_id)
        if device.status == int(DeviceStatus.REVOKED):
            raise ErrorException(errors.live.DEVICE_ALREADY_REVOKED, status_code=409)
        issued = await self._issue_token(device, ttl_hours=ttl_hours)
        await self._local.commit()
        await self._audit_and_commit(
            AuditActions.DEVICE_TOKEN_ISSUED,
            entity_id=str(device.public_id),
            payload={},
        )
        return issued

    async def _issue_token(self, device: DeviceModel, *, ttl_hours: int) -> IssuedToken:
        await self._repo.consume_open_tokens(device.id)
        plaintext = tokens.generate()
        normalised = tokens.normalise(plaintext)
        # Generated by us, so this cannot be None; asserted rather than assumed
        # because a silently-skipped hash would store a token nobody can redeem.
        if normalised is None:  # pragma: no cover - unreachable by construction
            raise ErrorException(errors.live.ISSUE_TOKEN, status_code=500)
        expires_at = datetime.datetime.now(datetime.UTC) + datetime.timedelta(hours=ttl_hours)
        self._repo.add_token(
            id_device=device.id,
            token_hash=tokens.hash_token(normalised),
            expires_at=expires_at,
        )
        return IssuedToken(plaintext, expires_at)

    # ---- revoke ---------------------------------------------------------

    async def revoke_device(self, public_id: uuid.UUID) -> DeviceModel:
        """Disable -> clear the retained status -> delete. In that order.

        Revocation is immediate and server-side. There is no device-side action
        and no notification: from the connector's point of view its credentials
        simply stop working.

        The BROKER side runs first and the commit second, mirroring enrolment's
        ordering rule for the same reason - of the two half-failures, a broker
        client deleted with the row still ACTIVE is recoverable by re-running
        revoke, whereas a row marked REVOKED with the client still alive leaves a
        device publishing that the UI says is gone.
        """
        device = await self.get_device_or_404(public_id)
        if device.status == int(DeviceStatus.REVOKED):
            raise ErrorException(errors.live.DEVICE_ALREADY_REVOKED, status_code=409)

        username = str(device.public_id)
        try:
            # A CLIENT THAT IS NOT THERE IS THE GOAL, NOT A FAILURE. A device
            # created and never enrolled has no broker client at all, and
            # dynsec answers `Client not found.` - which used to become a 502
            # and leave the row PENDING for ever, with its EAN locked, because
            # `uq_device_community_ean_live` excludes only REVOKED rows. A
            # typo'd EAN was therefore unrecoverable without hand-editing the
            # database. Found by scripts/verify-live-ingest.sh; the suite could
            # not see it, because the fake broker used to allow disabling a
            # client it had never created.
            #
            # Tolerated per command rather than around the block, so that a
            # device whose client was deleted by hand still gets its retained
            # status cleared.
            try:
                await self._broker.disable_device(username)
            except BrokerClientNotFoundError:
                logger.info("revoke: no broker client for %s; already gone", username)

            # THE MIDDLE STEP, and the reason for the ordering. A retained
            # `status` that outlives its device replays on every worker
            # reconnect, so the offline alert fires on every deploy until the
            # team stops reading it. It is a publish, so it has no notion of a
            # missing client and always runs.
            await self._broker.clear_retained_status(
                status_topic(device.id_community, device.public_id)
            )

            try:
                await self._broker.delete_device(username)
            except BrokerClientNotFoundError:
                logger.info("revoke: nothing to delete for %s", username)
        except BrokerError as exc:
            raise ErrorException(errors.live.BROKER_COMMAND_FAILED, status_code=502) from exc

        device.status = int(DeviceStatus.REVOKED)
        device.revoked_at = datetime.datetime.now(datetime.UTC)
        await self._local.commit()
        await self._local.refresh(device)

        await self._audit_and_commit(
            AuditActions.DEVICE_REVOKED,
            entity_id=str(device.public_id),
            payload={"ean": device.ean},
        )
        return device

    # ---- audit ----------------------------------------------------------

    async def _audit_and_commit(self, action: str, *, entity_id: str | None, payload: dict) -> None:
        """Stage an audit row and commit the CRM session, best-effort.

        The swallow is right here: the local write is already durable, and a CRM
        hiccup must not turn a successful revoke into a 500 the operator retries
        against a device that is already gone.

        `id_community` is NOT passed - on this surface the ContextVar is set by
        GatewayScopeMiddleware and is the correct source. The PUBLIC leg passes
        it explicitly, because there it is not.
        """
        await self._audit.log(
            AuditLogInput(
                action=action,
                entity_type="live_device",
                entity_id=entity_id,
                payload=payload,
                source=AUDIT_SOURCE,
            )
        )
        try:
            await self._crm.commit()
        except Exception:
            # LOGGED, never silent. The local write is already durable and a
            # CRM hiccup must not turn a successful operation into a 500 the
            # operator retries - but an audit trail that stops existing with
            # no trace at all is its own incident.
            logger.exception("audit commit failed for live_device %s", entity_id)

    # ---- used by the public leg -----------------------------------------

    @staticmethod
    def device_credentials(device_public_id: uuid.UUID, password: str) -> DeviceCredentials:
        """The credentials a device is created with.

        `client_id` equals the username, and pinning it at the broker is what
        enforces protocol 4.3 there rather than by asking connector authors
        nicely.
        """
        username = str(device_public_id)
        return DeviceCredentials(username=username, password=password, client_id=username)

    @staticmethod
    def device_role() -> str:
        return ROLE_DEVICE

    @staticmethod
    def feature() -> str:
        return FeatureName.LIVE_DATA.value
