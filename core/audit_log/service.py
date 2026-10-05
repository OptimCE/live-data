import logging

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.audit_log.dtos import AuditLogInput
from core.context_vars import current_internal_community_id, current_user_id
from core.database.models import AuditLogModel
from shared.models.crm_models import AppUserModel

logger = logging.getLogger(__name__)

# The table is SHARED across every annexe, so this string is the row's identity.
# It arrived as "administrative-document" and was harmless only because all three
# call sites pass `source=` explicitly - `core/audit_log/deps.py` exposes the
# service as an injectable dependency, so a fourth caller that omits it would
# have filed live-data's rows under a sibling's name, silently and for ever.
AUDIT_LOG_DEFAULT_SOURCE = "live-data"


class AuditLogService:
    """Append entries to the shared CRM ``audit_log`` table.

    The save path mirrors ``crm-backend``'s ``AuditLogService.log``: it reads
    the request context, denormalises the writer's identity, and stages an
    insert on the caller's CRM session so the row commits or rolls back with
    the surrounding business transaction. Failures are swallowed and logged —
    the audit must never abort the caller's write.
    """

    def __init__(self, crm_session: AsyncSession):
        self.crm_session = crm_session

    async def log(
        self,
        entry: AuditLogInput,
        id_community: int | None = None,
    ) -> None:
        """Append an entry to the CRM ``audit_log`` table.

        ``id_community`` is an explicit override for callers that run outside
        a request context (e.g. worker handlers); when ``None`` the value is
        read from ``current_internal_community_id`` as set by
        ``GatewayScopeMiddleware``.
        """
        try:
            resolved_community = (
                id_community if id_community is not None else current_internal_community_id.get()
            )
            auth_user_id = current_user_id.get()

            user_id: int | None = None
            user_email: str | None = None
            if auth_user_id:
                stmt = select(AppUserModel.id, AppUserModel.email).where(
                    AppUserModel.auth_user_id == auth_user_id
                )
                result = await self.crm_session.execute(stmt)
                row = result.first()
                if row is not None:
                    user_id, user_email = row

            audit_row = AuditLogModel(
                id_community=resolved_community,
                action=entry.action,
                source=entry.source or AUDIT_LOG_DEFAULT_SOURCE,
                entity_type=entry.entity_type,
                entity_id=entry.entity_id,
                user_id=user_id,
                user_email=user_email,
                payload=entry.payload or {},
            )
            # Stage the insert inside a SAVEPOINT so a failure (FK violation,
            # constraint, transient error) rolls back only this nested write
            # and leaves the caller's outer transaction clean and usable. The
            # explicit flush forces the INSERT — and its constraint checks — to
            # surface within the savepoint scope, where the rollback is absorbed.
            async with self.crm_session.begin_nested():
                self.crm_session.add(audit_row)
                await self.crm_session.flush()
        except Exception:
            # Audit must never break the caller's business write. Log loudly
            # so dropped audits remain visible in the application log.
            logger.exception(
                "audit.log failed",
                extra={"operation": "audit_log:log", "entry": entry.model_dump()},
            )
