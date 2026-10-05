from fastapi import Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.context_vars import current_community_id, current_internal_community_id
from core.database.database import get_crm_session
from core.database.models import Community


async def resolve_internal_community(
    crm_session: AsyncSession = Depends(get_crm_session),
) -> int | None:
    """
    Resolves the auth (Keycloak) community id from the request context to the
    internal CRM `community.id` integer and caches it in a ContextVar so
    repositories can read it without further DB calls.

    Mounted as a router-level dependency next to `require_community`.
    Returning None (rather than raising) lets `with_community_scope` apply its
    `WHERE false` fallback. Routes that need to *require* a community should
    keep using `require_community()` from `core.security.dependencies`.
    """
    auth_id = current_community_id.get()
    if auth_id is None:
        return None

    cached = current_internal_community_id.get()
    if cached is not None:
        return cached

    stmt = select(Community.id).where(Community.auth_community_id == auth_id)
    result = await crm_session.execute(stmt)
    internal_id = result.scalar_one_or_none()

    if internal_id is not None:
        current_internal_community_id.set(internal_id)
    return internal_id
