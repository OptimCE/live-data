# shared/middleware/gateway_scope.py
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

from core.context_vars import (
    current_community_id,
    current_source_ip,
    current_user_id,
    current_user_role,
)
from core.security.gateway_auth import parse_user_orgs


class GatewayScopeMiddleware(BaseHTTPMiddleware):
    """
    Sets ContextVars from the KrakenD-forwarded headers so that
    services, repositories, and audit logging can read them
    without passing UserContext explicitly.

    Role resolution mirrors crm-backend's ``shared/middlewares/context.ts``: the
    gateway injects ``x-user-orgs`` as a blob of *all* the caller's orgs and their
    roles (``[orgId:… orgPath:/x roles:[ADMIN]],map[orgId:… …]``). The effective
    role is the one attached to the org whose ``orgId`` equals the active
    ``x-community-id`` — not the raw blob. ``require_min_role`` reads the resolved
    single role from ``current_user_role``.
    """

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        # Build user context from headers (won't raise for unauthenticated routes).
        # Starlette headers are case-insensitive and return None for missing keys.
        user_id = request.headers.get("x-user-id")
        community_id = request.headers.get("x-community-id")
        user_orgs = request.headers.get("x-user-orgs")
        source_ip = request.headers.get("x-source-ip")

        role: str | None = None
        if community_id and user_orgs:
            for token in parse_user_orgs(user_orgs):
                if token.org_id == community_id:
                    role = token.role.value  # "ADMIN" | "MANAGER" | "MEMBER"
                    break

        t1 = current_user_id.set(user_id)
        t2 = current_community_id.set(community_id)
        t3 = current_user_role.set(role)
        t4 = current_source_ip.set(source_ip)

        try:
            return await call_next(request)
        finally:
            current_user_id.reset(t1)
            current_community_id.reset(t2)
            current_user_role.reset(t3)
            current_source_ip.reset(t4)
