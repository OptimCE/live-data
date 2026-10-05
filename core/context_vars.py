from contextvars import ContextVar

current_user_id: ContextVar[str | None] = ContextVar("current_user_id", default=None)
current_community_id: ContextVar[str | None] = ContextVar("current_community_id", default=None)
current_internal_community_id: ContextVar[int | None] = ContextVar(
    "current_internal_community_id", default=None
)
current_user_role: ContextVar[str | None] = ContextVar("current_user_role", default=None)
current_source_ip: ContextVar[str | None] = ContextVar("current_source_ip", default=None)
current_locale: ContextVar[str] = ContextVar("current_locale", default="fr_FR")
current_request_id: ContextVar[str | None] = ContextVar("current_request_id", default=None)
