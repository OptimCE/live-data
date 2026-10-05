from sqlalchemy import false
from sqlalchemy.sql import Select

from core.context_vars import current_internal_community_id


def with_community_scope[TStmt: Select](stmt: TStmt, model: type) -> TStmt:
    """
    Automatically applies the community filter based on the current request context.

    Python/FastAPI equivalent of `withCommunityScope` from the CRM NodeJS backend.
    Unlike the TypeORM version, this does NOT JOIN the community table: every
    CRM and Local model in this service denormalizes `id_community` as an int,
    so a direct WHERE on the resolved internal id is sufficient (and is the
    only option for Local DB models, which cannot JOIN the CRM community table).

    The internal community id is resolved once per request by the
    `resolve_internal_community` dependency and stored in a ContextVar.

    :param stmt: a SQLAlchemy 2.0 `select(...)` statement
    :param model: the ORM class providing the `id_community` column
    """
    internal_id = current_internal_community_id.get()

    if internal_id is None:
        # Security fallback: no community in context = no results.
        return stmt.where(false())

    return stmt.where(model.id_community == internal_id)  # type: ignore[attr-defined]
