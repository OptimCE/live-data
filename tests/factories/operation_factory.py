"""Factories for the CRM rows behind sharing operations and "my operations".

Raw `text()` INSERTs for the reason `meter_factory.py` gives: there is no ORM
model for these CRM tables in this service, deliberately. Build, FLUSH, never
commit - the per-test session is rolled back on teardown.
"""

from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

# SharingOperationType, mirroring crm-backend: 1 LOCAL, 2 CER, 3 CEC.
OPERATION_TYPE_LOCAL = 1

_INSERT_OPERATION = text(
    """
    INSERT INTO sharing_operation (name, type, id_community)
    VALUES (:name, :type, :id_community)
    RETURNING id
    """
)

_INSERT_MEMBER = text(
    """
    INSERT INTO member (name, member_type, status, id_community)
    VALUES (:name, 1, 1, :id_community)
    RETURNING id
    """
)

_INSERT_APP_USER = text(
    """
    INSERT INTO app_user (auth_user_id, email)
    VALUES (:auth_user_id, :email)
    ON CONFLICT (auth_user_id) DO UPDATE SET email = EXCLUDED.email
    RETURNING id
    """
)

_INSERT_LINK = text(
    "INSERT INTO user_member_link (id_user, id_member) VALUES (:id_user, :id_member)"
)


async def create_operation(
    session: AsyncSession, *, id_community: int, name: str, type_: int = OPERATION_TYPE_LOCAL
) -> int:
    """Insert a `sharing_operation` and return its id (an identity, so never 0)."""
    row: Any = await session.execute(
        _INSERT_OPERATION, {"name": name, "type": type_, "id_community": id_community}
    )
    await session.flush()
    return int(row.scalar_one())


async def create_member(session: AsyncSession, *, id_community: int, name: str = "Member") -> int:
    row: Any = await session.execute(_INSERT_MEMBER, {"name": name, "id_community": id_community})
    await session.flush()
    return int(row.scalar_one())


async def link_user_to_member(session: AsyncSession, *, auth_user_id: str, id_member: int) -> int:
    """Link a Keycloak subject to a CRM member, creating the `app_user` if needed.

    An `app_user` is GLOBAL - one person across every community - which is why
    "my operations" must scope through `member.id_community` and never through
    the link.
    """
    user: Any = await session.execute(
        _INSERT_APP_USER, {"auth_user_id": auth_user_id, "email": f"{auth_user_id}@example.test"}
    )
    id_user = int(user.scalar_one())
    await session.execute(_INSERT_LINK, {"id_user": id_user, "id_member": id_member})
    await session.flush()
    return id_user
