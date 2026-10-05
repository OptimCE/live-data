"""CRM reads behind the sharing-operation views (D-14).

Its own module rather than a fourth read on `crm_read` (whose docstring promises
the reads it has) or on `crm_core` (time-sliced ownership for the projection):
these answer the READ surface's questions, at request time, about NOW.

Raw `text()`, SELECT-only, and read-only-ness enforced by the `live_data_svc`
role - the same house rules as the two sibling ports.

----------------------------------------------------------------------------
EVERY JOIN IS SCOPED TO THE COMMUNITY, NOT JUST THE FIRST ONE.

`held_operations` walks app_user -> user_member_link -> member -> meter_data ->
meter -> sharing_operation, and `:c` is asserted on `member`, on `meter` AND on
`sharing_operation`. An `app_user` is GLOBAL - one person across every community -
and the link table carries no community at all, so a predicate on one table
would let a member of A holding a meter in B's operation see B's operation in A.

"Today" is a BRUSSELS date bound from Python (`domain.ownership.local_date_of`),
never `CURRENT_DATE`: the session is UTC, and the CRM's dates are Belgian.
`status = 1` (ACTIVE) only, matching the projection: a meter still waiting for
the DSO shares nothing yet, so it holds no operation for this purpose either.
----------------------------------------------------------------------------
"""

import datetime
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

_METER_DATA_ACTIVE = 1


@dataclass(frozen=True, slots=True)
class OperationRef:
    id: int
    name: str


class CrmOperationsReadPort(Protocol):
    async def operations(self, *, id_community: int, ids: Sequence[int]) -> list[OperationRef]:
        """The named operations among `ids` that belong to this community."""
        ...

    async def active_meter_counts(
        self, *, id_community: int, today: datetime.date
    ) -> dict[int, int]:
        """ACTIVE meters per operation today - the denominator of the coverage
        line next to the estimate ("N monitored meters out of M")."""
        ...

    async def held_operations(
        self, *, id_community: int, auth_user_id: str, today: datetime.date
    ) -> list[OperationRef]:
        """The operations in which the caller's member(s) hold an ACTIVE meter
        today, in this community. Empty for a caller with no member link."""
        ...


class SqlAlchemyCrmOperationsRead:
    """The real adapter. SELECT-only."""

    _OPERATIONS_SQL = text(
        """
        SELECT so.id, so.name
          FROM sharing_operation so
         WHERE so.id_community = :c
           AND so.id = ANY(:ids)
         ORDER BY so.name, so.id
        """
    )

    _ACTIVE_METERS_SQL = text(
        """
        SELECT md.id_sharing_operation AS id, COUNT(DISTINCT md.ean) AS n_meters
          FROM meter_data md
          JOIN meter m ON m.ean = md.ean AND m.id_community = :c
          JOIN sharing_operation so ON so.id = md.id_sharing_operation AND so.id_community = :c
         WHERE md.status = :active
           AND :today BETWEEN md.start_date AND COALESCE(md.end_date, 'infinity'::date)
         GROUP BY md.id_sharing_operation
        """
    )

    _HELD_SQL = text(
        """
        SELECT DISTINCT so.id, so.name
          FROM app_user au
          JOIN user_member_link uml ON uml.id_user = au.id
          JOIN member mb ON mb.id = uml.id_member AND mb.id_community = :c
          JOIN meter_data md ON md.id_member = mb.id
          JOIN meter m ON m.ean = md.ean AND m.id_community = :c
          JOIN sharing_operation so ON so.id = md.id_sharing_operation AND so.id_community = :c
         WHERE au.auth_user_id = :auth_user_id
           AND md.status = :active
           AND :today BETWEEN md.start_date AND COALESCE(md.end_date, 'infinity'::date)
         ORDER BY so.name, so.id
        """
    )

    def __init__(self, crm_session: AsyncSession) -> None:
        self._session = crm_session

    async def operations(self, *, id_community: int, ids: Sequence[int]) -> list[OperationRef]:
        if not ids:
            return []
        rows = await self._session.execute(
            self._OPERATIONS_SQL, {"c": id_community, "ids": list(ids)}
        )
        return [OperationRef(id=row.id, name=row.name) for row in rows]

    async def active_meter_counts(
        self, *, id_community: int, today: datetime.date
    ) -> dict[int, int]:
        rows = await self._session.execute(
            self._ACTIVE_METERS_SQL,
            {"c": id_community, "today": today, "active": _METER_DATA_ACTIVE},
        )
        return {row.id: row.n_meters for row in rows}

    async def held_operations(
        self, *, id_community: int, auth_user_id: str, today: datetime.date
    ) -> list[OperationRef]:
        rows = await self._session.execute(
            self._HELD_SQL,
            {
                "c": id_community,
                "auth_user_id": auth_user_id,
                "today": today,
                "active": _METER_DATA_ACTIVE,
            },
        )
        return [OperationRef(id=row.id, name=row.name) for row in rows]


class FakeCrmOperationsRead:
    """In-memory operations for tests. Scopes exactly like the real adapter:
    by community on every lookup, so a test cannot pass against the fake with a
    query the real one would refuse."""

    def __init__(
        self,
        *,
        operations: dict[int, tuple[int, str]] | None = None,
        meters: dict[int, int] | None = None,
        held: dict[tuple[int, str], list[int]] | None = None,
    ) -> None:
        # id -> (id_community, name); operation id -> active meters;
        # (id_community, auth_user_id) -> held operation ids.
        self._operations = dict(operations or {})
        self._meters = dict(meters or {})
        self._held = dict(held or {})

    async def operations(self, *, id_community: int, ids: Sequence[int]) -> list[OperationRef]:
        wanted = set(ids)
        found = [
            OperationRef(id=op_id, name=name)
            for op_id, (community, name) in self._operations.items()
            if op_id in wanted and community == id_community
        ]
        return sorted(found, key=lambda ref: (ref.name, ref.id))

    async def active_meter_counts(
        self, *, id_community: int, today: datetime.date
    ) -> dict[int, int]:
        return {
            op_id: count
            for op_id, count in self._meters.items()
            if self._operations.get(op_id, (None, ""))[0] == id_community
        }

    async def held_operations(
        self, *, id_community: int, auth_user_id: str, today: datetime.date
    ) -> list[OperationRef]:
        ids = self._held.get((id_community, auth_user_id), [])
        return await self.operations(id_community=id_community, ids=ids)
