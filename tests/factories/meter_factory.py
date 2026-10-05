"""Factories for the CRM meter rows the ownership projection reads.

Raw `text()` INSERTs rather than `factory.Factory`, unlike
`subscription_factory.py`, and for the same reason `ports/crm_read.py` and
`ports/crm_core.py` use raw SQL: **there is no ORM model for `meter` or
`meter_data` in this service, deliberately.** `shared/models/crm_models.py` maps
only `app_user`, and mapping these two would invite someone to write ownership
queries through the mapper and discover the CRM schema drifted only when a
mapper error surfaced somewhere unrelated.

House style otherwise unchanged: build, attach, FLUSH. Never commit - the
per-test session runs inside a connection-level transaction rolled back on
teardown.
"""

import datetime
import itertools
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

# MeterDataStatus. 1 ACTIVE, 2 INACTIVE, 3 WAITING_GRD, 4 WAITING_MANAGER.
METER_DATA_ACTIVE = 1
METER_DATA_INACTIVE = 2
METER_DATA_WAITING_GRD = 3

_ean_counter = itertools.count(541448000000000001)

_INSERT_METER = text(
    """
    INSERT INTO meter (ean, meter_number, id_community)
    VALUES (:ean, :meter_number, :id_community)
    ON CONFLICT (ean) DO NOTHING
    """
)

_INSERT_METER_DATA = text(
    """
    INSERT INTO meter_data
        (ean, id_member, status, start_date, end_date,
         injection_status, production_chain, total_generating_capacity,
         id_sharing_operation)
    VALUES
        (:ean, :id_member, :status, :start_date, :end_date,
         :injection_status, :production_chain, :total_generating_capacity,
         :id_sharing_operation)
    RETURNING id
    """
)


def next_ean() -> str:
    """A fresh 18-digit EAN.

    Belgian EANs start 541448 and are 18 digits.

    NOTE `DeviceCreate.ean` does NOT validate that shape. It is
    `Field(min_length=1, max_length=64)` and is deliberately lenient, so an
    existing meter whose EAN predates the 18-digit rule stays enrollable. What
    actually rejects a bad EAN is the CRM existence lookup in
    `api/live/service.py` (`EAN_NOT_FOUND`, 422), which is stronger than any
    format check. The shape still matters here because a test meter that cannot
    be enrolled is a test meter that proves nothing.
    """
    return str(next(_ean_counter))


async def create_meter(
    session: AsyncSession,
    *,
    id_community: int,
    ean: str | None = None,
    meter_number: str | None = None,
) -> str:
    """Insert a `meter` row and return its EAN.

    Every ownership query here is scoped through `meter.id_community` (the real
    `meter_data` has an `id_community` too; this mirror omits it), so a
    `meter_data` row with no matching `meter` is invisible to the projection.
    """
    resolved = ean or next_ean()
    await session.execute(
        _INSERT_METER,
        {
            "ean": resolved,
            "meter_number": meter_number or f"MTR-{resolved[-6:]}",
            "id_community": id_community,
        },
    )
    await session.flush()
    return resolved


async def create_meter_data(
    session: AsyncSession,
    *,
    ean: str,
    start_date: datetime.date,
    end_date: datetime.date | None = None,
    id_member: int | None = 1,
    status: int = METER_DATA_ACTIVE,
    injection_status: int | None = 1,
    production_chain: int | None = 1,
    total_generating_capacity: float | None = 5.0,
    id_sharing_operation: int | None = None,
) -> int:
    """Insert one ownership window and return its id.

    `end_date=None` is open-ended, and the bounds are CLOSED at both ends - the
    convention `meter_data` already uses everywhere. So a transfer on 1 March is
    written as `end_date = 2026-02-28` on the outgoing window and
    `start_date = 2026-03-01` on the incoming one, and those two do NOT overlap.
    """
    row: Any = await session.execute(
        _INSERT_METER_DATA,
        {
            "ean": ean,
            "id_member": id_member,
            "status": status,
            "start_date": start_date,
            "end_date": end_date,
            "injection_status": injection_status,
            "production_chain": production_chain,
            "total_generating_capacity": total_generating_capacity,
            "id_sharing_operation": id_sharing_operation,
        },
    )
    await session.flush()
    return int(row.scalar_one())


async def create_owned_meter(
    session: AsyncSession,
    *,
    id_community: int,
    id_member: int | None = 1,
    start_date: datetime.date | None = None,
    end_date: datetime.date | None = None,
    ean: str | None = None,
    **meter_data_kwargs,
) -> str:
    """The common case: one meter with one open-ended ACTIVE window.

    `start_date` defaults well into the past rather than to today, because a
    window starting today does not cover yesterday's measurements and the
    resulting "n_members is 0" reads exactly like a broken projection.
    """
    resolved = await create_meter(session, id_community=id_community, ean=ean)
    await create_meter_data(
        session,
        ean=resolved,
        id_member=id_member,
        start_date=start_date or datetime.date(2020, 1, 1),
        end_date=end_date,
        **meter_data_kwargs,
    )
    return resolved
