"""Factories for the owned schema: devices and their measurements.

Used by the ownership projection, the rollup tick and the read API. The device
tests under `tests/api/` deliberately do NOT use these - they go through the HTTP
surface, because creating a device is one of the things they are testing. These
exist for the jobs and the reads, where a device is a precondition rather than
the subject.

Raw `text()` for `measurement`, because it is PARTITIONED and its upsert targets
the parent with `ON CONFLICT (id_device, ts)`; the ORM has no way to express that
and would route around the very statement `worker/ingest.py` uses.

Build, attach, FLUSH. Never commit.
"""

import datetime
import itertools
import uuid

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from shared.const import DeviceStatus, DeviceType, ProductionChain
from worker.ingest import _UPSERT_SQL as _INGEST_UPSERT_SQL

_name_counter = itertools.count(1)

_INSERT_DEVICE = text(
    """
    INSERT INTO device (public_id, id_community, type, ean, name, status, pure_injection,
                        capacity_kva, production_chain, enrolled_at)
    VALUES (:public_id, :id_community, :type, :ean, :name, :status, :pure_injection,
            :capacity_kva, :production_chain, :enrolled_at)
    RETURNING id
    """
)

# THE REAL STATEMENT, imported rather than copied.
#
# It is private, and importing it anyway is the lesser evil. A local copy here
# would be a fake more forgiving than the real thing: `worker/ingest.py`'s upsert
# marks `rollup_dirty` in the same statement, and a factory that skipped that
# would make every "late data reaches the tick" test pass against a write path
# the service does not have. This repository has already paid for that shape once
# - 270 green tests over a device that could never be revoked.
#
# Legal on the partitioned parent only because the primary key contains the
# partition key.
_UPSERT_MEASUREMENT = _INGEST_UPSERT_SQL


async def create_device(
    session: AsyncSession,
    *,
    id_community: int,
    ean: str,
    device_type: DeviceType = DeviceType.PRODUCTION,
    status: DeviceStatus = DeviceStatus.ACTIVE,
    pure_injection: bool = True,
    capacity_kva: float | None = 5.0,
    production_chain: int | None = int(ProductionChain.PHOTOVOLTAIC),
    public_id: uuid.UUID | None = None,
    name: str | None = None,
) -> int:
    """Insert a device and return its INTEGER id.

    The integer, not the `public_id`: every table that references a device does so
    by `id`, and a factory that returned the UUID would have every caller
    re-querying for the integer it actually needs.
    """
    result = await session.execute(
        _INSERT_DEVICE,
        {
            "public_id": public_id or uuid.uuid4(),
            "id_community": id_community,
            "type": int(device_type),
            "ean": ean,
            "name": name or f"Device {next(_name_counter)}",
            "status": int(status),
            "pure_injection": pure_injection,
            "capacity_kva": capacity_kva,
            "production_chain": production_chain,
            "enrolled_at": datetime.datetime.now(datetime.UTC),
        },
    )
    await session.flush()
    return int(result.scalar_one())


async def create_measurement(
    session: AsyncSession,
    *,
    id_device: int,
    id_community: int,
    ts: datetime.datetime,
    import_wh: float = 0.0,
    export_wh: float = 0.0,
    production_wh: float | None = None,
    interval_s: int = 900,
) -> None:
    """One reading. `ts` is the END of its interval - see domain/buckets.py.

    So a reading covering 10:00-10:15 carries 10:15:00Z and belongs to the 10:00
    bucket, and one carrying 11:00:00Z belongs to the 10:00 bucket too. Passing a
    bucket start here is the single easiest way to write a rollup test that
    passes for the wrong reason.
    """
    await session.execute(
        _UPSERT_MEASUREMENT,
        {
            "id_device": id_device,
            "ts": ts,
            "id_community": id_community,
            "interval_s": interval_s,
            "import_wh": import_wh,
            "export_wh": export_wh,
            "production_wh": production_wh,
        },
    )
    await session.flush()


async def create_hour_of_measurements(
    session: AsyncSession,
    *,
    id_device: int,
    id_community: int,
    bucket: datetime.datetime,
    import_wh: float = 100.0,
    export_wh: float = 0.0,
    production_wh: float | None = 250.0,
) -> None:
    """Four quarter-hours filling `bucket`, each carrying the given values.

    The four `ts` values are bucket+15m, +30m, +45m and +60m - the last being the
    NEXT hour's start, which belongs to this bucket because `ts` is an interval
    end. Getting that fourth one wrong is how a rollup test silently asserts
    three quarters of an hour.
    """
    for minutes in (15, 30, 45, 60):
        await create_measurement(
            session,
            id_device=id_device,
            id_community=id_community,
            ts=bucket + datetime.timedelta(minutes=minutes),
            import_wh=import_wh,
            export_wh=export_wh,
            production_wh=production_wh,
        )
