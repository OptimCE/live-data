"""Create-ahead and retention. Build step 8.

`tests/test_partitions.py` pins the naming and bounds helpers against the SQL
that creates them. This file pins the two JOBS: the one that creates partitions
before they are needed, and the one that drops them when they are not.

The failure both exist to prevent is the same and it is platform-wide: nothing
errors at write time when the create-ahead job stops, because rows land in the
DEFAULT partition and every query keeps working. It surfaces months later, at
00:00 UTC on the first of a month, when creating the partition that would fix it
starts failing too.
"""

import datetime

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from domain.partitions import (
    PARTITIONED_TABLES,
    default_partition_name,
    month_bounds,
    month_start,
    months_to_provision,
    partition_name,
)
from tests.factories.device_factory import create_device
from tests.factories.meter_factory import create_owned_meter
from tests.factories.subscription_factory import create_community
from worker import partitions, retention

NOW = datetime.datetime(2026, 9, 16, 10, 30, tzinfo=datetime.UTC)
# Far outside the range schema.sql provisioned, so no partition exists for it.
# Relative to the REAL clock, because that is what schema.sql provisions from: a
# fixed date here (it was 2028-04) falls inside MONTHS_AHEAD once the calendar
# catches up, and every test below that needs it absent fails.
_THIS_MONTH = month_start(datetime.datetime.now(datetime.UTC))
UNPROVISIONED = _THIS_MONTH.replace(year=_THIS_MONTH.year + 2)


async def _partition_names(session: AsyncSession, parent: str) -> set[str]:
    rows = await session.execute(
        text(
            "SELECT c.relname FROM pg_inherits i "
            "JOIN pg_class c ON c.oid = i.inhrelid "
            "JOIN pg_class p ON p.oid = i.inhparent WHERE p.relname = :p"
        ),
        {"p": parent},
    )
    return set(rows.scalars().all())


@pytest.fixture
async def device(db_session: AsyncSession) -> tuple[int, int]:
    community = await create_community(db_session)
    ean = await create_owned_meter(db_session, id_community=community.id, id_member=1)
    device_id = await create_device(db_session, id_community=community.id, ean=ean)
    return community.id, device_id


class TestPartitionKey:
    @pytest.mark.parametrize("table", PARTITIONED_TABLES)
    async def test_the_key_is_read_from_the_catalog(self, db_session: AsyncSession, table: str):
        """`measurement` ranges on `ts` and the rollups on `bucket`. Reading it
        from `pg_partitioned_table` rather than from a mapping in Python means
        there is no third list to keep in step with schema.sql."""
        key = await partitions.partition_key(db_session, table)
        assert key in {"ts", "bucket"}

    async def test_a_table_that_is_not_partitioned_raises(self, db_session: AsyncSession):
        """Loud rather than silent: a registry row naming an ordinary table would
        otherwise make the job skip it for ever."""
        with pytest.raises(RuntimeError, match="not partitioned"):
            await partitions.partition_key(db_session, "device")


class TestCreateAhead:
    async def test_every_registry_table_gets_every_month(self, db_session: AsyncSession):
        # The REAL clock, not NOW: schema.sql provisioned from `now()`, and the
        # job agrees with it only at the same moment. With NOW this held only
        # while the calendar was near NOW - from October 2026 the job had NOW's
        # oldest month to create.
        now = datetime.datetime.now(datetime.UTC)
        created, _ = await partitions.ensure_partitions(db_session, now=now)
        assert created == []  # schema.sql already made them all

        for table in PARTITIONED_TABLES:
            names = await _partition_names(db_session, table)
            expected = {partition_name(table, m) for m in months_to_provision(now)}
            assert expected <= names

    async def test_a_missing_month_is_created_for_every_table(self, db_session: AsyncSession):
        created, _ = await partitions.ensure_partitions(
            db_session, now=UNPROVISIONED, months_ahead=0, months_back=0
        )
        assert sorted(created) == sorted(
            partition_name(table, UNPROVISIONED) for table in PARTITIONED_TABLES
        )

    async def test_running_twice_creates_nothing_the_second_time(self, db_session: AsyncSession):
        await partitions.ensure_partitions(
            db_session, now=UNPROVISIONED, months_ahead=0, months_back=0
        )
        again, _ = await partitions.ensure_partitions(
            db_session, now=UNPROVISIONED, months_ahead=0, months_back=0
        )
        assert again == []

    async def test_two_missing_months_in_one_run(self, db_session: AsyncSession):
        """The temp-table collision, asserted.

        `_drain` is one name and this loops over (table, month) inside a single
        transaction. Creating it unconditionally raises "relation already exists"
        on the second drain and takes out the whole nightly run.
        """
        created, _ = await partitions.ensure_partitions(
            db_session, now=UNPROVISIONED, months_ahead=1, months_back=0
        )
        assert len(created) == 2 * len(PARTITIONED_TABLES)


class TestTheDrain:
    async def test_the_raw_function_fails_over_a_populated_default(
        self, db_session: AsyncSession, device
    ):
        """THE NEGATIVE CONTROL, and the platform-wide failure reproduced in a
        second.

        `live_ensure_monthly_partitions` cannot create a partition over a range
        the DEFAULT already holds rows in - Postgres scans the default to prove
        the range is empty and RAISES when it is not. Three months after launch
        with a stopped create-ahead job, that is every table, at 00:00 UTC on the
        first of a month, for every community at once.
        """
        id_community, device_id = device
        await db_session.execute(
            text(
                "INSERT INTO measurement (id_device, ts, id_community, interval_s, "
                "import_wh, export_wh) VALUES (:d, :ts, :c, 900, 1, 0)"
            ),
            {"d": device_id, "ts": UNPROVISIONED + datetime.timedelta(days=3), "c": id_community},
        )
        in_default = await db_session.scalar(text("SELECT count(*) FROM measurement_default"))
        assert in_default == 1, "the fixture must land the row in the default"

        lower, upper = month_bounds(UNPROVISIONED)
        with pytest.raises(Exception, match="(?i)default partition|would be violated"):
            await db_session.execute(
                text(
                    f"CREATE TABLE {partition_name('measurement', UNPROVISIONED)} "
                    f"PARTITION OF measurement FOR VALUES FROM ('{lower}') TO ('{upper}')"
                )
            )

    async def test_the_job_drains_and_succeeds_where_the_raw_call_fails(
        self, db_session: AsyncSession, device
    ):
        """The same situation, through the job. The row ends up in the new
        partition and the default is empty again - which is what breaks the
        cycle the test above describes."""
        id_community, device_id = device
        stamp = UNPROVISIONED + datetime.timedelta(days=3)
        await db_session.execute(
            text(
                "INSERT INTO measurement (id_device, ts, id_community, interval_s, "
                "import_wh, export_wh) VALUES (:d, :ts, :c, 900, 42, 0)"
            ),
            {"d": device_id, "ts": stamp, "c": id_community},
        )

        drained = await partitions.create_partition_draining_default(
            db_session, table="measurement", month=UNPROVISIONED
        )
        # THE ROW COUNT, not a flag. It was computed under the lock and thrown
        # away, while `core/metrics.partition_default_rows_drained` - documented
        # in the runbook as "should be zero for ever" - was fed by nothing. A
        # counter wired to nothing reads as a flat line, which is exactly how a
        # repaired create-ahead cycle would have looked like an event that never
        # happened.
        assert drained == 1

        child = partition_name("measurement", UNPROVISIONED)
        moved = await db_session.scalar(text(f"SELECT count(*) FROM {child}"))  # noqa: S608
        assert moved == 1
        left = await db_session.scalar(text("SELECT count(*) FROM measurement_default"))
        assert left == 0

        # And the row itself survived intact - a drain that loses data is worse
        # than a drain that never ran.
        value = await db_session.scalar(
            text("SELECT import_wh FROM measurement WHERE id_device = :d AND ts = :ts"),
            {"d": device_id, "ts": stamp},
        )
        assert value == pytest.approx(42.0)

    async def test_an_existing_partition_is_distinguishable_from_an_empty_drain(self, db_session):
        """-1 for "already there", 0 for "created, nothing to move".

        A bool collapsed those, and 0 is the healthy steady state: partitions are
        created six months ahead, so almost every call creates one with an empty
        default. If "already existed" also read as 0, the counter could never
        tell the two apart and neither could the caller.
        """
        first = await partitions.create_partition_draining_default(
            db_session, table="measurement", month=UNPROVISIONED
        )
        assert first == 0
        again = await partitions.create_partition_draining_default(
            db_session, table="measurement", month=UNPROVISIONED
        )
        assert again == -1

    async def test_rows_outside_the_month_stay_in_the_default(
        self, db_session: AsyncSession, device
    ):
        """The drain moves the month it is creating, and nothing else. Draining
        the whole default would hit an insert with nowhere to route to."""
        id_community, device_id = device
        # Unprovisioned too, or the row is routed to a real partition and never
        # reaches the default this test watches.
        far = UNPROVISIONED.replace(year=UNPROVISIONED.year + 1) + datetime.timedelta(days=3)
        await db_session.execute(
            text(
                "INSERT INTO measurement (id_device, ts, id_community, interval_s, "
                "import_wh, export_wh) VALUES (:d, :ts, :c, 900, 1, 0)"
            ),
            {"d": device_id, "ts": far, "c": id_community},
        )
        await partitions.create_partition_draining_default(
            db_session, table="measurement", month=UNPROVISIONED
        )
        left = await db_session.scalar(text("SELECT count(*) FROM measurement_default"))
        assert left == 1


class TestPendingDetach:
    async def test_nothing_pending_is_a_no_op(self, db_session: AsyncSession):
        assert await partitions.finalise_pending_detaches(db_session) == []


class TestDefaultCounts:
    async def test_every_registry_default_is_reported(self, db_session: AsyncSession):
        counts = await partitions.default_partition_counts(db_session)
        assert set(counts) == {default_partition_name(t) for t in PARTITIONED_TABLES}
        assert set(counts.values()) == {0}


class TestRetentionArithmetic:
    def test_the_cutoff_is_calendar_months(self):
        assert retention.cutoff(NOW, 13) == datetime.datetime(2025, 8, 1, tzinfo=datetime.UTC)

    def test_a_zero_retention_is_refused(self):
        """A retention of 0 would drop the current month, live data included."""
        with pytest.raises(ValueError, match="at least 1"):
            retention.cutoff(NOW, 0)

    def test_the_default_partition_is_never_droppable(self):
        """Its bound is the literal `DEFAULT`, which does not parse as a range.
        Dropping it would turn every out-of-range insert from "stored somewhere
        visible" into an error on the ingest hot path."""
        rows = [("measurement_default", "DEFAULT")]
        far_future = datetime.datetime(2099, 1, 1, tzinfo=datetime.UTC)
        assert retention.droppable(rows, before=far_future) == []

    def test_a_partition_is_selected_by_its_upper_bound(self):
        """Not its lower one. Testing the lower bound drops the month the cutoff
        falls inside, taking live data with it."""
        straddling = [
            (
                "measurement_2025_08",
                "FOR VALUES FROM ('2025-08-01 00:00:00+00') TO ('2025-09-01 00:00:00+00')",
            )
        ]
        cut = datetime.datetime(2025, 8, 15, tzinfo=datetime.UTC)
        assert retention.droppable(straddling, before=cut) == []

    def test_a_fully_expired_partition_is_selected(self):
        rows = [
            (
                "measurement_2025_07",
                "FOR VALUES FROM ('2025-07-01 00:00:00+00') TO ('2025-08-01 00:00:00+00')",
            )
        ]
        cut = datetime.datetime(2025, 8, 1, tzinfo=datetime.UTC)
        assert [name for name, _ in retention.droppable(rows, before=cut)] == [
            "measurement_2025_07"
        ]

    def test_an_oddly_named_partition_is_still_selected(self):
        """SELECTED BY BOUND, NEVER BY NAME. A partition attached by hand with a
        different name is invisible to a name-driven job - it survives every
        retention run and quietly keeps personal data for ever."""
        rows = [
            (
                "measurement_restored_from_backup",
                "FOR VALUES FROM ('2020-01-01 00:00:00+00') TO ('2020-02-01 00:00:00+00')",
            )
        ]
        cut = datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC)
        assert [name for name, _ in retention.droppable(rows, before=cut)] == [
            "measurement_restored_from_backup"
        ]

    def test_rollup_community_day_has_no_retention(self):
        """Deliberate, and the only table without one: no device and no member
        survives the aggregation, and it is the only series that can answer "how
        did we do last year"."""
        assert "rollup_community_day" not in retention.retention_months()
        assert set(retention.retention_months()) == {
            "measurement",
            "rollup_device_hour",
            "rollup_device_day",
            "rollup_community_hour",
            # NOT exempt like the community day (D-14): an operation of two
            # households is two households.
            "rollup_operation_hour",
            "rollup_operation_day",
        }
