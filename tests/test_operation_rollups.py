"""Per sharing operation rollups (migration 0003, D-14).

Each test names the silent failure it exists for. The operation rows are a
second cut of the same energy as the community hour, so the two families of
defect are: energy counted in the wrong row (or twice), and a shared figure
that claims more than was ever shared.
"""

import datetime

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from domain import buckets
from ports.crm_core import SqlAlchemyCrmCoreRead
from shared.const import NO_SHARING_OPERATION
from tests.factories.device_factory import (
    create_device,
    create_hour_of_measurements,
    create_measurement,
)
from tests.factories.meter_factory import (
    METER_DATA_WAITING_GRD,
    create_meter,
    create_meter_data,
    create_owned_meter,
)
from tests.factories.operation_factory import create_operation
from tests.factories.subscription_factory import create_community
from worker import rollups
from worker.ownership import refresh_community

T = datetime.datetime(2026, 6, 10, 10, 30, tzinfo=datetime.UTC)


def hour(value: str) -> datetime.datetime:
    return datetime.datetime.fromisoformat(value).replace(tzinfo=datetime.UTC)


async def _device_in(
    session: AsyncSession,
    *,
    id_community: int,
    operation: int | None,
    member: int,
    **meter_data_kwargs,
) -> int:
    ean = await create_owned_meter(
        session,
        id_community=id_community,
        id_member=member,
        id_sharing_operation=operation,
        **meter_data_kwargs,
    )
    return await create_device(session, id_community=id_community, ean=ean)


async def _project(session: AsyncSession, id_community: int, now: datetime.datetime = T) -> None:
    await refresh_community(
        session, SqlAlchemyCrmCoreRead(session), id_community=id_community, now=now
    )


async def _operation_hours(session: AsyncSession, id_community: int) -> dict:
    rows = await session.execute(
        text(
            "SELECT id_sharing_operation, bucket, import_wh, export_wh, production_wh, "
            "shared_wh, n_devices, n_devices_production, n_members, computed_at "
            "FROM rollup_operation_hour WHERE id_community = :c "
            "ORDER BY id_sharing_operation, bucket"
        ),
        {"c": id_community},
    )
    return {(row.id_sharing_operation, row.bucket): row for row in rows}


async def _community_hour(session: AsyncSession, id_community: int, bucket: datetime.datetime):
    return (
        await session.execute(
            text(
                "SELECT import_wh, export_wh, production_wh, n_members "
                "FROM rollup_community_hour WHERE id_community = :c AND bucket = :b"
            ),
            {"c": id_community, "b": bucket},
        )
    ).one()


@pytest.fixture
async def community(db_session: AsyncSession) -> int:
    return (await create_community(db_session)).id


class TestShared:
    async def test_shared_is_computed_per_quarter_never_per_hour(
        self, db_session: AsyncSession, community: int
    ):
        """A 09:15 surplus cannot cover a 09:30 offtake. Per quarter the operation
        shares 0 + 0 + 30; from the hour's sums it would claim 130 - energy that
        was never shared, on a chart that says it was."""
        op = await create_operation(db_session, id_community=community, name="Op")
        producer = await _device_in(db_session, id_community=community, operation=op, member=1)
        consumer = await _device_in(db_session, id_community=community, operation=op, member=2)
        await _project(db_session, community)
        quarters = [  # (ts, producer export, consumer import)
            (hour("2026-06-10 09:15"), 100.0, 0.0),
            (hour("2026-06-10 09:30"), 0.0, 100.0),
            (hour("2026-06-10 09:45"), 50.0, 30.0),
        ]
        for ts, export_wh, import_wh in quarters:
            await create_measurement(
                db_session, id_device=producer, id_community=community, ts=ts, export_wh=export_wh
            )
            await create_measurement(
                db_session, id_device=consumer, id_community=community, ts=ts, import_wh=import_wh
            )

        await rollups.tick_community(db_session, id_community=community, now=T)

        row = (await _operation_hours(db_session, community))[(op, hour("2026-06-10 09:00"))]
        assert (row.import_wh, row.export_wh) == (130.0, 150.0)
        assert row.shared_wh == 30.0
        # NEGATIVE CONTROL: what the hourly form would have said.
        assert row.shared_wh != min(row.import_wh, row.export_wh)

    async def test_shared_never_exceeds_the_lesser_flow(
        self, db_session: AsyncSession, community: int
    ):
        op = await create_operation(db_session, id_community=community, name="Op")
        for member, (import_wh, export_wh) in enumerate([(80.0, 0.0), (0.0, 200.0)], start=1):
            device = await _device_in(
                db_session, id_community=community, operation=op, member=member
            )
            await create_hour_of_measurements(
                db_session,
                id_device=device,
                id_community=community,
                bucket=hour("2026-06-10 08:00"),
                import_wh=import_wh,
                export_wh=export_wh,
                production_wh=None,
            )
        await _project(db_session, community)

        await rollups.tick_community(db_session, id_community=community, now=T)

        row = (await _operation_hours(db_session, community))[(op, hour("2026-06-10 08:00"))]
        assert row.shared_wh == 320.0  # 4 quarters x min(80, 200)
        assert row.shared_wh <= min(row.import_wh, row.export_wh)


class TestEveryDeviceHourInExactlyOneRow:
    async def test_the_rows_of_a_bucket_sum_to_the_community_hour(
        self, db_session: AsyncSession, community: int
    ):
        """The remainder row is what makes this exact - and the global privacy
        verdict depends on it: it must know whether anything outside the visible
        operations exists at all."""
        op1 = await create_operation(db_session, id_community=community, name="Solar")
        op2 = await create_operation(db_session, id_community=community, name="Wind")
        bucket = hour("2026-06-10 07:00")
        for member, operation, (import_wh, export_wh) in [
            (1, op1, (10.0, 40.0)),
            (2, op2, (20.0, 5.0)),
            (3, None, (7.0, 3.0)),
        ]:
            device = await _device_in(
                db_session, id_community=community, operation=operation, member=member
            )
            await create_hour_of_measurements(
                db_session,
                id_device=device,
                id_community=community,
                bucket=bucket,
                import_wh=import_wh,
                export_wh=export_wh,
            )
        await _project(db_session, community)

        await rollups.tick_community(db_session, id_community=community, now=T)

        rows = {
            key[0]: row
            for key, row in (await _operation_hours(db_session, community)).items()
            if key[1] == bucket
        }
        assert set(rows) == {op1, op2, NO_SHARING_OPERATION}
        total = await _community_hour(db_session, community, bucket)
        assert sum(r.import_wh for r in rows.values()) == total.import_wh
        assert sum(r.export_wh for r in rows.values()) == total.export_wh
        assert sum(r.production_wh for r in rows.values()) == total.production_wh
        assert rows[NO_SHARING_OPERATION].shared_wh is None
        assert rows[op1].shared_wh is not None and rows[op2].shared_wh is not None

    async def test_overlapping_windows_go_to_the_remainder_counted_once(
        self, db_session: AsyncSession, community: int
    ):
        """Rollup invariant 4: a join against time-sliced ownership multiplies the
        totals. Two overlapping ACTIVE windows on one EAN are ambiguous - the
        device lands in the remainder, once, with no member."""
        op = await create_operation(db_session, id_community=community, name="Op")
        ean = await create_meter(db_session, id_community=community)
        for member in (1, 2):
            await create_meter_data(
                db_session,
                ean=ean,
                id_member=member,
                start_date=datetime.date(2026, 1, 1),
                id_sharing_operation=op,
            )
        device = await create_device(db_session, id_community=community, ean=ean)
        bucket = hour("2026-06-10 06:00")
        await create_hour_of_measurements(
            db_session, id_device=device, id_community=community, bucket=bucket, import_wh=25.0
        )
        await _project(db_session, community)

        await rollups.tick_community(db_session, id_community=community, now=T)

        rows = await _operation_hours(db_session, community)
        assert set(rows) == {(NO_SHARING_OPERATION, bucket)}
        assert rows[(NO_SHARING_OPERATION, bucket)].import_wh == 100.0
        assert rows[(NO_SHARING_OPERATION, bucket)].n_members == 0

    async def test_a_meter_waiting_for_the_dso_is_in_no_operation_yet(
        self, db_session: AsyncSession, community: int
    ):
        """WAITING_GRD: added to the operation in the CRM, not yet activated by the
        DSO - which shares nothing for it. The projection reads ACTIVE windows
        only, so its energy lands in the remainder (D-14)."""
        op = await create_operation(db_session, id_community=community, name="Op")
        device = await _device_in(
            db_session,
            id_community=community,
            operation=op,
            member=1,
            status=METER_DATA_WAITING_GRD,
        )
        bucket = hour("2026-06-10 06:00")
        await create_hour_of_measurements(
            db_session, id_device=device, id_community=community, bucket=bucket
        )
        await _project(db_session, community)

        await rollups.tick_community(db_session, id_community=community, now=T)

        assert set(await _operation_hours(db_session, community)) == {
            (NO_SHARING_OPERATION, bucket)
        }

    async def test_a_meter_changes_operation_at_its_local_midnight(
        self, db_session: AsyncSession, community: int
    ):
        """The window is matched on the BUCKET's Brussels date. 21:00Z is 23:00 on
        8 June in Brussels (old operation); 22:00Z is 00:00 on 9 June (new one).
        Each hour lands in exactly one operation and nothing is doubled."""
        old = await create_operation(db_session, id_community=community, name="Old")
        new = await create_operation(db_session, id_community=community, name="New")
        ean = await create_meter(db_session, id_community=community)
        await create_meter_data(
            db_session,
            ean=ean,
            id_member=1,
            start_date=datetime.date(2020, 1, 1),
            end_date=datetime.date(2026, 6, 8),
            id_sharing_operation=old,
        )
        await create_meter_data(
            db_session,
            ean=ean,
            id_member=1,
            start_date=datetime.date(2026, 6, 9),
            id_sharing_operation=new,
        )
        device = await create_device(db_session, id_community=community, ean=ean)
        for bucket in (hour("2026-06-08 21:00"), hour("2026-06-08 22:00")):
            await create_hour_of_measurements(
                db_session, id_device=device, id_community=community, bucket=bucket
            )
        await _project(db_session, community)

        await rollups.tick_community(db_session, id_community=community, now=T)

        assert set(await _operation_hours(db_session, community)) == {
            (old, hour("2026-06-08 21:00")),
            (new, hour("2026-06-08 22:00")),
        }


class TestMembers:
    async def test_a_member_is_counted_once_per_operation(
        self, db_session: AsyncSession, community: int
    ):
        """k thresholds on MEMBERS: a member with two meters is one member."""
        op = await create_operation(db_session, id_community=community, name="Op")
        bucket = hour("2026-06-10 05:00")
        for member in (1, 1, 2):
            device = await _device_in(
                db_session, id_community=community, operation=op, member=member
            )
            await create_hour_of_measurements(
                db_session, id_device=device, id_community=community, bucket=bucket
            )
        await _project(db_session, community)

        await rollups.tick_community(db_session, id_community=community, now=T)

        row = (await _operation_hours(db_session, community))[(op, bucket)]
        assert (row.n_devices, row.n_members) == (3, 2)


class TestTheDay:
    async def test_a_day_sums_its_hours_and_carries_its_least_populated_hour(
        self, db_session: AsyncSession, community: int
    ):
        """`n_members_min` is what the read side publishes a day on: judged on its
        MAX, "day minus its published hours" would reveal the withheld ones."""
        op = await create_operation(db_session, id_community=community, name="Op")
        alone = await _device_in(db_session, id_community=community, operation=op, member=1)
        joins_later = await _device_in(db_session, id_community=community, operation=op, member=2)
        # 9 June in Brussels: closed at T, and entirely inside the 48 h window.
        await create_hour_of_measurements(
            db_session, id_device=alone, id_community=community, bucket=hour("2026-06-09 06:00")
        )
        for device in (alone, joins_later):
            await create_hour_of_measurements(
                db_session,
                id_device=device,
                id_community=community,
                bucket=hour("2026-06-09 07:00"),
            )
        await _project(db_session, community)

        await rollups.tick_community(db_session, id_community=community, now=T)

        day = (
            await db_session.execute(
                text(
                    "SELECT import_wh, n_members, n_members_min, n_hours FROM rollup_operation_day "
                    "WHERE id_community = :c AND id_sharing_operation = :o AND bucket = :b"
                ),
                {"c": community, "o": op, "b": hour("2026-06-08 22:00")},
            )
        ).one()
        assert tuple(day) == (1200.0, 2, 1, 2)
        community_min = await db_session.scalar(
            text(
                "SELECT n_members_min FROM rollup_community_day WHERE id_community = :c "
                "AND bucket = :b"
            ),
            {"c": community, "b": hour("2026-06-08 22:00")},
        )
        assert community_min == 1

    async def test_the_25_hour_day_has_25_hours(self, db_session: AsyncSession, community: int):
        """25 October 2026 in Brussels runs 22:00Z on the 24th to 23:00Z on the
        25th. `day + 24h` or `bucket::date` would lose an hour of energy."""
        op = await create_operation(db_session, id_community=community, name="Op")
        device = await _device_in(db_session, id_community=community, operation=op, member=1)
        start = hour("2026-10-24 22:00")
        for offset in range(25):
            await create_hour_of_measurements(
                db_session,
                id_device=device,
                id_community=community,
                bucket=start + datetime.timedelta(hours=offset),
            )
        now = hour("2026-10-27 12:00")
        await _project(db_session, community, now)

        await rollups.tick_community(db_session, id_community=community, now=now)

        n_hours = await db_session.scalar(
            text(
                "SELECT n_hours FROM rollup_operation_day WHERE id_community = :c "
                "AND id_sharing_operation = :o AND bucket = :b"
            ),
            {"c": community, "o": op, "b": start},
        )
        assert n_hours == 25


class TestErosion:
    async def test_the_tick_at_t_and_at_t_plus_49h_leaves_old_operation_rows_untouched(
        self, db_session: AsyncSession, community: int
    ):
        """The step-8 gate, for the new table: rows that fall below the new `lo`
        must be byte-identical, `computed_at` included."""
        op = await create_operation(db_session, id_community=community, name="Op")
        device = await _device_in(db_session, id_community=community, operation=op, member=1)
        lo, hi = buckets.window(T)
        cursor = lo
        while cursor < hi:
            await create_hour_of_measurements(
                db_session, id_device=device, id_community=community, bucket=cursor, export_wh=40.0
            )
            cursor += datetime.timedelta(hours=1)
        await _project(db_session, community)

        await rollups.tick_community(db_session, id_community=community, now=T)
        before = await _operation_hours(db_session, community)
        assert len(before) == 48

        later = T + datetime.timedelta(hours=49)
        await rollups.tick_community(db_session, id_community=community, now=later)
        after = await _operation_hours(db_session, community)

        new_lo, _ = buckets.window(later)
        original = {key: row for key, row in before.items() if key[1] < new_lo}
        assert original, "the fixture must leave rows below the new lo or this asserts nothing"
        assert {key: tuple(after[key]) for key in original} == {
            key: tuple(row) for key, row in original.items()
        }
