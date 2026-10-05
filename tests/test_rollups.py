"""The rollup tick. Build step 8's gate.

plan 13: "Four independent designs produced four rollups and the adversarial pass
found a fatal defect in every one. Treat step 8 as design, not translation, and
test the tick at T and T+49 h."

Every test here is one of those defects. None of them raises in production - the
totals stay plausible, the chart stays smooth, and the numbers are wrong.
"""

import datetime
from collections.abc import Sequence

import pytest
from sqlalchemy import Row, text
from sqlalchemy.ext.asyncio import AsyncSession

from domain import buckets
from ports.crm_core import SqlAlchemyCrmCoreRead
from tests.factories.device_factory import (
    create_device,
    create_hour_of_measurements,
    create_measurement,
)
from tests.factories.meter_factory import create_meter, create_meter_data, create_owned_meter
from tests.factories.subscription_factory import create_community
from worker import retention, rollups
from worker.ingest import _UPSERT_SQL as INGEST_UPSERT_SQL
from worker.ownership import refresh_community

# A fixed instant, so every window in this file is computable by hand.
# window(T) = [2026-06-08 11:00, 2026-06-10 11:00)
T = datetime.datetime(2026, 6, 10, 10, 30, tzinfo=datetime.UTC)


def hour(value: str) -> datetime.datetime:
    return datetime.datetime.fromisoformat(value).replace(tzinfo=datetime.UTC)


async def _device_hours(session: AsyncSession, id_community: int) -> list:
    rows = await session.execute(
        text(
            "SELECT id_device, bucket, import_wh, export_wh, production_wh, "
            "n_samples, n_production_samples, computed_at "
            "FROM rollup_device_hour WHERE id_community = :c ORDER BY id_device, bucket"
        ),
        {"c": id_community},
    )
    return [tuple(row) for row in rows.all()]


async def _community_hours(session: AsyncSession, id_community: int) -> Sequence[Row]:
    rows = await session.execute(
        text(
            "SELECT bucket, import_wh, export_wh, production_wh, n_devices, "
            "n_devices_production, n_members, n_devices_unattributed "
            "FROM rollup_community_hour WHERE id_community = :c ORDER BY bucket"
        ),
        {"c": id_community},
    )
    return rows.all()


@pytest.fixture
async def setup(db_session: AsyncSession):
    """One community, one production device, one clean open-ended owner window."""
    community = await create_community(db_session)
    ean = await create_owned_meter(db_session, id_community=community.id, id_member=11)
    device_id = await create_device(db_session, id_community=community.id, ean=ean)
    await refresh_community(
        db_session, SqlAlchemyCrmCoreRead(db_session), id_community=community.id, now=T
    )
    return community.id, device_id, ean


class TestTheStatementsAgree:
    """Two string-level assertions that each stand in for a silent data defect."""

    def test_the_same_target_predicate_sweeps_and_produces(self):
        """MARK AND SWEEP, asserted at the source level.

        A DELETE narrower than the INSERT violates the primary key and is loud.
        A DELETE wider removes rows that are never reinserted, and the only
        symptom is a hole in a chart nobody can date. The predicate is written
        once precisely so the two cannot drift; this asserts it stayed that way.
        """
        pairs = [
            (rollups._DELETE_DEVICE_HOUR_SQL, rollups._INSERT_DEVICE_HOUR_SQL),
            (rollups._DELETE_COMMUNITY_HOUR_SQL, rollups._INSERT_COMMUNITY_HOUR_SQL),
            (rollups._DELETE_DEVICE_DAY_SQL, rollups._INSERT_DEVICE_DAY_SQL),
            (rollups._DELETE_COMMUNITY_DAY_SQL, rollups._INSERT_COMMUNITY_DAY_SQL),
            (rollups._DELETE_OPERATION_HOUR_SQL, rollups._INSERT_OPERATION_HOUR_SQL),
            (rollups._DELETE_OPERATION_DAY_SQL, rollups._INSERT_OPERATION_DAY_SQL),
        ]
        for delete_sql, insert_sql in pairs:
            assert rollups._TARGET in str(delete_sql)
            assert rollups._TARGET in str(insert_sql)

    def test_ingest_and_the_tick_derive_the_same_bucket(self):
        """If these two expressions ever disagreed, ingest would mark one bucket
        and the tick would recompute another: the mark would never clear, the
        chart would never correct, and no statement would fail."""
        assert buckets.bucket_sql("m.ts") in str(rollups._INSERT_DEVICE_HOUR_SQL)
        assert buckets.bucket_sql("s.ts") in str(INGEST_UPSERT_SQL)
        # The operation pass reads `measurement` again for the per-quarter shared
        # figure, and must file each reading in the same bucket.
        assert buckets.bucket_sql("m.ts") in str(rollups._INSERT_OPERATION_HOUR_SQL)

    def test_no_statement_calls_now(self):
        """`now` is a parameter everywhere. A single `now()` in the SQL and the
        T / T+49 h test below can only ever assert against the wall clock."""
        for sql in (
            rollups._INSERT_DEVICE_HOUR_SQL,
            rollups._INSERT_COMMUNITY_HOUR_SQL,
            rollups._INSERT_DEVICE_DAY_SQL,
            rollups._INSERT_COMMUNITY_DAY_SQL,
            rollups._INSERT_OPERATION_HOUR_SQL,
            rollups._INSERT_OPERATION_DAY_SQL,
        ):
            assert "NOW()" not in str(sql).upper()


class TestTheHourBucket:
    async def test_an_hour_sums_its_four_quarters(self, db_session: AsyncSession, setup):
        id_community, device_id, _ = setup
        await create_hour_of_measurements(
            db_session,
            id_device=device_id,
            id_community=id_community,
            bucket=hour("2026-06-10 09:00"),
            import_wh=100.0,
            production_wh=250.0,
        )
        await rollups.tick_community(db_session, id_community=id_community, now=T)

        rows = await _device_hours(db_session, id_community)
        assert len(rows) == 1
        assert rows[0][1] == hour("2026-06-10 09:00")
        assert rows[0][2] == pytest.approx(400.0)
        assert rows[0][4] == pytest.approx(1000.0)
        assert rows[0][5] == 4

    async def test_a_reading_on_the_hour_belongs_to_the_previous_bucket(
        self, db_session: AsyncSession, setup
    ):
        """THE defect this whole module is shaped around.

        `ts` is the END of the interval, so 10:00:00Z closes 09:45-10:00 and
        belongs to bucket 09:00. `date_trunc('hour', ts)` - the expression every
        implementation reaches for first - files it under 10:00 and moves a
        quarter of every hour's energy into the next hour, for ever.
        """
        await create_measurement(
            db_session,
            id_device=setup[1],
            id_community=setup[0],
            ts=hour("2026-06-10 10:00"),
            import_wh=77.0,
        )
        await rollups.tick_community(db_session, id_community=setup[0], now=T)

        rows = await _device_hours(db_session, setup[0])
        assert [row[1] for row in rows] == [hour("2026-06-10 09:00")]

    async def test_production_is_null_not_zero_when_nothing_measures_it(
        self, db_session: AsyncSession, setup
    ):
        """NULL means "this device does not measure production"; 0 asserts
        "produced nothing" - a different and false statement that then
        propagates into the community balance."""
        id_community, device_id, _ = setup
        await create_hour_of_measurements(
            db_session,
            id_device=device_id,
            id_community=id_community,
            bucket=hour("2026-06-10 09:00"),
            production_wh=None,
        )
        await rollups.tick_community(db_session, id_community=id_community, now=T)

        rows = await _device_hours(db_session, id_community)
        assert rows[0][4] is None
        assert rows[0][6] == 0

        community = await _community_hours(db_session, id_community)
        assert community[0].production_wh is None
        assert community[0].n_devices_production == 0


class TestTheWindow:
    async def test_the_tick_at_t_and_at_t_plus_49h_does_not_erode(
        self, db_session: AsyncSession, setup
    ):
        """THE EROSION TEST, and the gate for this step.

        Fill the whole 48-hour window, tick, then tick again 49 hours later with
        NO new data. Every row that has fallen below the new `lo` must be
        BYTE-IDENTICAL, `computed_at` included.

        What this catches: an unaligned window (`lo = now - 48h`) recomputes the
        oldest hour from the fragment still inside the window and overwrites a
        correct row with a partial one. Every hour is eroded on its way out,
        silently, for ever - and a test that only ticks once cannot see it.
        """
        id_community, device_id, _ = setup
        lo, hi = buckets.window(T)
        cursor = lo
        while cursor < hi:
            await create_hour_of_measurements(
                db_session, id_device=device_id, id_community=id_community, bucket=cursor
            )
            cursor += datetime.timedelta(hours=1)

        await rollups.tick_community(db_session, id_community=id_community, now=T)
        before = await _device_hours(db_session, id_community)
        assert len(before) == 48

        later = T + datetime.timedelta(hours=49)
        await rollups.tick_community(db_session, id_community=id_community, now=later)
        after = await _device_hours(db_session, id_community)

        new_lo, _ = buckets.window(later)
        survivors = [row for row in after if row[1] < new_lo]
        original = [row for row in before if row[1] < new_lo]
        assert original, "the fixture must leave rows below the new lo or this asserts nothing"
        assert survivors == original

    async def test_an_unaligned_window_breaks_the_test_above(
        self, db_session: AsyncSession, setup, monkeypatch
    ):
        """NEGATIVE CONTROL - proves the erosion test is sensitive to alignment.

        Swap `buckets.window` for the naive `now - 48h` and re-run the same
        fixture. The tick must NOT produce the 48 intact rows the test above
        asserts. Without this, an erosion test whose fixture happened never to
        exercise the boundary would pass for ever while asserting nothing about
        the window at all.

        The failure shape differs from the classic one and is worth naming: this
        design states its target as an explicit list of bucket instants, so an
        unaligned `lo` yields targets on the half hour that match no bucket -
        the tick writes nothing instead of writing eroded rows. Quieter, and
        equally wrong.
        """

        def unaligned(now: datetime.datetime):
            return now - datetime.timedelta(hours=48), buckets.hour_floor(now) + datetime.timedelta(
                hours=1
            )

        monkeypatch.setattr(buckets, "window", unaligned)

        id_community, device_id, _ = setup
        lo, hi = unaligned(T)
        cursor = buckets.hour_floor(lo)
        while cursor < hi:
            await create_hour_of_measurements(
                db_session, id_device=device_id, id_community=id_community, bucket=cursor
            )
            cursor += datetime.timedelta(hours=1)

        await rollups.tick_community(db_session, id_community=id_community, now=T)
        rows = await _device_hours(db_session, id_community)
        assert len(rows) != 48

    async def test_every_surviving_hour_still_has_four_samples(
        self, db_session: AsyncSession, setup
    ):
        """The same defect, stated as the number a human would notice.

        An eroded hour keeps its row and loses its samples, so `n_samples` drops
        from 4 to 1 or 2 while the energy drops with it. Asserting the count
        makes the failure legible in the test output.
        """
        id_community, device_id, _ = setup
        for offset in range(6):
            await create_hour_of_measurements(
                db_session,
                id_device=device_id,
                id_community=id_community,
                bucket=hour("2026-06-08 11:00") + datetime.timedelta(hours=offset),
            )
        await rollups.tick_community(db_session, id_community=id_community, now=T)
        await rollups.tick_community(
            db_session, id_community=id_community, now=T + datetime.timedelta(hours=49)
        )

        rows = await _device_hours(db_session, id_community)
        assert {row[5] for row in rows} == {4}

    async def test_a_bucket_written_at_the_window_edge_is_still_recomputed(
        self, db_session: AsyncSession, setup
    ):
        """THE EDGE RACE.

        A measurement for the oldest bucket in the window, written at T, ticked
        at T+61 min - by which time that bucket has left the window. It is only
        recomputed because ingest marked it dirty UNCONDITIONALLY. Any design
        that marks dirty only for data outside the window fails here, and in
        production loses the reading with no error.
        """
        id_community, device_id, _ = setup
        lo, _hi = buckets.window(T)
        await create_hour_of_measurements(
            db_session, id_device=device_id, id_community=id_community, bucket=lo
        )

        later = T + datetime.timedelta(minutes=61)
        assert lo < buckets.window(later)[0], "the fixture must put the bucket outside the window"

        await rollups.tick_community(db_session, id_community=id_community, now=later)
        rows = await _device_hours(db_session, id_community)
        assert [row[1] for row in rows] == [lo]

    async def test_the_claim_empties_rollup_dirty(self, db_session: AsyncSession, setup):
        id_community, device_id, _ = setup
        await create_hour_of_measurements(
            db_session,
            id_device=device_id,
            id_community=id_community,
            bucket=hour("2026-06-10 09:00"),
        )
        pending = await db_session.scalar(
            text("SELECT count(*) FROM rollup_dirty WHERE id_community = :c"), {"c": id_community}
        )
        assert pending == 1, "ingest's upsert must have marked the bucket"

        await rollups.tick_community(db_session, id_community=id_community, now=T)
        remaining = await db_session.scalar(
            text("SELECT count(*) FROM rollup_dirty WHERE id_community = :c"), {"c": id_community}
        )
        assert remaining == 0

    async def test_a_claim_below_the_raw_edge_is_consumed_and_its_day_survives(
        self, db_session: AsyncSession, setup
    ):
        """Its readings are gone. Recomputing it would DELETE the hour, re-insert
        nothing, and re-derive the day from nothing - erasing the one series kept
        for ever. The claim is consumed (so it does not linger) and nothing moves."""
        id_community, _, _ = setup
        old = retention.raw_floor(T) - datetime.timedelta(days=40)
        # The REAL day bucket of that hour (Brussels midnight, in UTC) - the row a
        # recompute would delete. Any other instant would make this test blind.
        (old_day,) = buckets.day_targets([old], T)
        await db_session.execute(
            text(
                "INSERT INTO rollup_community_hour (id_community, bucket, import_wh, export_wh, "
                "n_devices, n_devices_production, n_members, n_devices_unattributed, computed_at) "
                "VALUES (:c, :b, 7, 0, 1, 0, 1, 0, :t)"
            ),
            {"c": id_community, "b": old, "t": T},
        )
        await db_session.execute(
            text(
                "INSERT INTO rollup_community_day (id_community, bucket, import_wh, export_wh, "
                "n_devices, n_devices_production, n_members, n_devices_unattributed, n_hours, "
                "computed_at) VALUES (:c, :b, 168, 0, 1, 0, 1, 0, 24, :t)"
            ),
            {"c": id_community, "b": old_day, "t": T},
        )
        await db_session.execute(
            text("INSERT INTO rollup_dirty (id_community, bucket) VALUES (:c, :b)"),
            {"c": id_community, "b": old},
        )

        await rollups.tick_community(db_session, id_community=id_community, now=T)

        counts = (
            await db_session.execute(
                text(
                    "SELECT (SELECT count(*) FROM rollup_dirty WHERE id_community = :c), "
                    "(SELECT count(*) FROM rollup_community_hour WHERE id_community = :c "
                    "AND bucket = :b), "
                    "(SELECT count(*) FROM rollup_community_day WHERE id_community = :c "
                    "AND bucket = :d)"
                ),
                {"c": id_community, "b": old, "d": old_day},
            )
        ).one()
        assert tuple(counts) == (0, 1, 1)

    async def test_a_ten_day_old_backfill_is_recomputed(self, db_session: AsyncSession, setup):
        """Store-and-forward: a device reconnects after a week and delivers the
        week. Those buckets are far outside the 48-hour window and reach the tick
        only through `rollup_dirty`."""
        id_community, device_id, _ = setup
        old = hour("2026-05-31 09:00")
        await create_hour_of_measurements(
            db_session, id_device=device_id, id_community=id_community, bucket=old
        )
        await rollups.tick_community(db_session, id_community=id_community, now=T)

        rows = await _device_hours(db_session, id_community)
        assert old in [row[1] for row in rows]


class TestMembership:
    async def test_n_members_counts_members_not_devices(self, db_session: AsyncSession):
        """plan 9.3: "a member with three meters is one member; counting devices
        makes the guarantee decorative"."""
        community = await create_community(db_session)
        for _ in range(3):
            ean = await create_owned_meter(db_session, id_community=community.id, id_member=5)
            device_id = await create_device(db_session, id_community=community.id, ean=ean)
            await create_hour_of_measurements(
                db_session,
                id_device=device_id,
                id_community=community.id,
                bucket=hour("2026-06-10 09:00"),
            )
        await refresh_community(
            db_session, SqlAlchemyCrmCoreRead(db_session), id_community=community.id, now=T
        )
        await rollups.tick_community(db_session, id_community=community.id, now=T)

        rows = await _community_hours(db_session, community.id)
        assert rows[0].n_devices == 3
        assert rows[0].n_members == 1

    async def test_overlapping_windows_do_not_double_the_community_energy(
        self, db_session: AsyncSession
    ):
        """THE FAN-OUT. A LEFT JOIN against `device_owner_window` inside the
        energy aggregation produces one row per matching window, so a meter with
        two overlapping windows contributes its energy TWICE - and the community
        total is simply wrong, with nothing to indicate it.

        Membership and energy are aggregated in separate CTEs so this cannot
        happen; this asserts the separation held.
        """
        community = await create_community(db_session)
        ean = await create_meter(db_session, id_community=community.id)
        await create_meter_data(
            db_session,
            ean=ean,
            id_member=1,
            start_date=datetime.date(2026, 1, 1),
            end_date=datetime.date(2026, 12, 31),
        )
        await create_meter_data(
            db_session, ean=ean, id_member=2, start_date=datetime.date(2026, 6, 1)
        )
        device_id = await create_device(db_session, id_community=community.id, ean=ean)
        await create_hour_of_measurements(
            db_session,
            id_device=device_id,
            id_community=community.id,
            bucket=hour("2026-06-10 09:00"),
            import_wh=100.0,
        )
        await refresh_community(
            db_session, SqlAlchemyCrmCoreRead(db_session), id_community=community.id, now=T
        )
        await rollups.tick_community(db_session, id_community=community.id, now=T)

        rows = await _community_hours(db_session, community.id)
        assert len(rows) == 1
        assert rows[0].import_wh == pytest.approx(400.0)  # NOT 800
        assert rows[0].n_devices == 1

    async def test_an_ambiguous_owner_counts_energy_but_no_member(self, db_session: AsyncSession):
        """The other half of the same case, and the privacy-relevant one.

        Excluding the energy would understate the community total; counting the
        member would overstate `n_members`, and k thresholds on it. Undercounting
        over-suppresses; overcounting IS the privacy failure.
        """
        community = await create_community(db_session)
        ean = await create_meter(db_session, id_community=community.id)
        await create_meter_data(
            db_session,
            ean=ean,
            id_member=1,
            start_date=datetime.date(2026, 1, 1),
            end_date=datetime.date(2026, 12, 31),
        )
        await create_meter_data(
            db_session, ean=ean, id_member=2, start_date=datetime.date(2026, 6, 1)
        )
        device_id = await create_device(db_session, id_community=community.id, ean=ean)
        await create_hour_of_measurements(
            db_session,
            id_device=device_id,
            id_community=community.id,
            bucket=hour("2026-06-10 09:00"),
        )
        await refresh_community(
            db_session, SqlAlchemyCrmCoreRead(db_session), id_community=community.id, now=T
        )
        await rollups.tick_community(db_session, id_community=community.id, now=T)

        rows = await _community_hours(db_session, community.id)
        assert rows[0].n_members == 0
        assert rows[0].n_devices_unattributed == 1
        assert rows[0].import_wh == pytest.approx(400.0)


class TestTheDayRollup:
    async def test_a_closed_day_totals_its_hours(self, db_session: AsyncSession, setup):
        id_community, device_id, _ = setup
        # 2026-06-09 local = 2026-06-08 22:00 UTC to 2026-06-09 22:00 UTC (CEST).
        start = hour("2026-06-08 22:00")
        for offset in range(24):
            await create_hour_of_measurements(
                db_session,
                id_device=device_id,
                id_community=id_community,
                bucket=start + datetime.timedelta(hours=offset),
                import_wh=10.0,
            )
        await rollups.tick_community(db_session, id_community=id_community, now=T)

        row = (
            await db_session.execute(
                text(
                    "SELECT bucket, import_wh, n_hours FROM rollup_device_day "
                    "WHERE id_community = :c ORDER BY bucket"
                ),
                {"c": id_community},
            )
        ).all()
        assert len(row) == 1
        assert row[0].bucket == start
        assert row[0].n_hours == 24
        assert row[0].import_wh == pytest.approx(24 * 40.0)

    async def test_a_day_still_in_progress_produces_no_row(self, db_session: AsyncSession, setup):
        """plan 6.3: "Day rollups are derived from whole days only. Deriving them
        from a partial day freezes a partial total for ever, because closed
        periods are never revisited"."""
        id_community, device_id, _ = setup
        await create_hour_of_measurements(
            db_session,
            id_device=device_id,
            id_community=id_community,
            bucket=hour("2026-06-10 09:00"),  # T's own local day, still open
        )
        await rollups.tick_community(db_session, id_community=id_community, now=T)

        count = await db_session.scalar(
            text("SELECT count(*) FROM rollup_device_day WHERE id_community = :c"),
            {"c": id_community},
        )
        assert count == 0

    @pytest.mark.parametrize(
        ("day_start_utc", "expected_hours"),
        [
            ("2026-10-24 22:00", 25),  # last Sunday in October: CEST -> CET
            ("2026-03-28 23:00", 23),  # last Sunday in March: CET -> CEST
        ],
    )
    async def test_the_two_dst_days_are_twenty_five_and_twenty_three_hours(
        self, db_session: AsyncSession, day_start_utc: str, expected_hours: int
    ):
        """`day + 24 hours` and `bucket::date` are both wrong on exactly these two
        days, by exactly one hour of community energy, and neither raises.

        25 and 23 are CORRECT DATA here, not anomalies to be normalised away.
        """
        community = await create_community(db_session)
        ean = await create_owned_meter(db_session, id_community=community.id, id_member=1)
        device_id = await create_device(db_session, id_community=community.id, ean=ean)

        start = hour(day_start_utc)
        for offset in range(expected_hours):
            await create_hour_of_measurements(
                db_session,
                id_device=device_id,
                id_community=community.id,
                bucket=start + datetime.timedelta(hours=offset),
                import_wh=10.0,
            )
        # A `now` safely after the day closes, so `is_day_closed` passes.
        now = start + datetime.timedelta(hours=expected_hours + 2)
        await refresh_community(
            db_session, SqlAlchemyCrmCoreRead(db_session), id_community=community.id, now=now
        )
        await rollups.tick_community(db_session, id_community=community.id, now=now)

        row = (
            await db_session.execute(
                text(
                    "SELECT bucket, n_hours, import_wh FROM rollup_device_day "
                    "WHERE id_community = :c"
                ),
                {"c": community.id},
            )
        ).one()
        assert row.bucket == start
        assert row.n_hours == expected_hours
        assert row.import_wh == pytest.approx(expected_hours * 40.0)

    async def test_the_community_day_takes_the_max_of_n_members_not_the_sum(
        self, db_session: AsyncSession
    ):
        """SUM gives a five-member community an `n_members` of 120, and k then
        passes on a day that should have been suppressed. MAX is a valid lower
        bound on the day's distinct union, so it fails closed."""
        community = await create_community(db_session)
        ean = await create_owned_meter(db_session, id_community=community.id, id_member=1)
        device_id = await create_device(db_session, id_community=community.id, ean=ean)
        start = hour("2026-06-08 22:00")
        for offset in range(24):
            await create_hour_of_measurements(
                db_session,
                id_device=device_id,
                id_community=community.id,
                bucket=start + datetime.timedelta(hours=offset),
            )
        await refresh_community(
            db_session, SqlAlchemyCrmCoreRead(db_session), id_community=community.id, now=T
        )
        await rollups.tick_community(db_session, id_community=community.id, now=T)

        row = (
            await db_session.execute(
                text("SELECT n_members, n_hours FROM rollup_community_day WHERE id_community = :c"),
                {"c": community.id},
            )
        ).one()
        assert row.n_hours == 24
        assert row.n_members == 1


class TestHourTargets:
    def test_the_window_hours_are_all_present(self):
        lo, hi = buckets.window(T)
        targets = rollups.hour_targets(lo, hi, [])
        assert len(targets) == 48
        assert targets[0] == lo
        assert targets[-1] == hi - datetime.timedelta(hours=1)

    def test_a_claimed_bucket_inside_the_window_is_not_duplicated(self):
        lo, hi = buckets.window(T)
        targets = rollups.hour_targets(lo, hi, [lo, lo + datetime.timedelta(hours=1)])
        assert len(targets) == 48
        assert len(set(targets)) == len(targets)

    def test_a_claimed_bucket_outside_the_window_extends_the_target_set(self):
        lo, hi = buckets.window(T)
        old = hour("2026-01-01 00:00")
        targets = rollups.hour_targets(lo, hi, [old])
        assert targets[0] == old
        assert len(targets) == 49
