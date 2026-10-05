"""The scheduler: the grid, the locks, and the bootstrap. Build step 8.

The jobs themselves are tested in `test_rollups.py`, `test_ownership_projection.py`
and `test_partition_maintenance.py`. What is left here is the part that decides
WHEN they run and WHETHER two processes can run them at once - and those two are
where a scheduler fails silently rather than loudly.
"""

import datetime
import inspect
import time

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core import metrics as app_metrics
from core.config import settings
from domain import buckets
from shared import const
from tests.conftest import sessionmaker_for
from tests.factories.device_factory import create_device, create_hour_of_measurements
from tests.factories.meter_factory import create_owned_meter
from tests.factories.subscription_factory import create_community
from worker import rollups, scheduler
from worker.context import advisory_lock
from worker.subscriptions import SubscriptionCache, SubscriptionsUnavailable

NOW = datetime.datetime(2026, 9, 16, 10, 37, 12, tzinfo=datetime.UTC)


class TestTheGrid:
    def test_the_next_tick_is_on_the_wall_clock_grid(self):
        """Not `now + 15 min`. An interval-based scheduler re-phases on every
        restart, so after a few deploys the tick that closes an hour lands at a
        different offset than it did last week - and "the rollup runs at :01,
        :16, :31, :46" stops being a thing anyone can rely on."""
        assert scheduler.next_tick(NOW, minutes=15, offset_seconds=90) == datetime.datetime(
            2026, 9, 16, 10, 46, 30, tzinfo=datetime.UTC
        )

    def test_the_offset_is_inside_the_slot(self):
        """+90 s, so a device publishing at :00 has arrived, validated and
        committed before the tick that closes its interval reads the table."""
        at_the_boundary = datetime.datetime(2026, 9, 16, 10, 30, tzinfo=datetime.UTC)
        assert scheduler.next_tick(
            at_the_boundary, minutes=15, offset_seconds=90
        ) == datetime.datetime(2026, 9, 16, 10, 31, 30, tzinfo=datetime.UTC)

    def test_the_next_tick_is_always_in_the_future(self):
        """A tick exactly on the offset must move to the NEXT slot, not return
        itself - otherwise the loop spins with a zero delay."""
        exactly = datetime.datetime(2026, 9, 16, 10, 31, 30, tzinfo=datetime.UTC)
        assert scheduler.next_tick(exactly, minutes=15, offset_seconds=90) > exactly

    def test_the_last_slot_of_the_hour_rolls_over(self):
        late = datetime.datetime(2026, 9, 16, 10, 59, tzinfo=datetime.UTC)
        assert scheduler.next_tick(late, minutes=15, offset_seconds=90) == datetime.datetime(
            2026, 9, 16, 11, 1, 30, tzinfo=datetime.UTC
        )

    def test_the_configured_interval_divides_the_hour(self):
        """`core/config.py` asserts this at boot, unconditionally. Restated here
        so the reason is visible next to the code that depends on it: a value
        like 7 drifts the grid every hour."""
        assert 60 % settings.ROLLUP_TICK_MINUTES == 0


class TestMaintenanceSchedule:
    def test_it_does_not_run_before_its_hour(self):
        early = datetime.datetime(2026, 9, 16, 1, 0, tzinfo=datetime.UTC)
        assert scheduler.maintenance_is_due(early, None) is False

    def test_it_runs_once_at_its_hour(self):
        due = datetime.datetime(2026, 9, 16, 2, 1, tzinfo=datetime.UTC)
        assert scheduler.maintenance_is_due(due, None) is True

    def test_it_does_not_run_twice_in_one_day(self):
        first = datetime.datetime(2026, 9, 16, 2, 1, tzinfo=datetime.UTC)
        later = datetime.datetime(2026, 9, 16, 14, 0, tzinfo=datetime.UTC)
        assert scheduler.maintenance_is_due(later, first) is False

    def test_it_runs_again_the_next_day(self):
        """Compared on the UTC DATE, not on elapsed hours. "More than 24 h ago"
        drifts forward by one tick interval every day until the job eventually
        runs at an hour nobody chose."""
        first = datetime.datetime(2026, 9, 16, 2, 1, tzinfo=datetime.UTC)
        tomorrow = datetime.datetime(2026, 9, 17, 2, 1, tzinfo=datetime.UTC)
        assert scheduler.maintenance_is_due(tomorrow, first) is True


class TestOwnershipSchedule:
    """Hourly, and on the first tick after a device appears on a new EAN.

    Found 2026-10-04: three prosumers created just after an hourly refresh read
    `n_members = 0` for the next hour and a quarter, so every grid term was
    withheld and the dashboard showed nothing at all.
    """

    KNOWN = frozenset({(1, "541448200000000003")})

    def test_it_runs_on_the_first_tick(self):
        assert scheduler.ownership_is_due(NOW, None, known=frozenset(), current=None) is True

    def test_it_waits_for_the_hour_when_nothing_changed(self):
        last = NOW - datetime.timedelta(minutes=30)
        assert scheduler.ownership_is_due(NOW, last, known=self.KNOWN, current=self.KNOWN) is False

    def test_it_runs_once_the_hour_has_passed(self):
        last = NOW - datetime.timedelta(minutes=settings.OWNERSHIP_REFRESH_MINUTES)
        assert scheduler.ownership_is_due(NOW, last, known=self.KNOWN, current=self.KNOWN) is True

    def test_a_device_on_a_new_ean_does_not_wait_for_the_hour(self):
        last = NOW - datetime.timedelta(minutes=15)
        current = self.KNOWN | {(1, "541448200000000001")}
        assert scheduler.ownership_is_due(NOW, last, known=self.KNOWN, current=current) is True

    def test_the_same_ean_in_another_community_is_new(self):
        """The projection is per community: the pair is the key, not the EAN."""
        last = NOW - datetime.timedelta(minutes=15)
        current = self.KNOWN | {(2, "541448200000000003")}
        assert scheduler.ownership_is_due(NOW, last, known=self.KNOWN, current=current) is True

    def test_a_vanished_ean_does_not_trigger_a_refresh(self):
        """Device rows are not deleted in normal operation; one that was has no
        new owner to look up, and the hourly refresh tidies its windows."""
        last = NOW - datetime.timedelta(minutes=15)
        assert scheduler.ownership_is_due(NOW, last, known=self.KNOWN, current=frozenset()) is False

    def test_an_unreadable_ean_set_falls_back_to_the_hour(self):
        last = NOW - datetime.timedelta(minutes=15)
        assert scheduler.ownership_is_due(NOW, last, known=self.KNOWN, current=None) is False


class TestDeviceEans:
    async def test_every_ean_that_ever_had_a_device_once_per_community(
        self, db_session: AsyncSession
    ):
        """Revoked devices included - their measurements still need an owner,
        which is why the projection covers them - and a replaced device's EAN
        is one pair, not two."""
        first, second = 98101, 98102
        await create_device(
            db_session, id_community=first, ean="EAN-A", status=const.DeviceStatus.REVOKED
        )
        await create_device(db_session, id_community=first, ean="EAN-A")
        await create_device(
            db_session, id_community=second, ean="EAN-B", status=const.DeviceStatus.REVOKED
        )

        pairs = await scheduler.device_eans(sessionmaker_for(db_session))

        assert {pair for pair in pairs if pair[0] in (first, second)} == {
            (first, "EAN-A"),
            (second, "EAN-B"),
        }


class TestTheLoopBody:
    """`scheduler_main._run_once`: what runs on one wake, and in what order."""

    @pytest.fixture
    def jobs(self, monkeypatch):
        """Record the jobs instead of running them. `eans` is what the device
        table holds on the next wake; `fail` names jobs that raise."""
        state = {"calls": [], "eans": frozenset({(1, "EAN-A")}), "fail": set()}

        async def device_eans(_sessions):
            if "eans" in state["fail"]:
                raise ConnectionRefusedError("local db down")
            return state["eans"]

        def job(name, result):
            async def run(*_args, **_kwargs):
                state["calls"].append(name)
                if name in state["fail"]:
                    raise RuntimeError(f"{name} failed")
                return result

            return run

        monkeypatch.setattr(scheduler, "device_eans", device_eans)
        monkeypatch.setattr(scheduler, "run_ownership", job("ownership", 1))
        monkeypatch.setattr(scheduler, "run_rollups", job("rollups", 1))
        monkeypatch.setattr(scheduler, "run_maintenance", job("maintenance", None))
        return state

    @staticmethod
    async def _wake(loop_state, at: datetime.datetime) -> None:
        from worker import scheduler_main

        async def load() -> frozenset[int]:
            return frozenset({1})

        await scheduler_main._run_once(
            None, None, SubscriptionCache(load, ttl_seconds=60), loop_state, now=at
        )

    @staticmethod
    def _state():
        from worker import scheduler_main

        return scheduler_main._LoopState()

    async def test_ownership_runs_before_the_rollup_tick(self, jobs):
        """So the tick that follows a refresh already counts the new member,
        instead of the one fifteen minutes later."""
        await self._wake(self._state(), NOW)

        assert jobs["calls"][:2] == ["ownership", "rollups"]

    async def test_a_new_device_is_projected_on_the_next_wake(self, jobs):
        loop_state = self._state()
        await self._wake(loop_state, NOW)
        jobs["calls"].clear()

        # Fifteen minutes later, nothing new: the hour has not passed.
        await self._wake(loop_state, NOW + datetime.timedelta(minutes=15))
        assert "ownership" not in jobs["calls"]

        # A manager adds a device on another meter.
        jobs["calls"].clear()
        jobs["eans"] = jobs["eans"] | {(1, "EAN-B")}
        await self._wake(loop_state, NOW + datetime.timedelta(minutes=30))
        assert jobs["calls"][0] == "ownership"
        assert loop_state.known_eans == frozenset({(1, "EAN-A"), (1, "EAN-B")})

    async def test_a_failed_refresh_leaves_the_device_new(self, jobs):
        """Retried on the next wake rather than marked done - and the rollup
        tick still runs."""
        loop_state = self._state()
        jobs["fail"].add("ownership")

        await self._wake(loop_state, NOW)

        assert "rollups" in jobs["calls"]
        assert loop_state.last_ownership is None
        assert loop_state.known_eans == frozenset()

    async def test_an_unreadable_device_table_never_stops_the_tick(self, jobs, caplog):
        loop_state = self._state()
        await self._wake(loop_state, NOW)
        jobs["calls"].clear()
        jobs["fail"].add("eans")

        with caplog.at_level("WARNING", logger="worker.scheduler_main"):
            await self._wake(loop_state, NOW + datetime.timedelta(minutes=15))

        assert jobs["calls"] == ["rollups"]
        assert any(
            getattr(record, "operation", None) == "scheduler:device-eans-unavailable"
            for record in caplog.records
        )


class TestAdvisoryLocks:
    async def test_the_lock_is_taken_and_released(self, test_engine):
        async with advisory_lock(const.ADVISORY_LOCK_ROLLUPS, engine=test_engine) as acquired:
            assert acquired is True
        # And again, which only works if the first one released.
        async with advisory_lock(const.ADVISORY_LOCK_ROLLUPS, engine=test_engine) as acquired:
            assert acquired is True

    async def test_a_second_holder_is_refused_rather_than_queued(self, test_engine):
        """NON-BLOCKING, on purpose. A tick that queued behind the holder would
        pile ticks up and then run them all at once against the same rows; one
        that skips simply tries again in 15 minutes."""
        async with advisory_lock(const.ADVISORY_LOCK_ROLLUPS, engine=test_engine) as first:
            assert first is True
            async with advisory_lock(const.ADVISORY_LOCK_ROLLUPS, engine=test_engine) as second:
                assert second is False

    async def test_the_lock_is_released_even_when_the_body_raises(self, test_engine):
        """Otherwise one exception inside a job leaks the lock for the lifetime
        of the connection, and every later run of that job is skipped - with no
        error anywhere, because being skipped is a normal outcome."""
        with pytest.raises(RuntimeError):
            async with advisory_lock(const.ADVISORY_LOCK_RETENTION, engine=test_engine):
                raise RuntimeError("boom")

        async with advisory_lock(const.ADVISORY_LOCK_RETENTION, engine=test_engine) as again:
            assert again is True

    async def test_the_four_jobs_do_not_exclude_each_other(self, test_engine):
        """plan 6.3: four distinct keys "so one long job does not exclude
        another". Asserted by holding all four at once."""
        async with (
            advisory_lock(const.ADVISORY_LOCK_ROLLUPS, engine=test_engine) as a,
            advisory_lock(const.ADVISORY_LOCK_PARTITIONS, engine=test_engine) as b,
            advisory_lock(const.ADVISORY_LOCK_RETENTION, engine=test_engine) as c,
            advisory_lock(const.ADVISORY_LOCK_FORECAST, engine=test_engine) as d,
            advisory_lock(const.ADVISORY_LOCK_OWNERSHIP, engine=test_engine) as e,
        ):
            assert [a, b, c, d, e] == [True] * 5


class TestMaintenanceLeavesATrace:
    """A healthy night must still say it happened.

    Found by reading 40 hours of the dev scheduler's real logs: the rollup tick
    was visible every 15 minutes and the ownership refresh every hour, and the
    nightly maintenance produced NOTHING - because `run_partitions` logs only
    when it creates something and `run_retention` only when it drops something,
    and on a healthy night neither does.

    `docs/runbooks/live-data.md` sends an operator to these logs to answer
    "has the create-ahead job stopped?", which is the failure that becomes a
    platform-wide outage three months later. The answer has to be readable.

    Both run on the TEST's session, so the partition DDL is rolled back with it.
    On the session-scoped engine it committed, and the partitions outlived the
    test for the rest of the suite.
    """

    async def test_it_logs_even_when_there_is_nothing_to_do(
        self, db_session: AsyncSession, test_engine, caplog
    ):
        sessions = sessionmaker_for(db_session)
        # The night before, so that the run under test is the healthy one. NOW is
        # frozen and schema.sql bootstraps partitions from the REAL clock, so
        # without this "nothing to do" held only while the calendar was near NOW:
        # from October 2026 the bootstrap no longer reached back to NOW's oldest
        # month, and the run created three partitions.
        await scheduler.run_maintenance(sessions, now=NOW, engine=test_engine)
        caplog.clear()
        with caplog.at_level("INFO", logger="worker.scheduler"):
            result = await scheduler.run_maintenance(sessions, now=NOW, engine=test_engine)

        assert result.partitions_created == []
        assert result.partitions_dropped == []
        assert any(
            "maintenance:" in record.message for record in caplog.records
        ), "a healthy maintenance run must still leave a line - its ABSENCE is the signal"

    async def test_the_line_carries_the_counts(self, db_session: AsyncSession, test_engine, caplog):
        """Not just "ran": an operator reading it wants to know whether the
        create-ahead job is still producing anything."""
        sessions = sessionmaker_for(db_session)
        with caplog.at_level("INFO", logger="worker.scheduler"):
            await scheduler.run_maintenance(sessions, now=NOW, engine=test_engine)
        line = next(r.getMessage() for r in caplog.records if "maintenance:" in r.message)
        assert "created" in line
        assert "dropped" in line


class TestBackfill:
    async def test_history_with_no_rollup_is_marked(self, db_session: AsyncSession):
        """The bootstrap, and it is not optional.

        Every deployment after the first lands on a database that already holds
        telemetry. Without this the service publishes the last 48 hours and
        nothing else, and the gap never fills, because closed buckets are not
        revisited.
        """
        community = await create_community(db_session)
        ean = await create_owned_meter(db_session, id_community=community.id, id_member=1)
        device_id = await create_device(db_session, id_community=community.id, ean=ean)
        old = datetime.datetime(2026, 8, 1, 9, tzinfo=datetime.UTC)
        await create_hour_of_measurements(
            db_session, id_device=device_id, id_community=community.id, bucket=old
        )
        await db_session.execute(text("DELETE FROM rollup_dirty"))

        marked = await scheduler.backfill_dirty(db_session, now=NOW)
        assert marked == 1

        found = await db_session.scalar(
            text("SELECT count(*) FROM rollup_dirty WHERE id_community = :c AND bucket = :b"),
            {"c": community.id, "b": old},
        )
        assert found == 1

    async def test_a_bucket_that_already_has_a_rollup_is_not_marked(self, db_session: AsyncSession):
        """Self-limiting: after the first successful pass it matches nothing, so
        running it at every startup costs one anti-join and no work."""
        community = await create_community(db_session)
        ean = await create_owned_meter(db_session, id_community=community.id, id_member=1)
        device_id = await create_device(db_session, id_community=community.id, ean=ean)
        old = datetime.datetime(2026, 8, 1, 9, tzinfo=datetime.UTC)
        await create_hour_of_measurements(
            db_session, id_device=device_id, id_community=community.id, bucket=old
        )
        # The REAL tick, not a hand-inserted community row: "rolled up" now means
        # the community hour AND its operation rows (migration 0003), and only
        # the tick writes both. The ingest upsert marked `old` dirty; this claims it.
        await rollups.tick_community(db_session, id_community=community.id, now=NOW)
        await db_session.execute(text("DELETE FROM rollup_dirty"))

        assert await scheduler.backfill_dirty(db_session, now=NOW) == 0

    async def test_a_community_hour_with_no_operation_row_is_marked_once(
        self, db_session: AsyncSession
    ):
        """The operation rollups' bootstrap (D-14): history rolled up before
        migration 0003 has community hours and no operation rows. It is marked -
        then, once ticked, never again: the remainder row 0 guarantees every
        recomputed community hour has at least one operation row."""
        community = await create_community(db_session)
        ean = await create_owned_meter(db_session, id_community=community.id, id_member=1)
        device_id = await create_device(db_session, id_community=community.id, ean=ean)
        old = datetime.datetime(2026, 8, 1, 9, tzinfo=datetime.UTC)
        await create_hour_of_measurements(
            db_session, id_device=device_id, id_community=community.id, bucket=old
        )
        await rollups.tick_community(db_session, id_community=community.id, now=NOW)
        # What a pre-0003 database looks like: the community hour, no operation row.
        await db_session.execute(
            text("DELETE FROM rollup_operation_hour WHERE id_community = :c"), {"c": community.id}
        )
        await db_session.execute(text("DELETE FROM rollup_dirty"))

        assert await scheduler.backfill_dirty(db_session, now=NOW) == 1

        await rollups.tick_community(db_session, id_community=community.id, now=NOW)
        operation_rows = await db_session.scalar(
            text(
                "SELECT count(*) FROM rollup_operation_hour "
                "WHERE id_community = :c AND bucket = :b"
            ),
            {"c": community.id, "b": old},
        )
        assert operation_rows == 1
        assert await scheduler.backfill_dirty(db_session, now=NOW) == 0

    async def test_buckets_beyond_raw_retention_are_not_marked(self, db_session: AsyncSession):
        """Recomputing a bucket whose raw rows are about to be dropped is work
        thrown away, and on a large database it is the difference between a
        startup that finishes and one that does not."""
        community = await create_community(db_session)
        ean = await create_owned_meter(db_session, id_community=community.id, id_member=1)
        device_id = await create_device(db_session, id_community=community.id, ean=ean)
        ancient = datetime.datetime(2020, 1, 1, 9, tzinfo=datetime.UTC)
        await create_hour_of_measurements(
            db_session, id_device=device_id, id_community=community.id, bucket=ancient
        )
        await db_session.execute(text("DELETE FROM rollup_dirty"))

        assert await scheduler.backfill_dirty(db_session, now=NOW) == 0


def _points(delta: dict, name: str) -> dict[tuple, float]:
    return {attrs: value for (metric, attrs), value in delta.items() if metric == name}


# Outside the 48-hour window at NOW, so only a dirty mark can bring it back in.
_OLD_BUCKET = datetime.datetime(2026, 9, 10, 9, tzinfo=datetime.UTC)

_ROLLUP_TABLES = (
    "rollup_device_hour",
    "rollup_community_hour",
    "rollup_device_day",
    "rollup_community_day",
)


class TestInactiveCommunities:
    """D-12: a switched-off community is drained, then skipped - and the skip is
    EXACT.

    The scheduler used to take its communities from `SELECT DISTINCT
    id_community FROM device`, so a community switched off on the Annex services
    page was rolled up for ever. `run_rollups` now takes the active set. These pin
    what it does with it, and what the jobs that must NOT take one still do.
    """

    @staticmethod
    async def _community_with_device(session: AsyncSession) -> tuple[int, int]:
        community = await create_community(session)
        ean = await create_owned_meter(session, id_community=community.id, id_member=1)
        device_id = await create_device(session, id_community=community.id, ean=ean)
        return community.id, device_id

    @staticmethod
    def _spy_on_ticks(monkeypatch) -> list[int]:
        ticked: list[int] = []
        original = rollups.tick_community

        async def spy(session, *, id_community, now):
            ticked.append(id_community)
            return await original(session, id_community=id_community, now=now)

        monkeypatch.setattr(rollups, "tick_community", spy)
        return ticked

    @staticmethod
    async def _snapshot(session: AsyncSession, id_community: int) -> dict[str, list]:
        snapshot = {}
        for table in _ROLLUP_TABLES:
            rows = await session.execute(
                text(f"SELECT * FROM {table} WHERE id_community = :c"),  # noqa: S608 - constant
                {"c": id_community},
            )
            snapshot[table] = sorted((tuple(row) for row in rows), key=repr)
        return snapshot

    def test_rollup_targets(self):
        with_devices = [1, 2, 3, 4]
        # Unknown set: everyone, nothing skipped. Rollups fail OPEN.
        assert scheduler.rollup_targets(with_devices, active=None, pending=set()) == (
            [1, 2, 3, 4],
            0,
        )
        assert scheduler.rollup_targets(with_devices, active=frozenset({2}), pending=set()) == (
            [2],
            3,
        )
        # Switched off, but with work pending: drained.
        assert scheduler.rollup_targets(with_devices, active=frozenset({2}), pending={3}) == (
            [2, 3],
            2,
        )
        assert scheduler.rollup_targets(with_devices, active=frozenset(), pending=set()) == (
            [],
            4,
        )
        # Neither the set nor the pending work invents a community with no device.
        assert scheduler.rollup_targets([1], active=frozenset({9}), pending={8}) == ([], 1)

    async def test_an_inactive_community_with_nothing_pending_is_skipped(
        self, db_session, test_engine, metric_delta, monkeypatch, caplog
    ):
        active_id, _ = await self._community_with_device(db_session)
        await self._community_with_device(db_session)  # switched off, nothing pending
        ticked = self._spy_on_ticks(monkeypatch)

        with caplog.at_level("INFO", logger="worker.scheduler"):
            done = await scheduler.run_rollups(
                sessionmaker_for(db_session),
                now=NOW,
                active=frozenset({active_id}),
                engine=test_engine,
            )

        assert done == 1
        assert ticked == [active_id]
        assert _points(metric_delta(), "rollup.communities.total") == {(("outcome", "ok"),): 1}
        assert any(
            "1 community/communities skipped" in record.getMessage() for record in caplog.records
        ), "a skip must leave a line, or a switched-off community looks like a stalled one"

    async def test_an_inactive_community_drains_its_dirty_buckets_then_stops(
        self, db_session, test_engine, monkeypatch
    ):
        """What the worker accepted BEFORE the switch-off still reaches the
        rollups. The ingest upsert marks `rollup_dirty` in the same statement, so
        an hour stored just before the flip is pending work."""
        switched_off, device_id = await self._community_with_device(db_session)
        await create_hour_of_measurements(
            db_session, id_device=device_id, id_community=switched_off, bucket=_OLD_BUCKET
        )
        ticked = self._spy_on_ticks(monkeypatch)
        sessions = sessionmaker_for(db_session)

        first = await scheduler.run_rollups(
            sessions, now=NOW, active=frozenset(), engine=test_engine
        )
        assert first == 1
        assert ticked == [switched_off]
        rolled = await db_session.scalar(
            text(
                "SELECT count(*) FROM rollup_community_hour WHERE id_community = :c AND bucket = :b"
            ),
            {"c": switched_off, "b": _OLD_BUCKET},
        )
        assert rolled == 1, "the drain must actually recompute the dirty bucket"

        second = await scheduler.run_rollups(
            sessions, now=NOW, active=frozenset(), engine=test_engine
        )
        assert second == 0, "drained: nothing left, so the community is skipped"
        assert ticked == [switched_off]

    async def test_an_inactive_community_is_ticked_while_its_last_reading_is_in_the_window(
        self, db_session, test_engine, monkeypatch
    ):
        """`ts > lo`, strictly: `ts` is an interval END, so a reading stamped
        exactly `lo` belongs to the bucket BEFORE the window."""
        lo, _hi = buckets.window(NOW)
        inside, inside_device = await self._community_with_device(db_session)
        outside, outside_device = await self._community_with_device(db_session)
        for id_community, id_device, ts in (
            (inside, inside_device, lo + datetime.timedelta(seconds=1)),
            (outside, outside_device, lo),
        ):
            await db_session.execute(
                text(
                    "INSERT INTO device_last (id_device, id_community, ts, last_seen_at) "
                    "VALUES (:d, :c, :ts, :ts)"
                ),
                {"d": id_device, "c": id_community, "ts": ts},
            )
        ticked = self._spy_on_ticks(monkeypatch)

        done = await scheduler.run_rollups(
            sessionmaker_for(db_session), now=NOW, active=frozenset(), engine=test_engine
        )

        assert done == 1
        assert ticked == [inside]

    async def test_skipping_is_indistinguishable_from_ticking(self, db_session: AsyncSession):
        """THE CLAIM THE SKIP RESTS ON. A switched-off community with nothing
        dirty and no reading in the window is recomputed to exactly what it
        already holds - every row of all four rollup tables, `computed_at`
        included - so skipping it loses nothing."""
        switched_off, device_id = await self._community_with_device(db_session)
        await create_hour_of_measurements(
            db_session, id_device=device_id, id_community=switched_off, bucket=_OLD_BUCKET
        )
        await db_session.execute(
            text(
                "INSERT INTO device_last (id_device, id_community, ts, last_seen_at) "
                "VALUES (:d, :c, :ts, :ts)"
            ),
            {"d": device_id, "c": switched_off, "ts": _OLD_BUCKET + datetime.timedelta(hours=1)},
        )
        # The drain.
        await rollups.tick_community(db_session, id_community=switched_off, now=NOW)

        lo, _hi = buckets.window(NOW)
        pending = set((await db_session.execute(scheduler._PENDING_SQL, {"lo": lo})).scalars())
        assert switched_off not in pending, "the fixture must be a community the tick SKIPS"
        before = await self._snapshot(db_session, switched_off)
        assert before["rollup_community_hour"], "nothing rolled up - the comparison is vacuous"
        assert before["rollup_community_day"], "nothing rolled up - the comparison is vacuous"

        # What skipping it saves.
        await rollups.tick_community(db_session, id_community=switched_off, now=NOW)

        assert await self._snapshot(db_session, switched_off) == before

    async def test_an_unknown_subscription_set_ticks_everyone(
        self, db_session, test_engine, monkeypatch
    ):
        """None - the CRM has never answered - fails OPEN for the rollups:
        recomputing stored data is never wrong, and failing closed would freeze
        every community through a CRM outage."""
        ids = [(await self._community_with_device(db_session))[0] for _ in range(2)]
        ticked = self._spy_on_ticks(monkeypatch)

        done = await scheduler.run_rollups(
            sessionmaker_for(db_session), now=NOW, active=None, engine=test_engine
        )

        assert done == 2
        assert sorted(ticked) == sorted(ids)

    async def test_ownership_refreshes_only_active_communities(self, db_session, test_engine):
        active_id, _ = await self._community_with_device(db_session)
        switched_off, _ = await self._community_with_device(db_session)
        sessions = sessionmaker_for(db_session)

        written = await scheduler.run_ownership(
            sessions, sessions, now=NOW, active=frozenset({active_id}), engine=test_engine
        )

        assert written == 1
        counts = {
            id_community: await db_session.scalar(
                text("SELECT count(*) FROM device_owner_window WHERE id_community = :c"),
                {"c": id_community},
            )
            for id_community in (active_id, switched_off)
        }
        assert counts == {active_id: 1, switched_off: 0}

    async def test_ownership_refuses_to_run_without_a_subscription_set(
        self, test_engine, metric_delta
    ):
        """Unlike the rollups it does NOT fail open, and it refuses before the
        lock or any session is touched - counted `failed`, retried next tick."""

        def untouchable():  # pragma: no cover - the assertion is that this never runs
            raise AssertionError("an unknown set must not reach the database")

        with pytest.raises(SubscriptionsUnavailable):
            await scheduler.run_ownership(
                untouchable, untouchable, now=NOW, active=None, engine=test_engine
            )

        assert _points(metric_delta(), "scheduler.job.runs.total") == {
            (("job", "ownership"), ("outcome", "failed")): 1
        }

    async def test_the_lag_gauge_ignores_a_deactivated_community(self, db_session: AsyncSession):
        """A switched-off community's newest bucket freezes by design. Left in
        the fleet's worst-lag it would pin `scope="data"` upward for ever and
        mask a real stall in an active one."""
        now = datetime.datetime.now(datetime.UTC)
        recent = now - datetime.timedelta(minutes=30)
        old = now - datetime.timedelta(days=2)
        await db_session.execute(
            text(
                "INSERT INTO rollup_community_hour "
                "(id_community, bucket, import_wh, export_wh, n_devices, "
                " n_devices_production, n_members, n_devices_unattributed, computed_at) "
                "VALUES (9101, :recent, 0, 0, 1, 1, 1, 0, :recent), "
                "       (9102, :old,    0, 0, 1, 1, 1, 0, :old)"
            ),
            {"recent": recent, "old": old},
        )
        sessions = sessionmaker_for(db_session)

        app_metrics.rollup_newest_bucket_epoch.clear()
        try:
            await scheduler._refresh_rollup_lag(sessions, only=frozenset({9101}))
            filtered = app_metrics.rollup_newest_bucket_epoch["data"]
            assert filtered == pytest.approx(recent.timestamp(), abs=1)

            # The control: unfiltered, the frozen community is the worst.
            await scheduler._refresh_rollup_lag(sessions, only=None)
            assert app_metrics.rollup_newest_bucket_epoch["data"] == pytest.approx(
                old.timestamp(), abs=1
            )
        finally:
            app_metrics.rollup_newest_bucket_epoch.clear()

    async def test_no_active_community_clears_the_lag_gauge(self):
        """Nothing is active, so nothing's freshness can be judged. A value left
        behind would climb as though the scheduler had died - and the database is
        not even asked."""

        def untouchable():  # pragma: no cover - the assertion is that this never runs
            raise AssertionError("an empty set must not query")

        app_metrics.rollup_newest_bucket_epoch["data"] = time.time() - 600
        app_metrics.rollup_newest_bucket_epoch["tick"] = time.time() - 60
        try:
            await scheduler._refresh_rollup_lag(untouchable, only=frozenset())
            assert app_metrics.rollup_newest_bucket_epoch == {}
        finally:
            app_metrics.rollup_newest_bucket_epoch.clear()

    def test_partitions_and_retention_take_no_subscription_set(self):
        """Table-wide by design, and retention in particular is a GDPR cap that
        must keep reaching a switched-off community's history. And the two jobs
        that do take the set take it with no default: None is a decision."""
        for job in (
            scheduler.run_partitions,
            scheduler.run_retention,
            scheduler.run_maintenance,
            scheduler.backfill_dirty,
        ):
            assert "active" not in inspect.signature(job).parameters, job.__name__
        for job in (scheduler.run_rollups, scheduler.run_ownership):
            parameter = inspect.signature(job).parameters["active"]
            assert parameter.kind is inspect.Parameter.KEYWORD_ONLY, job.__name__
            assert parameter.default is inspect.Parameter.empty, job.__name__

    async def test_a_cold_crm_gives_the_scheduler_no_set_rather_than_no_tick(self, caplog):
        """The scheduler does not wait for the CRM the way the ingest worker
        does: a tick missed for want of a set would freeze every community."""
        from worker import scheduler_main

        async def load() -> frozenset[int]:
            raise ConnectionRefusedError("crm down")

        with caplog.at_level("WARNING", logger="worker.scheduler_main"):
            active = await scheduler_main._active_communities(
                SubscriptionCache(load, ttl_seconds=60)
            )

        assert active is None
        assert any(
            getattr(record, "operation", None) == "scheduler:subscriptions-unavailable"
            for record in caplog.records
        )
