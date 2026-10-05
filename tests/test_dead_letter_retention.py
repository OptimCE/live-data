"""Dead-letter retention: `ingest_dead_letter` is pruned nightly, in bounded batches.

The table is written for every message-scoped rejection, every unknown device and
every unparseable topic, and nothing ever deleted from it. A connector that keeps
publishing something the worker cannot store - a misconfigured one, or a revoked
one whose broker client outlived its row - adds a row every fifteen minutes for as
long as it has power, so the table grew without bound. `schema.sql` called it
"deliberately small"; nothing made it so.

The nightly retention job now deletes rows older than
`RETENTION_DEAD_LETTER_DAYS`. What can go wrong without anything failing:

  * the WINDOW - an off-by-one at the edge, or a prune that reaches rows through
    `device` and so never touches the rows no device could be found for, which
    are exactly the rows a connector publishing for ever writes;
  * the BATCHES - one DELETE over a backlog of millions is one long transaction,
    holding its row locks and the vacuum horizon for as long as it runs, and a
    run with no cap keeps the scheduler's single loop - and every rollup tick
    queued behind it - waiting until the backlog is gone;
  * the WIRING - a setting read by nothing, a prune that runs without the
    retention advisory lock, or deletions nobody counted.

Every prune here runs through `sessionmaker_for`, so its commits are savepoint
releases inside the test's rolled-back transaction and nothing outlives the test.
"""

import datetime
import uuid

import pytest
from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import Settings, settings
from domain.reasons import RejectReason
from shared.const import ADVISORY_LOCK_RETENTION
from tests.conftest import sessionmaker_for
from worker import retention, scheduler
from worker.context import advisory_lock

NOW = datetime.datetime(2026, 9, 16, 10, 30, tzinfo=datetime.UTC)
DAYS = 90
# The instant a row is exactly DAYS old. Strictly older is pruned; this is kept.
EDGE = NOW - datetime.timedelta(days=DAYS)


async def _dead_letter(
    session: AsyncSession,
    *,
    received_at: datetime.datetime,
    id_device: int | None = 4242,
    reason: RejectReason = RejectReason.SCHEMA_INVALID,
    topic: str = "ce/1/00000000-0000-0000-0000-000000000001/telemetry",
) -> int:
    """One row, shaped like `worker.ingest._dead_letter` writes it, with its
    `received_at` set by hand - the column's DEFAULT is the only thing the
    ingest path leaves to the database."""
    new_id = await session.scalar(
        text(
            "INSERT INTO ingest_dead_letter "
            "(topic, id_device, reason, detail, payload, received_at) "
            "VALUES (:topic, :id_device, :reason, 'fixture', '{}', :received_at) "
            "RETURNING id"
        ),
        {
            "topic": topic,
            "id_device": id_device,
            "reason": reason.value,
            "received_at": received_at,
        },
    )
    assert new_id is not None
    return int(new_id)


async def _surviving(session: AsyncSession, ids: list[int]) -> set[int]:
    rows = await session.execute(
        text("SELECT id FROM ingest_dead_letter WHERE id = ANY(:ids)"), {"ids": ids}
    )
    return set(rows.scalars().all())


class _CommitRecorder:
    """The test's session, recording how many of `ids` are left at each COMMIT.

    The size of every committed batch is then OBSERVED, not inferred from a call
    count: a prune that deleted everything in one statement and then looped once
    more to find nothing would commit twice too.
    """

    def __init__(self, session: AsyncSession, ids: list[int]) -> None:
        self.left_at_each_commit: list[int] = []
        recorder = self

        class _Session:
            def __getattr__(self, name: str):
                return getattr(session, name)

            async def commit(self) -> None:
                recorder.left_at_each_commit.append(len(await _surviving(session, ids)))
                await session.commit()

        self.sessions = sessionmaker_for(_Session())


class TestTheWindow:
    def test_the_cutoff_is_the_window_in_days(self):
        """Days, not calendar months like `retention.cutoff`. The partitioned
        tables are cut on a month boundary because a partition IS a month; this
        table is not partitioned, so there is no boundary to align to and the
        window can be exact."""
        assert retention.dead_letter_cutoff(NOW, DAYS) == EDGE

    @pytest.mark.parametrize("days", [0, -1])
    def test_a_window_under_one_day_is_refused(self, days):
        """0 would delete the last 24 hours that `/ops/health` counts as
        `dead_letters_24h`; a negative window puts the cutoff in the future,
        which is every row - including the one the worker wrote a second ago."""
        with pytest.raises(ValueError, match="at least 1 day"):
            retention.dead_letter_cutoff(NOW, days)


class TestThePrune:
    async def test_rows_older_than_the_window_are_deleted_and_newer_ones_kept(
        self, db_session: AsyncSession
    ):
        """Both halves in ONE run, because each is the other's control.

        The deletion of `expired` is the positive control for the survival of
        `recent`: a prune that did nothing at all would keep `recent` too, and
        pass a test that asserted only that. And the rows are counted BEFORE the
        run, so "deleted" cannot be satisfied by a fixture that never wrote them.
        """
        expired = [
            await _dead_letter(db_session, received_at=EDGE - datetime.timedelta(days=1)),
            await _dead_letter(db_session, received_at=EDGE - datetime.timedelta(days=400)),
        ]
        recent = [
            await _dead_letter(db_session, received_at=EDGE + datetime.timedelta(days=1)),
            await _dead_letter(db_session, received_at=NOW - datetime.timedelta(minutes=5)),
        ]
        assert await _surviving(db_session, expired + recent) == set(expired + recent)

        pruned = await retention.prune_dead_letters(
            sessionmaker_for(db_session), now=NOW, days=DAYS
        )

        assert pruned == 2
        assert await _surviving(db_session, expired + recent) == set(recent)

    async def test_a_row_exactly_at_the_edge_is_kept(self, db_session: AsyncSession):
        """Strictly older than the window, never "older or equal". A row exactly
        DAYS old has not outlived it yet, and the second either side pins the
        comparison in both directions."""
        at_edge = await _dead_letter(db_session, received_at=EDGE)
        just_past = await _dead_letter(db_session, received_at=EDGE - datetime.timedelta(seconds=1))

        pruned = await retention.prune_dead_letters(
            sessionmaker_for(db_session), now=NOW, days=DAYS
        )

        assert pruned == 1
        assert await _surviving(db_session, [at_edge, just_past]) == {at_edge}

    async def test_rows_no_device_could_be_found_for_are_pruned_too(self, db_session: AsyncSession):
        """THE ROWS THIS JOB EXISTS FOR.

        An unparseable topic and an unknown device are dead-lettered with
        `id_device` NULL, and the table has no `id_community` at all. Every
        per-community READ of it must go through `device` (CLAUDE.md), so a
        prune written the same way would skip precisely the connector that
        publishes for ever and grows the table - and pass every test that
        seeded its rows with a device id.
        """
        unattributable = [
            await _dead_letter(
                db_session,
                received_at=EDGE - datetime.timedelta(days=3),
                id_device=None,
                reason=RejectReason.SCHEMA_INVALID,
                topic="not/a/device/topic",
            ),
            await _dead_letter(
                db_session,
                received_at=EDGE - datetime.timedelta(days=3),
                id_device=None,
                reason=RejectReason.DEVICE_UNKNOWN,
                topic=f"ce/1/{uuid.uuid4()}/telemetry",
            ),
        ]

        pruned = await retention.prune_dead_letters(
            sessionmaker_for(db_session), now=NOW, days=DAYS
        )

        assert pruned == 2
        assert await _surviving(db_session, unattributable) == set()


class TestTheBatches:
    async def test_each_batch_is_bounded_and_committed_on_its_own(self, db_session: AsyncSession):
        """Five expired rows, batches of two: three commits, leaving 3, 1 and 0.

        One DELETE over the whole backlog would be one transaction for as long
        as it ran - its row locks held throughout, and the database's vacuum
        horizon pinned behind it. A commit per bounded batch is what keeps a
        backlog of millions from being that.
        """
        ids = [
            await _dead_letter(db_session, received_at=EDGE - datetime.timedelta(days=d))
            for d in range(1, 6)
        ]
        recorder = _CommitRecorder(db_session, ids)

        pruned = await retention.prune_dead_letters(
            recorder.sessions, now=NOW, days=DAYS, batch_rows=2
        )

        assert pruned == 5
        assert recorder.left_at_each_commit == [3, 1, 0]

    async def test_a_run_stops_at_its_cap_and_the_next_one_continues(
        self, db_session: AsyncSession, caplog
    ):
        """The run is bounded as well as the batch.

        Maintenance runs inside the scheduler's one loop, so a prune working
        through a backlog for an hour is an hour of rollup ticks not taken. A
        capped run stops, says so, and leaves the rest to the next night - and it
        takes the OLDEST first, so what is left is the stretch just past the edge
        and `min(received_at)` says how far behind the prune is.
        """
        oldest_first = [
            await _dead_letter(db_session, received_at=EDGE - datetime.timedelta(days=d))
            for d in (5, 4, 3, 2, 1)
        ]
        sessions = sessionmaker_for(db_session)

        with caplog.at_level("WARNING", logger="worker.retention"):
            first = await retention.prune_dead_letters(
                sessions, now=NOW, days=DAYS, batch_rows=2, max_batches=2
            )
        assert first == 4
        assert await _surviving(db_session, oldest_first) == {oldest_first[-1]}
        assert any("per-run cap" in r.getMessage() for r in caplog.records), (
            "a capped run that leaves rows behind must say so, or a backlog the "
            "prune cannot keep up with looks exactly like a healthy night"
        )

        caplog.clear()
        with caplog.at_level("WARNING", logger="worker.retention"):
            second = await retention.prune_dead_letters(
                sessions, now=NOW, days=DAYS, batch_rows=2, max_batches=2
            )
        assert second == 1
        assert await _surviving(db_session, oldest_first) == set()
        assert not any("per-run cap" in r.getMessage() for r in caplog.records)

    async def test_a_run_that_ends_exactly_at_its_cap_does_not_report_a_backlog(
        self, db_session: AsyncSession, caplog
    ):
        """Four rows, two full batches, cap two: the cap is reached AND nothing is
        left. The warning is the backlog signal, so it has to be exact - one that
        fired whenever the last batch happened to be full would cry wolf on an
        ordinary night."""
        ids = [
            await _dead_letter(db_session, received_at=EDGE - datetime.timedelta(days=d))
            for d in range(1, 5)
        ]

        with caplog.at_level("WARNING", logger="worker.retention"):
            pruned = await retention.prune_dead_letters(
                sessionmaker_for(db_session), now=NOW, days=DAYS, batch_rows=2, max_batches=2
            )

        assert pruned == 4
        assert await _surviving(db_session, ids) == set()
        assert not any("per-run cap" in r.getMessage() for r in caplog.records)


class TestTheNightlyJob:
    """Through `run_retention` and `run_maintenance`, as `scheduler_main` calls them."""

    async def test_the_job_prunes_with_the_configured_window(
        self, db_session: AsyncSession, test_engine, monkeypatch
    ):
        """The setting reaches the job. A 40-day-old row survives the default
        window and is pruned at 30 days - so a job with the window hard-coded
        fails here, where the name-level check in `test_settings_have_readers`
        would be satisfied by any mention at all."""
        monkeypatch.setattr(settings, "RETENTION_DEAD_LETTER_DAYS", 30)
        forty_days = await _dead_letter(db_session, received_at=NOW - datetime.timedelta(days=40))
        twenty_days = await _dead_letter(db_session, received_at=NOW - datetime.timedelta(days=20))

        result = await scheduler.run_retention(
            sessionmaker_for(db_session), now=NOW, engine=test_engine
        )

        assert result.dead_letters_pruned == 1
        assert result.partitions_dropped == []
        assert await _surviving(db_session, [forty_days, twenty_days]) == {twenty_days}

    async def test_nothing_is_pruned_while_another_replica_holds_the_lock(
        self, db_session: AsyncSession, test_engine
    ):
        """Under the RETENTION advisory lock, like the partition drops. The same
        call once the lock is free is the control: the only thing that differs
        between the two runs is who holds it."""
        expired = await _dead_letter(
            db_session,
            received_at=NOW - datetime.timedelta(days=settings.RETENTION_DEAD_LETTER_DAYS + 1),
        )
        sessions = sessionmaker_for(db_session)

        async with advisory_lock(ADVISORY_LOCK_RETENTION, engine=test_engine) as held:
            assert held, "the fixture could not take the lock it needs to hold"
            skipped = await scheduler.run_retention(sessions, now=NOW, engine=test_engine)
        assert skipped.dead_letters_pruned == 0
        assert await _surviving(db_session, [expired]) == {expired}

        ran = await scheduler.run_retention(sessions, now=NOW, engine=test_engine)
        assert ran.dead_letters_pruned == 1
        assert await _surviving(db_session, [expired]) == set()

    async def test_the_maintenance_line_says_how_many_were_pruned(
        self, db_session: AsyncSession, test_engine, caplog
    ):
        """The nightly line is what `docs/runbooks/live-data.md` sends an operator
        to read, so the prune's count belongs on it rather than on a line of its
        own that a `grep "maintenance:"` would never show."""
        await _dead_letter(
            db_session,
            received_at=NOW - datetime.timedelta(days=settings.RETENTION_DEAD_LETTER_DAYS + 1),
        )

        with caplog.at_level("INFO", logger="worker.scheduler"):
            result = await scheduler.run_maintenance(
                sessionmaker_for(db_session), now=NOW, engine=test_engine
            )

        assert result.dead_letters_pruned == 1
        line = next(r.getMessage() for r in caplog.records if "maintenance:" in r.getMessage())
        assert "1 dead letter(s) pruned" in line, line


class TestTheSetting:
    """Unconditional boot assertions, like the other arithmetic in `core/config.py`:
    they fire under ENV=test too, so the suite can cover them."""

    @pytest.mark.parametrize("days", [0, -1])
    def test_a_window_under_one_day_is_refused_at_boot(self, days):
        with pytest.raises(ValidationError, match="RETENTION_DEAD_LETTER_DAYS .* must be between"):
            Settings(**{**settings.model_dump(), "RETENTION_DEAD_LETTER_DAYS": days})

    def test_a_window_longer_than_raw_retention_is_refused_at_boot(self):
        """A dead letter keeps up to 2 KB of the rejected payload - a household's
        quarter-hourly readings, which is personal data the raw retention caps at
        `RETENTION_RAW_MONTHS`. A rejected reading must not outlive every
        accepted one. 28 is February: the bound has to hold in the shortest
        month."""
        too_long = settings.RETENTION_RAW_MONTHS * 28 + 1
        with pytest.raises(ValidationError, match="RETENTION_DEAD_LETTER_DAYS .* must be between"):
            Settings(**{**settings.model_dump(), "RETENTION_DEAD_LETTER_DAYS": too_long})

    @pytest.mark.parametrize("days", [1, 30, 90, 13 * 28])
    def test_the_bounds_themselves_boot(self, days):
        """The negative control, both edges included. 90 is the default."""
        booted = Settings(**{**settings.model_dump(), "RETENTION_DEAD_LETTER_DAYS": days})
        assert booted.model_dump()["RETENTION_DEAD_LETTER_DAYS"] == days
