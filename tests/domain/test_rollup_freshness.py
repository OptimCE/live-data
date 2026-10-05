"""Is the SCHEDULER keeping up? Pure: every input is a parameter, `now` included.

The question the newest data bucket cannot answer. A fleet that goes quiet stops
producing buckets while the scheduler is perfectly healthy, and reading that age
as rollup staleness is how the Ops tab came to say "not recomputed for 90 minutes"
over a quiet night and blame the scheduler for it.

Three watermarks, because each has a blind spot the others cover:

  * the newest bucket          - how new the DATA is;
  * the newest `computed_at`   - when a tick last rewrote the 48-hour window;
  * the oldest pending mark    - how long a stored reading has waited for a tick.
"""

import datetime

from domain import buckets
from domain.rollup_freshness import RollupFreshness, classify

NOW = datetime.datetime(2026, 9, 29, 10, 37, tzinfo=datetime.UTC)
TICK_MINUTES = 15
LIMIT = datetime.timedelta(minutes=3 * TICK_MINUTES)
LO, _HI = buckets.window(NOW)


def _verdict(
    *,
    newest_bucket: datetime.datetime | None = None,
    computed_at: datetime.datetime | None = None,
    pending_since: datetime.datetime | None = None,
    tick_minutes: int = TICK_MINUTES,
):
    return classify(
        now=NOW,
        newest_bucket=newest_bucket,
        computed_at=computed_at,
        pending_since=pending_since,
        tick_minutes=tick_minutes,
    )


def _hours(n: float) -> datetime.timedelta:
    return datetime.timedelta(hours=n)


def _minutes(n: float) -> datetime.timedelta:
    return datetime.timedelta(minutes=n)


class TestNever:
    def test_nothing_rolled_up_and_nothing_waiting(self):
        verdict = _verdict()
        assert verdict.state is RollupFreshness.NEVER
        assert verdict.lag is None

    def test_a_first_reading_waiting_for_its_first_tick_is_not_yet_a_fault(self):
        """A community enrolled five minutes ago has readings and no rollup. That
        is the next tick's job, not a stalled scheduler."""
        verdict = _verdict(pending_since=NOW - _minutes(5))
        assert verdict.state is RollupFreshness.NEVER


class TestFresh:
    def test_a_quiet_fleet_is_not_a_stalled_scheduler(self):
        """THE BUG. The newest bucket is five hours old because nobody produced
        anything - and the tick rewrote the window four minutes ago."""
        verdict = _verdict(
            newest_bucket=buckets.hour_floor(NOW) - _hours(5),
            computed_at=NOW - _minutes(4),
        )
        assert verdict.state is RollupFreshness.FRESH
        assert verdict.lag == _minutes(4)

    def test_the_limit_itself_is_still_fresh(self):
        verdict = _verdict(newest_bucket=LO, computed_at=NOW - LIMIT)
        assert verdict.state is RollupFreshness.FRESH

    def test_clock_skew_never_produces_a_negative_age(self):
        """`computed_at` is the scheduler's clock and `now` is the API's."""
        verdict = _verdict(newest_bucket=LO, computed_at=NOW + datetime.timedelta(seconds=30))
        assert verdict.state is RollupFreshness.FRESH
        assert verdict.lag == datetime.timedelta(0)


class TestStale:
    def test_no_recompute_for_three_ticks_while_data_is_in_the_window(self):
        verdict = _verdict(
            newest_bucket=buckets.hour_floor(NOW) - _hours(3),
            computed_at=NOW - _hours(2),
        )
        assert verdict.state is RollupFreshness.STALE
        assert verdict.lag == _hours(2)

    def test_readings_waiting_two_hours_before_any_rollup_exists(self):
        """A scheduler dead since the community's first reading. There is no
        rollup row at all, so without the pending mark this reads NEVER for
        ever - a verdict that is never red."""
        verdict = _verdict(pending_since=NOW - _hours(2))
        assert verdict.state is RollupFreshness.STALE
        assert verdict.lag == _hours(2)

    def test_readings_resuming_after_a_long_silence_but_never_processed(self):
        """The newest bucket fell out of the window during a silence longer than
        48 hours, and the scheduler died meanwhile. New readings are waiting; the
        bucket watermark alone would call this IDLE."""
        verdict = _verdict(
            newest_bucket=LO - _hours(5),
            computed_at=NOW - _hours(60),
            pending_since=NOW - _hours(2),
        )
        assert verdict.state is RollupFreshness.STALE

    def test_the_lag_is_the_larger_of_the_two_ages(self):
        verdict = _verdict(
            newest_bucket=LO,
            computed_at=NOW - _hours(2),
            pending_since=NOW - _minutes(50),
        )
        assert verdict.state is RollupFreshness.STALE
        assert verdict.lag == _hours(2)

    def test_the_limit_follows_the_tick_cadence(self):
        twenty_minutes_ago = {"newest_bucket": LO, "computed_at": NOW - _minutes(20)}
        assert _verdict(**twenty_minutes_ago).state is RollupFreshness.FRESH
        assert _verdict(**twenty_minutes_ago, tick_minutes=5).state is RollupFreshness.STALE


class TestIdle:
    def test_nothing_left_inside_the_window(self):
        """No bucket inside the window means the tick has nothing to rewrite, so
        `computed_at` cannot move - and its age then says nothing at all about
        the scheduler. Never red."""
        verdict = _verdict(newest_bucket=LO - _hours(1), computed_at=NOW - _hours(3 * 24))
        assert verdict.state is RollupFreshness.IDLE
        assert verdict.lag is None

    def test_the_oldest_bucket_of_the_window_is_still_inside_it(self):
        verdict = _verdict(newest_bucket=LO, computed_at=NOW - _hours(3))
        assert verdict.state is RollupFreshness.STALE
