"""The bucket algebra, pinned.

Every assertion here corresponds to a way the rollups can be silently wrong.
None of them needs a database, which is the point: the arithmetic is settled
before any SQL depends on it.
"""

import datetime
from zoneinfo import ZoneInfo

import pytest

from domain.buckets import (
    bucket_of,
    day_targets,
    hour_floor,
    is_day_closed,
    local_day_end,
    local_day_start,
    window,
)
from shared.const import ROLLUP_WINDOW_HOURS

UTC = datetime.UTC
BRUSSELS = ZoneInfo("Europe/Brussels")


def at(year, month, day, hour=0, minute=0, second=0):
    return datetime.datetime(year, month, day, hour, minute, second, tzinfo=UTC)


class TestBucketOf:
    """`ts` is the END of the interval. This class is the whole module."""

    def test_a_reading_stamped_on_the_hour_belongs_to_the_previous_hour(self):
        """THE test. A measurement covering 10:45-11:00 carries 11:00:00Z.

        `date_trunc('hour', ts)` - the expression everyone writes first - puts it
        in hour 11 and moves a quarter of every hour's energy forward, for ever,
        with nothing anywhere reporting it.
        """
        assert bucket_of(at(2026, 9, 16, 11, 0)) == at(2026, 9, 16, 10, 0)

    def test_a_reading_inside_the_hour_belongs_to_that_hour(self):
        assert bucket_of(at(2026, 9, 16, 11, 15)) == at(2026, 9, 16, 11, 0)
        assert bucket_of(at(2026, 9, 16, 11, 45)) == at(2026, 9, 16, 11, 0)

    def test_the_four_quarters_of_an_hour_share_one_bucket(self):
        """11:15, 11:30, 11:45 and 12:00 are the four readings of hour 11."""
        quarters = [
            at(2026, 9, 16, 11, 15),
            at(2026, 9, 16, 11, 30),
            at(2026, 9, 16, 11, 45),
            at(2026, 9, 16, 12, 0),
        ]
        assert {bucket_of(q) for q in quarters} == {at(2026, 9, 16, 11, 0)}

    def test_the_naive_expression_would_disagree(self):
        """The negative control, so the test above is about the rule and not luck.

        `date_trunc('hour', ts)` is `hour_floor(ts)`. If the two ever agree on an
        on-the-hour reading, `bucket_of` has lost its `- 1 second`.
        """
        on_the_hour = at(2026, 9, 16, 11, 0)
        assert bucket_of(on_the_hour) != hour_floor(on_the_hour)

    def test_a_naive_datetime_is_refused(self):
        with pytest.raises(ValueError, match="timezone-aware"):
            bucket_of(datetime.datetime(2026, 9, 16, 11, 0))

    def test_the_answer_does_not_depend_on_how_the_instant_is_expressed(self):
        """The same instant in Brussels must bucket identically."""
        utc = at(2026, 9, 16, 11, 0)
        assert bucket_of(utc.astimezone(BRUSSELS)) == bucket_of(utc)


class TestWindow:
    def test_both_ends_are_hour_aligned(self):
        lo, hi = window(at(2026, 9, 16, 10, 37, 42))
        for end in (lo, hi):
            assert end.minute == 0
            assert end.second == 0
            assert end.microsecond == 0

    def test_it_spans_exactly_the_configured_hours(self):
        lo, hi = window(at(2026, 9, 16, 10, 37, 42))
        assert (hi - lo) == datetime.timedelta(hours=ROLLUP_WINDOW_HOURS)

    def test_the_hour_in_progress_is_inside_the_window(self):
        """`hi` is the END of the current hour, so the partial hour is rewritten
        every tick until it closes rather than withheld for up to an hour."""
        now = at(2026, 9, 16, 10, 37, 42)
        _, hi = window(now)
        assert hi == at(2026, 9, 16, 11, 0)
        assert bucket_of(now + datetime.timedelta(seconds=1)) < hi

    def test_the_window_does_not_erode_as_the_clock_moves_within_an_hour(self):
        """The erosion failure, stated as a property.

        Every instant inside one hour must produce the SAME window. An unaligned
        `now - 48h` gives a different `lo` every minute, and each hour is
        recomputed from a shrinking fragment on its way out.
        """
        windows = {window(at(2026, 9, 16, 10, minute)) for minute in (0, 17, 37, 59)}
        assert len(windows) == 1


class TestLocalDay:
    """Two days a year are not 24 hours long, and neither raises."""

    def test_an_ordinary_day_is_twenty_four_hours(self):
        start = local_day_start(at(2026, 9, 16, 12, 0))
        assert (local_day_end(start) - start) == datetime.timedelta(hours=24)

    def test_the_october_day_is_twenty_five_hours(self):
        """Last Sunday in October 2026: clocks go back."""
        start = local_day_start(at(2026, 10, 25, 12, 0))
        assert (local_day_end(start) - start) == datetime.timedelta(hours=25)

    def test_the_march_day_is_twenty_three_hours(self):
        """Last Sunday in March 2026: clocks go forward."""
        start = local_day_start(at(2026, 3, 29, 12, 0))
        assert (local_day_end(start) - start) == datetime.timedelta(hours=23)

    def test_adding_twenty_four_hours_would_be_wrong_on_those_days(self):
        """The negative control for the two tests above."""
        for month, day in ((10, 25), (3, 29)):
            start = local_day_start(at(2026, month, day, 12, 0))
            assert local_day_end(start) != start + datetime.timedelta(hours=24)

    def test_every_hour_of_a_dst_day_maps_to_exactly_one_day(self):
        """No hour may fall into two days, and none into zero."""
        for month, day, expected_hours in ((10, 25, 25), (3, 29, 23)):
            start = local_day_start(at(2026, month, day, 12, 0))
            end = local_day_end(start)
            hours = []
            cursor = start
            while cursor < end:
                hours.append(cursor)
                cursor += datetime.timedelta(hours=1)
            assert len(hours) == expected_hours
            assert {local_day_start(h) for h in hours} == {start}

    def test_a_day_is_closed_only_once_its_last_hour_has_completed(self):
        start = local_day_start(at(2026, 9, 16, 12, 0))
        end = local_day_end(start)
        assert not is_day_closed(start, end - datetime.timedelta(minutes=30))
        assert is_day_closed(start, end)

    def test_closure_is_gated_on_the_hour_not_on_the_instant(self):
        """A day must never be derived from an hour still in progress."""
        start = local_day_start(at(2026, 9, 16, 12, 0))
        end = local_day_end(start)
        assert not is_day_closed(start, end - datetime.timedelta(seconds=1))


class TestDayTargets:
    def test_an_open_day_is_never_a_target(self):
        """The partial-day guard. One dirty bucket inside today must not produce
        a 'day' made of the hours elapsed so far - it would freeze there."""
        now = at(2026, 9, 16, 10, 0)
        today = [at(2026, 9, 16, 3, 0)]
        assert day_targets(today, now) == []

    def test_a_closed_day_is_a_target_exactly_once(self):
        now = at(2026, 9, 16, 10, 0)
        buckets = [at(2026, 9, 14, h, 0) for h in (2, 7, 19)]
        assert day_targets(buckets, now) == [local_day_start(buckets[0])]

    def test_targets_are_sorted_and_deduplicated(self):
        now = at(2026, 9, 16, 10, 0)
        buckets = [at(2026, 9, 14, 5, 0), at(2026, 9, 12, 5, 0), at(2026, 9, 14, 6, 0)]
        targets = day_targets(buckets, now)
        assert targets == sorted(targets)
        assert len(targets) == 2
