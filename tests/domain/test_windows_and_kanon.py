"""Window snapping and the k threshold, without a session.

The API tests in `tests/api/test_read_api.py` exercise these through HTTP. These
pin the two cases HTTP cannot reach cheaply: the DST day boundaries, and the
guard that stops a hostile window from being counted one grid step at a time.
"""

import datetime

import pytest

from domain import kanon
from domain.windows import (
    MAX_POINTS,
    Resolution,
    Window,
    WindowError,
    WindowProblem,
    count_points,
    exceeds_cap,
    is_snapped,
    resolve_window,
    step_after,
)

NOW = datetime.datetime(2026, 9, 16, 10, 37, tzinfo=datetime.UTC)


def utc(value: str) -> datetime.datetime:
    return datetime.datetime.fromisoformat(value).replace(tzinfo=datetime.UTC)


class TestSnapping:
    def test_an_hour_boundary_is_snapped(self):
        assert is_snapped(utc("2026-09-16 10:00"), Resolution.HOUR)

    def test_a_minute_past_the_hour_is_not(self):
        assert not is_snapped(utc("2026-09-16 10:01"), Resolution.HOUR)

    def test_a_quarter_boundary_is_snapped(self):
        assert is_snapped(utc("2026-09-16 10:45"), Resolution.QUARTER)
        assert not is_snapped(utc("2026-09-16 10:46"), Resolution.QUARTER)

    @pytest.mark.parametrize(
        "midnight",
        [
            "2026-06-30 22:00",  # CEST: local midnight is 22:00Z the day before
            "2026-01-31 23:00",  # CET: 23:00Z
            "2026-10-24 22:00",  # the 25-hour day opens here
            "2026-03-28 23:00",  # the 23-hour day opens here
        ],
    )
    def test_local_midnight_is_snapped_in_both_dst_states(self, midnight: str):
        """A day bound is an instant of Europe/Brussels midnight, and those
        instants are 22:00Z or 23:00Z depending on the season. Any rule of the
        form `(ts - epoch) % 86400 == 0` accepts neither."""
        assert is_snapped(utc(midnight), Resolution.DAY)

    def test_utc_midnight_is_not_a_belgian_day_bound(self):
        """NEGATIVE CONTROL. 00:00Z is 01:00 or 02:00 local - the middle of the
        night, not the start of the day. A naive implementation accepts exactly
        this and rejects every real one."""
        assert not is_snapped(utc("2026-06-30 00:00"), Resolution.DAY)

    def test_an_unsnapped_bound_raises_rather_than_being_corrected(self):
        with pytest.raises(WindowError) as caught:
            resolve_window(now=NOW, resolution="hour", start=utc("2026-09-16 09:17"))
        assert caught.value.problem is WindowProblem.NOT_SNAPPED

    def test_both_bounds_omitted_is_always_valid(self):
        """The SPA sends only `resolution`, so the defaults must be snapped by
        construction - a default window that tripped the rule would 422 every
        first page load."""
        window = resolve_window(now=NOW, resolution="day")
        assert is_snapped(window.start, Resolution.DAY)
        assert is_snapped(window.end, Resolution.DAY)


class TestTheDayGrid:
    def test_the_october_day_is_twenty_five_hours(self):
        start = utc("2026-10-24 22:00")
        assert (step_after(start, Resolution.DAY) - start) == datetime.timedelta(hours=25)

    def test_the_march_day_is_twenty_three_hours(self):
        start = utc("2026-03-28 23:00")
        assert (step_after(start, Resolution.DAY) - start) == datetime.timedelta(hours=23)

    def test_a_month_spanning_the_change_counts_its_days_correctly(self):
        """`span / 24 h` is off by one across the October change. Walking the
        grid is not an implementation detail here - it is the only thing that
        gets the count right."""
        window = Window(
            start=utc("2026-10-01 22:00"),
            end=utc("2026-10-31 23:00"),
            resolution=Resolution.DAY,
        )
        assert count_points(window) == 30
        naive = (window.end - window.start).total_seconds() / 86400
        assert naive != 30


class TestTheCap:
    def test_a_window_at_the_cap_is_allowed(self):
        window = resolve_window(
            now=NOW,
            resolution="hour",
            start=utc("2026-01-01 00:00"),
            end=utc("2026-01-01 00:00") + datetime.timedelta(hours=MAX_POINTS[Resolution.HOUR]),
        )
        assert count_points(window) == MAX_POINTS[Resolution.HOUR]

    def test_a_window_one_point_over_is_refused(self):
        with pytest.raises(WindowError) as caught:
            resolve_window(
                now=NOW,
                resolution="hour",
                start=utc("2026-01-01 00:00"),
                end=utc("2026-01-01 00:00")
                + datetime.timedelta(hours=MAX_POINTS[Resolution.HOUR] + 1),
            )
        assert caught.value.problem is WindowProblem.TOO_LARGE

    def test_an_absurd_window_is_refused_without_walking_the_grid(self):
        """A thousand years at quarter-hour resolution is 35 million steps. The
        cheap guard rejects it on arithmetic; without it, this call would be a
        denial of service reachable from a query string.

        Asserted by the clock rather than by mocking, because what is being
        checked IS that it returns promptly.
        """
        window = Window(
            start=utc("2026-01-01 00:00"),
            end=utc("3026-01-01 00:00"),
            resolution=Resolution.QUARTER,
        )
        started = datetime.datetime.now()
        assert exceeds_cap(window) is True
        assert (datetime.datetime.now() - started).total_seconds() < 1.0

    def test_an_unknown_resolution_raises(self):
        with pytest.raises(WindowError) as caught:
            resolve_window(now=NOW, resolution="fortnight")
        assert caught.value.problem is WindowProblem.UNKNOWN_RESOLUTION

    def test_an_inverted_window_raises(self):
        with pytest.raises(WindowError) as caught:
            resolve_window(
                now=NOW,
                resolution="hour",
                start=utc("2026-09-16 10:00"),
                end=utc("2026-09-16 08:00"),
            )
        assert caught.value.problem is WindowProblem.INVERTED


class TestKAnonymity:
    def test_at_or_above_k_the_grid_is_visible(self):
        assert kanon.grid_is_visible(5, 5) is True
        assert kanon.grid_is_visible(6, 5) is True

    def test_below_k_it_is_not(self):
        assert kanon.grid_is_visible(4, 5) is False

    def test_none_suppresses(self):
        """FAIL CLOSED. None means the projection has not run for that bucket,
        not that nobody was there - and publishing an unprotected series on the
        strength of a missing value is the default that survives to production."""
        assert kanon.grid_is_visible(None, 5) is False

    def test_zero_and_negative_suppress(self):
        assert kanon.grid_is_visible(0, 5) is False
        assert kanon.grid_is_visible(-1, 5) is False

    def test_the_floor_applies_even_to_a_lower_configured_k(self):
        """Mirrors `ck_community_live_settings_k_floor`. Defence in depth: the
        constraint is the real guard, this is the one that still holds if a
        migration ever drops it."""
        assert kanon.effective_k(1) == kanon.K_FLOOR
        assert kanon.grid_is_visible(2, 1) is False

    def test_a_missing_k_falls_back_to_the_floor_not_to_zero(self):
        """A `None` k that became 0 would make `n_members >= 0` true for every
        bucket - the whole threshold off, silently, for any community whose
        settings row was missing a value."""
        assert kanon.effective_k(None) == kanon.K_FLOOR
        assert kanon.grid_is_visible(1, None) is False

    def test_the_absent_terms_name_the_grid_and_not_production(self):
        """The decision of 2026-09-16, stated where it is enforced."""
        terms = {item.term for item in kanon.ABSENT_GRID}
        assert terms == {"import_wh", "export_wh"}
        assert "production_wh" not in terms


def _op(id_op: int, n_min: int | None, n_max: int | None = None) -> kanon.ScopeMembers:
    return kanon.ScopeMembers(
        id_sharing_operation=id_op,
        n_members_min=n_min,
        n_members_max=n_min if n_max is None else n_max,
    )


REMAINDER = kanon.REMAINDER


class TestOperationsAndTheTotal:
    """D-14: per-operation k, and the community total withheld whenever
    subtracting the visible operations would isolate fewer than k members."""

    def test_an_operation_is_judged_on_its_own_members(self):
        assert kanon.operation_grid_is_visible(_op(7, 3), 3) is True
        assert kanon.operation_grid_is_visible(_op(7, 2), 3) is False

    def test_a_day_is_judged_on_its_least_populated_hour(self):
        """Judged on the MAX, "day minus its published hours" is the withheld
        ones - the leak the community view itself had before migration 0003."""
        assert kanon.operation_grid_is_visible(_op(7, 2, 9), 3) is False

    def test_the_remainder_is_never_published_as_an_operation(self):
        assert kanon.operation_grid_is_visible(_op(REMAINDER, 50), 3) is False

    def test_the_total_of_visible_operations_alone_is_shown(self):
        """Nothing outside them: the total is their sum, and tells nothing new."""
        assert kanon.community_grid_is_visible(9, [_op(1, 4), _op(2, 5)], 3) is True

    def test_one_household_outside_every_operation_hides_the_total(self):
        """Total minus the visible operation would be that one household."""
        assert kanon.community_grid_is_visible(6, [_op(1, 5), _op(REMAINDER, 1)], 3) is False

    def test_an_operation_below_k_hides_the_total(self):
        assert kanon.community_grid_is_visible(7, [_op(1, 5), _op(2, 2)], 3) is False

    def test_a_residual_of_k_members_keeps_the_total(self):
        assert kanon.community_grid_is_visible(8, [_op(1, 5), _op(REMAINDER, 3)], 3) is True

    def test_the_day_device_count_counter_example_is_withheld(self):
        """The case a device-count test misses. Hour 1: three members in O1 and
        one household in no operation; hour 2: four members in O1. Every day MAX
        reads 4, so device counts say "nothing outside O1" - yet total minus O1
        is that one household's hour-1 energy. The remainder row sees it."""
        day = [_op(1, 3, 4), _op(REMAINDER, 1, 1)]
        assert kanon.community_grid_is_visible(4, day, 3) is False

    def test_the_community_day_is_judged_on_its_least_populated_hour(self):
        assert kanon.community_grid_is_visible(2, [_op(1, 9)], 3) is False

    def test_a_bucket_rolled_up_before_0003_is_judged_on_the_community_alone(self):
        """No operation row means no operation figure was published either, so
        there is nothing to subtract."""
        assert kanon.community_grid_is_visible(5, [], 3) is True

    def test_a_missing_minimum_suppresses(self):
        """A community day computed before migration 0003 has no n_members_min.
        Fail closed until the backfill recomputes it."""
        assert kanon.community_grid_is_visible(None, [_op(1, 9)], 3) is False

    def test_the_shared_term_is_withheld_with_the_grid(self):
        assert kanon.ABSENT_SHARED.term == "shared_wh"
        assert kanon.ABSENT_SHARED.reason is kanon.AbsentReason.BELOW_K_THRESHOLD

    def test_the_remainder_id_agrees_with_the_service_constant(self):
        from shared.const import NO_SHARING_OPERATION

        assert kanon.REMAINDER == NO_SHARING_OPERATION
