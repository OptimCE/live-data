"""The ownership algebra, pinned.

Every assertion here maps to a way a kilowatt-hour gets attributed to the wrong
household. That is the failure mode: nothing raises, the totals stay plausible,
and the only symptom is that someone else's consumption appears under your name.
"""

import datetime

import pytest

from domain.ownership import (
    OwnershipWindow,
    covers,
    local_date_of,
    mark_ambiguous,
    members_at,
    overlaps,
)


def window(
    *,
    ean: str = "541448000000000001",
    member: int | None = 1,
    start: str = "2026-01-01",
    end: str | None = None,
    ambiguous: bool = False,
) -> OwnershipWindow:
    return OwnershipWindow(
        ean=ean,
        id_community=1,
        id_member=member,
        valid_from=datetime.date.fromisoformat(start),
        valid_to=datetime.date.fromisoformat(end) if end else None,
        ambiguous=ambiguous,
    )


class TestLocalDate:
    def test_a_naive_datetime_is_refused(self):
        """The timezone bug one layer up. Python would read it as local time on
        whatever machine happened to run the job."""
        with pytest.raises(ValueError, match="aware"):
            local_date_of(datetime.datetime(2026, 6, 1, 12, 0))

    def test_midnight_utc_in_summer_is_already_the_next_belgian_day(self):
        """CEST is UTC+2, so 22:30Z on 30 June is 00:30 on 1 July in Brussels.

        This is the whole reason the conversion exists: `meter_data`'s dates are
        Belgian calendar dates, and reading them against a UTC date moves every
        transfer by up to two hours' worth of readings.
        """
        moment = datetime.datetime(2026, 6, 30, 22, 30, tzinfo=datetime.UTC)
        assert local_date_of(moment) == datetime.date(2026, 7, 1)

    def test_the_naive_utc_reading_would_disagree(self):
        """NEGATIVE CONTROL. Without it the test above passes for any timezone,
        including UTC, and asserts nothing about Brussels."""
        moment = datetime.datetime(2026, 6, 30, 22, 30, tzinfo=datetime.UTC)
        assert moment.date() == datetime.date(2026, 6, 30)
        assert local_date_of(moment) != moment.date()

    def test_winter_is_one_hour_not_two(self):
        moment = datetime.datetime(2026, 1, 31, 23, 30, tzinfo=datetime.UTC)
        assert local_date_of(moment) == datetime.date(2026, 2, 1)


class TestCovers:
    def test_the_first_and_last_days_are_both_inside(self):
        """CLOSED at both ends, mirroring meter_data. An exclusive upper bound
        here loses the transfer day itself."""
        subject = window(start="2026-03-01", end="2026-03-31")
        assert covers(subject, datetime.date(2026, 3, 1))
        assert covers(subject, datetime.date(2026, 3, 31))

    def test_the_days_either_side_are_outside(self):
        subject = window(start="2026-03-01", end="2026-03-31")
        assert not covers(subject, datetime.date(2026, 2, 28))
        assert not covers(subject, datetime.date(2026, 4, 1))

    def test_an_open_ended_window_covers_the_far_future(self):
        assert covers(window(start="2020-01-01", end=None), datetime.date(2099, 1, 1))


class TestOverlaps:
    def test_a_normal_transfer_does_not_overlap(self):
        """`end_date = next.start_date - 1` is the SHAPE OF A CORRECT TRANSFER.

        Flagging it ambiguous would mark every meter that ever changed hands, and
        since an ambiguous window contributes no membership, k would then suppress
        every community with a transfer in its history.
        """
        outgoing = window(member=1, start="2026-01-01", end="2026-02-28")
        incoming = window(member=2, start="2026-03-01")
        assert not overlaps(outgoing, incoming)
        assert not overlaps(incoming, outgoing)

    def test_a_one_day_overlap_is_an_overlap(self):
        """Off by one in the other direction: the CRM has no constraint stopping
        this, and it means two members hold the meter on 28 February."""
        outgoing = window(member=1, start="2026-01-01", end="2026-02-28")
        incoming = window(member=2, start="2026-02-28")
        assert overlaps(outgoing, incoming)
        assert overlaps(incoming, outgoing)

    def test_two_open_ended_windows_always_overlap(self):
        assert overlaps(window(member=1, start="2020-01-01"), window(member=2, start="2026-01-01"))

    def test_an_open_ended_window_overlaps_anything_after_its_start(self):
        assert overlaps(
            window(member=1, start="2026-01-01"),
            window(member=2, start="2026-06-01", end="2026-06-30"),
        )

    def test_an_open_ended_window_does_not_reach_backwards(self):
        assert not overlaps(
            window(member=1, start="2026-06-01"),
            window(member=2, start="2026-01-01", end="2026-05-31"),
        )

    def test_a_window_contained_in_another_overlaps(self):
        assert overlaps(
            window(member=1, start="2026-01-01", end="2026-12-31"),
            window(member=2, start="2026-06-01", end="2026-06-30"),
        )


class TestMarkAmbiguous:
    def test_a_clean_history_flags_nothing(self):
        flagged = mark_ambiguous(
            [
                window(member=1, start="2026-01-01", end="2026-02-28"),
                window(member=2, start="2026-03-01"),
            ]
        )
        assert [w.ambiguous for w in flagged] == [False, False]

    def test_both_sides_of_an_overlap_are_flagged(self):
        """BOTH, not one. There is no way to tell which row is the mistake, and
        trusting either would attribute a household's energy to a member who may
        never have held the meter."""
        flagged = mark_ambiguous(
            [
                window(member=1, start="2026-01-01", end="2026-03-31"),
                window(member=2, start="2026-03-01"),
            ]
        )
        assert [w.ambiguous for w in flagged] == [True, True]

    def test_two_members_on_different_meters_are_not_ambiguous(self):
        """The normal case for a community, and the one a naive implementation
        breaks: grouping by anything other than EAN flags every member in every
        multi-meter community."""
        flagged = mark_ambiguous(
            [
                window(ean="541448000000000001", member=1, start="2026-01-01"),
                window(ean="541448000000000002", member=2, start="2026-01-01"),
            ]
        )
        assert [w.ambiguous for w in flagged] == [False, False]

    def test_a_single_window_is_never_ambiguous_with_itself(self):
        """The `position != index` guard, asserted. Without it every window
        overlaps itself and the whole projection flags everything."""
        flagged = mark_ambiguous([window(member=1, start="2026-01-01")])
        assert flagged[0].ambiguous is False

    def test_an_incoming_flag_is_recomputed_not_trusted(self):
        """Passing `ambiguous=True` on a clean history clears it.

        The port applies this on every read, so a stale flag from a previous
        refresh must not survive a CRM correction - otherwise fixing the CRM
        leaves the member suppressed for ever.
        """
        flagged = mark_ambiguous([window(member=1, start="2026-01-01", ambiguous=True)])
        assert flagged[0].ambiguous is False

    def test_three_windows_flag_only_the_overlapping_pair(self):
        flagged = mark_ambiguous(
            [
                window(member=1, start="2026-01-01", end="2026-01-31"),
                window(member=2, start="2026-02-01", end="2026-03-31"),
                window(member=3, start="2026-03-01", end="2026-04-30"),
            ]
        )
        assert [w.ambiguous for w in flagged] == [False, True, True]


class TestMembersAt:
    def test_the_holder_on_that_day_is_counted(self):
        windows = [window(member=7, start="2026-01-01")]
        assert members_at(windows, datetime.date(2026, 6, 1)) == {7}

    def test_an_ambiguous_window_contributes_no_member(self):
        """Its ENERGY still counts - that is the rollup's job - but its
        MEMBERSHIP does not. k thresholds on a lower bound: undercounting
        over-suppresses, overcounting is the privacy failure."""
        windows = mark_ambiguous(
            [
                window(member=1, start="2026-01-01", end="2026-03-31"),
                window(member=2, start="2026-03-01"),
            ]
        )
        assert members_at(windows, datetime.date(2026, 3, 15)) == set()

    def test_a_null_member_contributes_nothing(self):
        assert (
            members_at([window(member=None, start="2026-01-01")], datetime.date(2026, 6, 1))
            == set()
        )

    def test_a_member_holding_three_meters_counts_once(self):
        """plan 9.3, stated as a test: "a member with three meters is one member;
        counting devices makes the guarantee decorative"."""
        windows = [
            window(ean="541448000000000001", member=4, start="2026-01-01"),
            window(ean="541448000000000002", member=4, start="2026-01-01"),
            window(ean="541448000000000003", member=4, start="2026-01-01"),
        ]
        assert members_at(windows, datetime.date(2026, 6, 1)) == {4}
        assert len(members_at(windows, datetime.date(2026, 6, 1))) == 1

    def test_the_day_before_a_window_opens_has_nobody(self):
        windows = [window(member=1, start="2026-03-01")]
        assert members_at(windows, datetime.date(2026, 2, 28)) == set()
