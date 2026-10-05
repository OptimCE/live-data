"""Ownership-window algebra. Pure: no session, no CRM, no clock.

Step 7's counterpart to `domain/buckets.py`, and separate from `ports/crm_core.py`
for the same reason: the rules below decide who a kilowatt-hour is attributed to,
and they have to be testable without a database.

----------------------------------------------------------------------------
THE BOUNDS ARE INCLUSIVE AT BOTH ENDS, BECAUSE `meter_data`'S ARE.

`meter_data.start_date` / `end_date` are DATEs, and every existing reader in the
platform treats them as a CLOSED interval:

    BETWEEN md.start_date AND COALESCE(md.end_date, 'infinity'::date)

- `billing/ports/crm_core_sqlalchemy.py`, in both the attribution join and the
  overlap pre-flight;
- this service's own `ports/crm_read.py`, in `_METER_SQL`.

`device_owner_window` mirrors those two columns verbatim, so it inherits the
convention verbatim. Converting to a half-open form on the way in would mean
`valid_to = end_date + 1 day`, and every off-by-one there is a whole day of one
household's energy attributed to the wrong member - silently, and only on meters
that changed hands, which is precisely the case the table exists to get right.

The consequence worth stating: two windows are ADJACENT, not overlapping, when
one ends the day before the next begins. `end_date = next.start_date - 1` is the
normal, correct shape of a meter transfer and must NOT be flagged ambiguous.
`end_date = next.start_date` is a real overlap of exactly one day.
----------------------------------------------------------------------------

AMBIGUITY IS FLAGGED, NEVER REFUSED.

Billing raises a 422 and abandons the run when it finds overlapping windows
(`errors.billing.METER_OWNERSHIP_OVERLAP`). It can: an invoice that might be
wrong must not be issued, and a human fixes the CRM.

A background projection has nobody to refuse to. So it flags the window instead,
and the rollup then treats the two halves differently:

  - the ENERGY is still counted. Dropping it would understate the community
    total, and the energy is not in doubt - only its owner is.
  - the MEMBERSHIP is not. `n_members` must be a LOWER bound on the distinct
    members contributing to a bucket, because k thresholds on it: undercounting
    only over-suppresses, while overcounting is the privacy failure itself.
"""

import datetime
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, replace
from typing import Final
from zoneinfo import ZoneInfo

from shared.const import ROLLUP_DAY_TIMEZONE

# The same zone `domain/buckets.py` uses for the day rollup, and for the same
# reason: `meter_data`'s DATEs are Belgian calendar dates, so the instant->date
# conversion has to happen in the Belgian calendar or a transfer moves by a day
# for every reading between midnight and 01:00 or 02:00 local.
_OWNER_TZ: Final[ZoneInfo] = ZoneInfo(ROLLUP_DAY_TIMEZONE)


@dataclass(frozen=True, slots=True)
class OwnershipWindow:
    """One `meter_data` row, reduced to what attribution needs.

    `id_member` is NULLABLE and a None is not an error: the CRM genuinely has
    rows with no member attached. Their energy counts and their membership does
    not - the same treatment an ambiguous window gets, for a different reason.

    `id_sharing_operation` is the window's operation (D-14), None when the meter
    is in none at these dates. One per window: crm-backend's `addMeterData`
    closes the previous row before opening the next, so an EAN's windows form a
    chain and a meter changes operation only at a date boundary.
    """

    ean: str
    id_community: int
    id_member: int | None
    valid_from: datetime.date
    valid_to: datetime.date | None
    ambiguous: bool = False
    id_sharing_operation: int | None = None


def local_date_of(moment: datetime.datetime) -> datetime.date:
    """The Brussels-local calendar date containing `moment`.

    This is the function that crosses from the measurement's world (TIMESTAMPTZ,
    UTC, 900 s) into `meter_data`'s (DATE, Belgian calendar).
    """
    if moment.tzinfo is None:
        raise ValueError("local_date_of requires an aware datetime")
    return moment.astimezone(_OWNER_TZ).date()


def covers(window: OwnershipWindow, day: datetime.date) -> bool:
    """Whether `window` is in force on `day`. CLOSED at both ends."""
    if day < window.valid_from:
        return False
    return window.valid_to is None or day <= window.valid_to


def overlaps(left: OwnershipWindow, right: OwnershipWindow) -> bool:
    """Whether two windows are in force on any common day.

    The classic closed-interval test with an open upper bound for NULL, matching
    billing's `a.start_date <= COALESCE(b.end_date, 'infinity')
    AND b.start_date <= COALESCE(a.end_date, 'infinity')`.

    Adjacent windows do not overlap - see the module docstring.
    """
    left_end = left.valid_to
    right_end = right.valid_to
    if right_end is not None and left.valid_from > right_end:
        return False
    return left_end is None or right.valid_from <= left_end


def mark_ambiguous(windows: Iterable[OwnershipWindow]) -> list[OwnershipWindow]:
    """Flag every window that overlaps another window for the SAME EAN.

    Quadratic per EAN, deliberately. One EAN has a handful of `meter_data` rows
    over its whole life, and the alternative - a sweep line - buys nothing here
    while being the kind of code that is wrong at the boundary and looks right.

    Grouped by EAN and never across EANs: two members holding two different
    meters on the same day is the normal case, not an ambiguity.
    """
    by_ean: dict[str, list[OwnershipWindow]] = {}
    for window in windows:
        by_ean.setdefault(window.ean, []).append(window)

    flagged: list[OwnershipWindow] = []
    for group in by_ean.values():
        for index, window in enumerate(group):
            is_ambiguous = any(
                overlaps(window, other) for position, other in enumerate(group) if position != index
            )
            flagged.append(replace(window, ambiguous=is_ambiguous))
    return sorted(flagged, key=lambda w: (w.ean, w.valid_from, w.id_member or -1))


def members_at(windows: Sequence[OwnershipWindow], day: datetime.date) -> set[int]:
    """The distinct members unambiguously holding a meter on `day`.

    A LOWER BOUND, on purpose, and the number `n_members` is computed from. Three
    kinds of window are excluded and each exclusion fails CLOSED:

      - ambiguous ones, because two candidate owners cannot both be counted;
      - `id_member IS NULL`, because there is no member to count;
      - windows not in force on the day.
    """
    return {
        window.id_member
        for window in windows
        if window.id_member is not None and not window.ambiguous and covers(window, day)
    }
