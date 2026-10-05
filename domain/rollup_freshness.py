"""Is the SCHEDULER keeping up? Pure: no session, no clock of its own.

----------------------------------------------------------------------------
WHY THE NEWEST BUCKET CANNOT ANSWER IT.

`/ops/health` used to report `rollup_age_minutes` - `now - MAX(bucket)` - and
the SPA called anything over 90 minutes "not recomputed". But a bucket exists
only where a device reported. A fleet that goes quiet at dusk stops producing
buckets while the scheduler is perfectly healthy, so the age climbed past 90
every evening and the page blamed the rollups for a quiet fleet. The age of the
newest DATA is a fact about the devices; this module answers the question about
the scheduler.

Three per-community watermarks, because each has a blind spot the others cover:

  * `newest_bucket` - MAX(rollup_community_hour.bucket). How new the data is,
    and whether anything is left inside the recompute window at all.
  * `computed_at`   - MAX(rollup_community_hour.computed_at). Every tick DELETEs
    and re-INSERTs the whole 48-hour window with `computed_at = now`, so while
    anything is inside the window this moves on every tick - whatever the
    fleet did since. (The scheduler's own `rollup.lag{scope=tick}` gauge reads
    the same column.) Its blind spot: with nothing in the window, nothing is
    rewritten and it stops moving on a healthy scheduler.
  * `pending_since` - MIN(rollup_dirty.marked_at). Ingest marks a bucket in the
    SAME statement as every stored reading, keeping the first mark, and the
    tick's first statement claims the marks. So this is how long a stored
    reading has waited for a tick. It covers what `computed_at` cannot see: a
    scheduler dead since the community's first reading (no rollup row exists,
    so there is no `computed_at` at all), and one that died during a silence
    longer than the window, after which readings resumed.
----------------------------------------------------------------------------

The limit is a number of TICKS, not of minutes: `STALE_AFTER_TICKS` missed in a
row. One missed tick is a restart; three is a scheduler that is not running.
"""

import datetime
from dataclasses import dataclass
from enum import StrEnum

from domain import buckets

# A healthy recompute age runs from zero to one tick plus the tick's own
# duration. Three ticks is two in a row that did not happen.
STALE_AFTER_TICKS = 3


class RollupFreshness(StrEnum):
    # Nothing has been rolled up for this community, and nothing has been waiting
    # long enough to be a fault - a brand-new community before its first tick.
    NEVER = "never"
    # A tick has rewritten the window recently, and nothing is waiting.
    FRESH = "fresh"
    # Stored readings have waited too long, or the window has not been rewritten
    # for too long. The one state that means the scheduler is not keeping up.
    STALE = "stale"
    # Nothing is left inside the recompute window and nothing is waiting: there is
    # nothing for a tick to do, so how long ago one last did says nothing about
    # the scheduler. Never red - the devices page has the explanation.
    IDLE = "idle"


@dataclass(frozen=True, slots=True)
class Verdict:
    state: RollupFreshness
    # The age the verdict rests on: the recompute age when FRESH, the larger of
    # the two ages when STALE, None for NEVER and IDLE.
    lag: datetime.timedelta | None


def _age(now: datetime.datetime, moment: datetime.datetime) -> datetime.timedelta:
    # `computed_at` is the scheduler's clock and `marked_at` the database's; `now`
    # is the API's. A few seconds of skew must not produce a negative age.
    return max(now - moment, datetime.timedelta(0))


def classify(
    *,
    now: datetime.datetime,
    newest_bucket: datetime.datetime | None,
    computed_at: datetime.datetime | None,
    pending_since: datetime.datetime | None,
    tick_minutes: int,
) -> Verdict:
    """The verdict, checked in order: stale, never, idle, fresh."""
    limit = datetime.timedelta(minutes=tick_minutes * STALE_AFTER_TICKS)
    lo, _hi = buckets.window(now)

    waited = _age(now, pending_since) if pending_since is not None else None
    since_recompute = _age(now, computed_at) if computed_at is not None else None
    in_window = newest_bucket is not None and newest_bucket >= lo

    overdue = [
        age
        for age, applies in (
            (waited, True),
            # Only while there is something inside the window to rewrite.
            (since_recompute, in_window),
        )
        if applies and age is not None and age > limit
    ]
    if overdue:
        return Verdict(RollupFreshness.STALE, max(overdue))
    if newest_bucket is None or since_recompute is None:
        return Verdict(RollupFreshness.NEVER, None)
    if not in_window:
        return Verdict(RollupFreshness.IDLE, None)
    return Verdict(RollupFreshness.FRESH, since_recompute)
