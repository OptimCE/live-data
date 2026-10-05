"""The k threshold. Pure: no session, no settings object, no request.

plan 9.3, with its two refinements over the spec and one decision the spec left
open.

----------------------------------------------------------------------------
PRODUCTION IS NEVER SUPPRESSED. THE GRID TERMS ARE.

plan 9.3 ends "Production is not subject to k", and phase 1 has no consumption
term (deviation 6) - so read literally, k would suppress nothing at all and the
whole mechanism would be decorative until phase 2.

DECIDED 2026-09-16: k gates `import_wh` and `export_wh` per bucket; `production_wh`
is always published. The split is not a compromise, it is the distinction the
sentence was making:

  - GRID EXCHANGE is household behaviour. An hour of a community's import curve,
    with two members in it, is essentially one household's occupancy - when they
    woke, whether they were away, when the oven went on.
  - PRODUCTION is a property of the installations, and in an energy community it
    is collectively owned and already reported in aggregate elsewhere. A PV array
    at noon in June reveals the weather.

The practical consequence decided it: a pilot community of three or four members
would otherwise open the product to a blank chart on day one and stay that way
until it recruited a fifth member - the same "feature looks broken to its own
users" failure plan 9.4 warns about for the visibility defaults.
----------------------------------------------------------------------------

THRESHOLDED ON `n_members`, NEVER `n_devices`.

"A member with three meters is one member; counting devices makes the guarantee
decorative." `rollup_community_hour` stores both, and only one of them is safe to
threshold on.

SUPPRESSED PER BUCKET, NOT PER REQUEST.

An all-or-nothing "below k" bit computed as a MIN over a caller-chosen window is
bisectable on that same grid - and one bad bucket blanks a 30-day chart. Each
bucket is judged alone; the suppressed ones lose their grid terms and the
response carries a count.

`n_members is None` SUPPRESSES.

Fail closed. None means the projection has not run for that bucket, not that
nobody was there, and publishing an unprotected series on the strength of a
missing value is the kind of default that survives until someone notices it in
production.
"""

from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum
from typing import Final

# The remainder row of the operation rollups: devices in no operation. Mirrors
# `shared.const.NO_SHARING_OPERATION`, restated here because this module imports
# nothing from the service (see its docstring).
REMAINDER: Final[int] = 0

# The floor, mirrored from schema.sql's CHECK (k >= 3) so a settings row that
# somehow carries less cannot weaken the guarantee at read time. Defence in
# depth: the constraint is the real guard, this is the one that still holds if a
# migration ever drops it.
K_FLOOR: Final[int] = 3


class AbsentReason(StrEnum):
    """Why a term is missing from a payload.

    Every absent term is NAMED. plan 11.1's DTO rule: there is no
    `consumption: null` - the key is absent from the JSON and from the OpenAPI
    schema, and this list says why. `null` is what a chart renders as zero.
    """

    BELOW_K_THRESHOLD = "below_k_threshold"
    NO_CONSUMPTION_IN_PHASE_1 = "no_consumption_in_phase_1"
    NOT_MEASURED = "not_measured"
    # Measured, but no hour has CLOSED yet - a community in its first hour or so.
    # `/summary` reads closed hours only, and "not measured" would tell its
    # manager that the meters cannot see production, which is false.
    NO_CLOSED_HOUR_YET = "no_closed_hour_yet"


@dataclass(frozen=True, slots=True)
class Absent:
    term: str
    reason: AbsentReason


def effective_k(configured: int | None) -> int:
    """The threshold actually applied. Never below `K_FLOOR`, never None."""
    if configured is None:
        return K_FLOOR
    return max(int(configured), K_FLOOR)


def grid_is_visible(n_members: int | None, k: int | None) -> bool:
    """Whether a bucket's `import_wh`/`export_wh` may be published.

    `n_members is None` is False - see the module docstring. So is a negative or
    zero count, which no correct rollup produces and which would otherwise sail
    past a `>= k` written with the wrong comparison.
    """
    if n_members is None:
        return False
    return n_members >= effective_k(k)


ABSENT_GRID: Final[tuple[Absent, ...]] = (
    Absent("import_wh", AbsentReason.BELOW_K_THRESHOLD),
    Absent("export_wh", AbsentReason.BELOW_K_THRESHOLD),
)

# The estimated shared energy (D-14) is derived from the grid terms of the same
# rows, so it is withheld with them and for the same reason. Only on payloads
# that carry it - the community and operation series; never a member's.
ABSENT_SHARED: Final[Absent] = Absent("shared_wh", AbsentReason.BELOW_K_THRESHOLD)


# ---------------------------------------------------------------------------
# PER SHARING OPERATION, AND THE COMMUNITY TOTAL NEXT TO THEM (D-14).
#
# An operation's grid terms are judged on the operation's own members, exactly
# like the community's. The new risk is DIFFERENCING: once the operations are
# published next to the community total, total minus the visible operations is
# whatever is NOT visible - a small operation below k, or the few devices in no
# operation. So the total is published only when that residual is itself at
# least k members, or empty.
#
# Judged on STORED COUNTS, because k is a read-time setting. Two inputs make it
# exact rather than hopeful:
#
#   * the REMAINDER row (id 0, `shared.const.NO_SHARING_OPERATION`): every device
#     in no operation lands there, so "is anything outside the visible
#     operations?" is a row lookup. Device counts cannot answer it - a day's MAX
#     counts do not add up across rows;
#   * `n_members_min`, the bucket's least-populated hour (== n_members for an
#     hour). A DAY is published only if every hour of it was, or "day minus its
#     published hours" is the withheld hours - which judging a day on its MAX
#     allowed, for the community view too, before this.
#
# The residual uses the sum of the visible operations' members, which counts a
# member with meters in two operations twice - so it is a LOWER bound on the
# true residual, and the check errs towards withholding.
#
# ONE absent reason for both causes, deliberately. A distinct "withheld because
# of another operation" would itself tell the reader that a small operation
# exists. "Too few members to show without identifying them" is true of both.
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ScopeMembers:
    """One operation row's member counts for one bucket.

    For an hour, `n_members_min == n_members_max`. For a day, the MIN and the MAX
    over the day's hours. `id_sharing_operation == 0` is the remainder.
    """

    id_sharing_operation: int
    n_members_min: int | None
    n_members_max: int | None


def operation_grid_is_visible(scope: ScopeMembers, k: int | None) -> bool:
    """Whether one operation's grid terms (import, export, shared) may be shown.

    On the MIN: a day passes only if every one of its hours did. The remainder
    row is never published as an operation of its own.
    """
    if scope.id_sharing_operation == REMAINDER:
        return False
    return grid_is_visible(scope.n_members_min, k)


def community_grid_is_visible(
    n_members_min: int | None, operations: Iterable[ScopeMembers], k: int | None
) -> bool:
    """Whether the COMMUNITY's grid terms may be shown next to its operations'.

    `n_members_min` is the community bucket's (its own n_members for an hour).
    `operations` are the same bucket's operation rows, the remainder included.
    No rows at all - a bucket rolled up before migration 0003 - publishes nothing
    per operation either, so there is nothing to subtract and the community's own
    k is the whole test.
    """
    if n_members_min is None or not grid_is_visible(n_members_min, k):
        return False
    rows = list(operations)
    hidden = any(
        row.id_sharing_operation == REMAINDER or not operation_grid_is_visible(row, k)
        for row in rows
    )
    if not hidden:
        return True
    # Subtract every operation that could be visible in ANY hour of the bucket
    # (its MAX reaches k), at its MAX: the most members those rows could account
    # for. What remains must still be k members, or it is identifiable.
    visible_at_most = sum(
        row.n_members_max or 0
        for row in rows
        if row.id_sharing_operation != REMAINDER and grid_is_visible(row.n_members_max, k)
    )
    return n_members_min - visible_at_most >= effective_k(k)


# Phase 1 has no consumption term at all, for any bucket, at any k. Named rather
# than omitted silently, so a client can tell "not available in this phase" from
# "withheld for this bucket" - which is the difference between a roadmap question
# and a support call.
ABSENT_CONSUMPTION: Final[Absent] = Absent("consumption_wh", AbsentReason.NO_CONSUMPTION_IN_PHASE_1)
