"""Who may read a community aggregate, and how much of it.

The second half of plan 9.1's chokepoint. `api/live/repository._scoped` answers
"whose rows"; this answers "may this caller see them at all". They are separate
because they fail differently: a tenancy mistake leaks another community's data,
a visibility mistake leaks this community's data to its own members.

----------------------------------------------------------------------------
BOTH SWITCHES PRODUCE THE SAME 403 AND THE SAME CODE.

`members_see_aggregate` gates whether a member may read community aggregates at
all; `members_see_production` gates whether the production term may appear.
Distinguishing them in the response would tell a member which switch their
administrator had turned off - information about the community's configuration
that the refusal itself is meant to withhold - and the remediation is the same
either way.

A NULLED FIELD IS NOT ACCEPTABLE, which is why this is a refusal and not a
redaction. plan 9.4, restating criterion 8: "a balance minus a consumption
reconstructs the production exactly". Returning the summary with `production`
removed but the balance intact hands over the thing that was withheld.
----------------------------------------------------------------------------

THE DEFAULTS LIVE IN BOTH PLACES, AND A TEST PINS THEM TOGETHER.

`community_live_settings` carries column defaults; `DEFAULT_VISIBILITY` below
carries the same values for the community that has never saved the panel. They
must agree - a drift between them is a silent privacy change, visible to nobody,
that applies only to communities which never opened the settings screen.
`tests/test_read_api.py::test_the_code_defaults_match_the_database_defaults`
round-trips a `DEFAULT VALUES` insert to assert it.
"""

from dataclasses import dataclass

from core.context_vars import current_user_role
from core.errors.errors import ErrorException
from core.security.user_context import Role
from domain.kanon import K_FLOOR, effective_k
from shared.custom_errors import errors
from shared.models.local_models import CommunityLiveSettingsModel


@dataclass(frozen=True, slots=True)
class LiveVisibility:
    members_see_production: bool
    members_see_aggregate: bool
    k: int
    is_default: bool


# plan 9.4's defaults, and the reasoning has to be RE-MADE rather than inherited.
#
# The original justification for `members_see_aggregate = TRUE` was "otherwise
# every member's signal screen is a 403 wall on day one". D-5 held the member
# screen, so that sentence is now vacuous - schema.sql says so at the table.
#
# The default stands anyway, on a different argument: the endpoint exists and is
# member-reachable (it is what makes criterion 8 testable at all), the data is
# the community's own production, and a default that hides a feature from the
# people it was built for is the worse failure. An administrator who wants it
# closed has a switch; one who never knew it was open has nothing.
DEFAULT_VISIBILITY = LiveVisibility(
    members_see_production=True,
    members_see_aggregate=True,
    k=5,
    is_default=True,
)


def resolve(row: CommunityLiveSettingsModel | None) -> LiveVisibility:
    """The settings in force, whether or not a row exists."""
    if row is None:
        return DEFAULT_VISIBILITY
    return LiveVisibility(
        members_see_production=row.members_see_production,
        members_see_aggregate=row.members_see_aggregate,
        # Floored here as well as by the CHECK constraint. Defence in depth: the
        # constraint is the real guard, and this is the one that still holds if a
        # migration ever drops it.
        k=effective_k(row.k),
        is_default=False,
    )


def _current_role() -> Role | None:
    raw = current_user_role.get()
    if raw is None:
        return None
    try:
        return Role(raw)
    except ValueError:
        # An unparseable role is NOT a member. The gateway sets this header and a
        # value this service does not recognise means the two have drifted, which
        # is a reason to refuse rather than to guess downwards.
        return None


def require_aggregate_visible(visibility: LiveVisibility) -> None:
    """403 unless this caller may read the community aggregate.

    MANAGER and ADMIN always may - the switches govern what MEMBERS see, and an
    administrator who could lock themselves out of their own dashboard would file
    it as a bug.

    A caller whose role does not resolve is refused. `require_min_role(MEMBER)`
    on the route has already rejected an unauthenticated request, so reaching
    here without a role means the role header was present and unreadable.
    """
    role = _current_role()
    if role in (Role.MANAGER, Role.ADMIN):
        return
    if (
        role is Role.MEMBER
        and visibility.members_see_aggregate
        and visibility.members_see_production
    ):
        return
    raise ErrorException(errors.live.AGGREGATE_NOT_VISIBLE, status_code=403)


def k_for(visibility: LiveVisibility) -> int:
    """The threshold to apply to this request's buckets.

    Applied for EVERY role, managers included. k protects the members of the
    community from each other and from their own administrator - plan 16 is
    explicit that quarter-hourly household electricity is occupancy data - so a
    manager reading an aggregate of two households gets the same suppression a
    member would. The manager's privilege is over devices and settings, not over
    other members' consumption curves.
    """
    return max(visibility.k, K_FLOOR)
