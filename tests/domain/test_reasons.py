"""The rejection taxonomy is complete, scoped, and matches the frozen protocol.

`domain/reasons.py` is the reference list that resolved a contradiction the two
design documents carried (plan 7.4 vs protocol 4.2, deferred by plan 18 to build
step 2). These tests are what stop it drifting back apart.
"""

import pytest

from domain.reasons import SCOPES, ObservedCode, RejectReason, RejectScope, scope_of

# The thirteen reasons, transcribed from live-data-protocol.md 4.2 AS FROZEN -
# i.e. after `both_directions_positive` was removed and the combined
# device_unknown/device_revoked row was split in two.
#
# Written out literally rather than derived from the enum: a test that builds its
# expectation from the thing under test asserts only that the code is
# self-consistent. This list is the DOCUMENT, and the point is to catch the code
# drifting away from it.
_PROTOCOL_4_2 = {
    "schema_invalid",
    "unknown_field",
    "ts_in_future",
    "ts_too_old",
    "ts_not_aligned",
    "negative_energy",
    "over_device_ceiling",
    "implausible_production",
    "community_mismatch",
    "device_unknown",
    "device_revoked",
    "batch_too_large",
    "duplicate_ts_in_batch",
}


def test_the_enum_is_exactly_the_frozen_protocol_list():
    assert {r.value for r in RejectReason} == _PROTOCOL_4_2


def test_both_directions_positive_is_not_a_rejection_reason():
    """It was removed from protocol 4.2 BEFORE the freeze, and deliberately.

    A P1 connector differences cumulative import and export registers over a
    quarter-hour. When net flow changes sign inside that interval - a cloud
    crossing a PV site - BOTH registers advance, so import_wh > 0 AND
    export_wh > 0 in the same measurement. That is the normal case on a
    partly-cloudy spring day, and the rule as written would have discarded most
    of one, with the device unable to learn why.

    The physically meaningful statement is that TOTAL energy through the
    connection cannot exceed capacity x interval, which is `over_device_ceiling`
    applied to the sum.
    """
    assert "both_directions_positive" not in {r.value for r in RejectReason}
    # It survives as an observation, because a device reporting it on EVERY
    # interval is a different story - a connector sending index readings rather
    # than differences.
    assert ObservedCode.BOTH_DIRECTIONS_OBSERVED.value == "both_directions_observed"


def test_device_unknown_and_device_revoked_stay_split():
    """Two different alerts with opposite repairs.

    `device_unknown`: a dynsec client exists with no database row - an enrolment
    that half committed. `device_revoked`: the row is there and revoked while the
    broker client is still publishing - a revocation that half completed. Merging
    them hides which fired.
    """
    assert RejectReason.DEVICE_UNKNOWN != RejectReason.DEVICE_REVOKED
    assert scope_of(RejectReason.DEVICE_UNKNOWN) is RejectScope.MESSAGE
    assert scope_of(RejectReason.DEVICE_REVOKED) is RejectScope.MESSAGE


class TestEveryReasonHasAScope:
    def test_the_scope_map_is_exhaustive(self):
        """A missing entry must be a KeyError at the point of decision.

        `scope_of` is deliberately an unguarded dict lookup, so a reason added
        without deciding its scope fails loudly rather than defaulting to
        whichever behaviour happens to be safer-looking.
        """
        assert set(SCOPES) == set(RejectReason)

    @pytest.mark.parametrize("reason", list(RejectReason))
    def test_every_reason_resolves(self, reason: RejectReason):
        assert scope_of(reason) in (RejectScope.MESSAGE, RejectScope.MEASUREMENT)


class TestTheScopeSplitItself:
    """The single most consequential thing build step 2 froze.

    Neither design document said whether a rejection discards the whole MQTT
    message or just the offending measurement. Protocol 3.3 says a device
    republishes its backlog as an array of up to 200 measurements, that this is
    normal traffic, and that "the server accepts gaps" - so discarding 199 good
    readings because #147 is one second off its boundary contradicts all three.
    And because the device cannot learn it was rejected, it re-sends the same
    batch for ever.
    """

    @pytest.mark.parametrize(
        "reason",
        [
            RejectReason.SCHEMA_INVALID,
            RejectReason.UNKNOWN_FIELD,
            RejectReason.BATCH_TOO_LARGE,
            RejectReason.DUPLICATE_TS_IN_BATCH,
            RejectReason.DEVICE_UNKNOWN,
            RejectReason.DEVICE_REVOKED,
            RejectReason.COMMUNITY_MISMATCH,
        ],
    )
    def test_envelope_and_identity_faults_drop_the_message(self, reason: RejectReason):
        """The payload cannot be trusted, or the publisher cannot be identified."""
        assert scope_of(reason) is RejectScope.MESSAGE

    @pytest.mark.parametrize(
        "reason",
        [
            RejectReason.TS_IN_FUTURE,
            RejectReason.TS_TOO_OLD,
            RejectReason.TS_NOT_ALIGNED,
            RejectReason.NEGATIVE_ENERGY,
            RejectReason.OVER_DEVICE_CEILING,
            RejectReason.IMPLAUSIBLE_PRODUCTION,
        ],
    )
    def test_value_and_time_faults_drop_only_that_reading(self, reason: RejectReason):
        assert scope_of(reason) is RejectScope.MEASUREMENT

    def test_the_two_scopes_partition_the_taxonomy(self):
        message = {r for r in RejectReason if scope_of(r) is RejectScope.MESSAGE}
        measurement = {r for r in RejectReason if scope_of(r) is RejectScope.MEASUREMENT}
        assert message & measurement == set()
        assert message | measurement == set(RejectReason)
        # Both halves non-empty: a taxonomy that collapsed to one scope would
        # pass every test above.
        assert message and measurement
