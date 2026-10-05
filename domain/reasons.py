"""The rejection taxonomy, and the scope of each reason.

THIS MODULE IS THE REFERENCE. It resolves a contradiction the design documents
carried and deferred: `live-data-plan.md` 7.4 and `live-data-protocol.md` 4.2
each call themselves the "full taxonomy" and they do not agree. Plan 18's "Not
corrected, and why" defers the reconciliation to build step 2 - this is step 2.

Resolved in favour of the PROTOCOL, for the reason D-2 gave when the two
documents disagreed about the enrolment path: the protocol is the document that
leaves the building, with three independent implementers, and a path baked into
flashed firmware cannot be changed.

Two substantive changes were made to protocol 4.2 BEFORE the freeze, both
recorded in `live-data-decisions.md`:

1.  `both_directions_positive` is GONE as a rejection reason.

    It was physically wrong. A P1 connector differences cumulative import and
    export registers over a quarter-hour. If net flow changes sign inside that
    interval - a cloud crossing a PV site - BOTH registers genuinely advance, so
    `import_wh > 0` and `export_wh > 0` in the same measurement. That is the
    normal case on a partly-cloudy spring day, not an anomaly, and the rule as
    written would have discarded most of one. The device could never have learnt
    why: the flow is one-way and rejections are never sent back (protocol 4.1).

    The physically meaningful statement is that TOTAL energy through the
    connection in one interval cannot exceed capacity x interval, which is
    `over_device_ceiling` applied to `import_wh + export_wh`. The case is still
    worth seeing, so it survives as `both_directions_observed` - a counter, not a
    rejection. See `COUNTER_ONLY` at the bottom of this module.

2.  `device_unknown` and `device_revoked` stay SPLIT.

    Protocol 4.2 carried them on one row, which is why the two documents appear
    to disagree about the count (13 rows, 14 reason strings). They are different
    alerts and merging them hides which fired: `device_unknown` means a dynsec
    client exists with no database row behind it - an enrolment that half
    committed, i.e. plan 8.2's ordering bug. `device_revoked` means the row is
    there and revoked while its broker client is still publishing - a revocation
    that half completed. Same symptom, opposite repair.

SCOPE is the column neither document had, and it is the single most consequential
thing frozen here.

    Protocol 3.3 says a store-and-forward device republishes its backlog as an
    array of up to 200 measurements, that this is normal traffic rather than an
    exception, and that "the server accepts gaps". Rejecting the whole message
    because measurement #147 is one second off its boundary contradicts all three
    at once - and because the device cannot learn that it was rejected, it
    re-sends the same batch forever, losing the same 199 good readings each time.

    So: envelope and identity faults are MESSAGE-scoped (nothing is stored -
    either the payload cannot be trusted at all, or the publisher cannot be
    identified). Value and time faults are MEASUREMENT-scoped (that one reading
    is dropped and counted; the rest of the batch is stored).
"""

from enum import StrEnum
from typing import Final


class RejectScope(StrEnum):
    """What a rejection discards."""

    # The payload cannot be trusted, or the publisher cannot be identified.
    # Nothing from this message is stored.
    MESSAGE = "message"
    # One reading is bad. It is dropped and counted; the rest of the batch is
    # stored. This is what makes a 200-entry backlog survive one drifted reading.
    MEASUREMENT = "measurement"


class RejectReason(StrEnum):
    """protocol 4.2. Thirteen reasons, each with a scope (see SCOPES below).

    Stored in `device_last.last_reject_reason` - LAST ONE WINS, so the column
    answers "what most recently went wrong with this device" and cannot answer
    "how often". The count per reason is `core.metrics.ingest_rejections`, a
    counter with a `reason` attribute, emitted from `worker/main._record_outcome`.

    This docstring has been wrong twice in opposite directions: it first claimed
    that counter when no instrument existed, and was then "corrected" to cite an
    `ingest_reject` TABLE that does not exist either - there are 21 tables in
    `scripts/sql/schema.sql` and that is not one of them. The counter is now real
    and this paragraph names it.

    Message-scoped rejections also land in `ingest_dead_letter`; measurement-
    scoped ones do not, and rows whose device could not be identified carry
    `id_device = NULL` and are excluded from `/ops/health` by its join. So the
    counter is the only per-reason total that covers all thirteen.

    The string values are the protocol's own and are part of the published
    interface - do not rename them to match a Python convention.
    """

    # ---- MESSAGE scope: nothing from this message is stored ----
    # Wrong types, a missing required field, malformed JSON.
    SCHEMA_INVALID = "schema_invalid"
    # An unrecognised key at the ENVELOPE level. Note the asymmetry with
    # protocol 7, which is deliberate and is the whole of the compatibility
    # story: unknown keys at the envelope are REJECTED, unknown keys inside a
    # measurement are IGNORED AND COUNTED. Without the second half, the day a
    # connector adds a field every message from that firmware is rejected
    # forever - and the device is in a basement with no OTA.
    UNKNOWN_FIELD = "unknown_field"
    # More than 200 measurements in one message.
    BATCH_TOO_LARGE = "batch_too_large"
    # The same `ts` twice in one message. Caught BEFORE the upsert: Postgres
    # raises 21000 when one statement touches the same row twice, which would
    # abort the whole batch and leave the device retrying it forever.
    DUPLICATE_TS_IN_BATCH = "duplicate_ts_in_batch"
    # No device row for that id. A dynsec client exists with nothing behind it.
    DEVICE_UNKNOWN = "device_unknown"
    # The device row exists and is revoked, but its broker client is still
    # publishing. A revocation that half completed.
    DEVICE_REVOKED = "device_revoked"
    # The `community_id` in the topic is not the device's. LOAD-BEARING, not
    # belt-and-braces: Phase 0 demonstrated that a device CAN publish under
    # another community's id, because the broker ACL is `ce/+/%u/telemetry` and
    # the `+` permits any value at that level.
    COMMUNITY_MISMATCH = "community_mismatch"

    # ---- MEASUREMENT scope: this reading is dropped, the batch is stored ----
    # More than 5 minutes ahead of server time.
    TS_IN_FUTURE = "ts_in_future"
    # More than 35 days old. A drifted clock, not a backlog.
    TS_TOO_OLD = "ts_too_old"
    # Not on a 15-minute boundary.
    TS_NOT_ALIGNED = "ts_not_aligned"
    # Any `*_wh` below zero. A meter replacement resets the index; the connector
    # is required to mark that interval invalid and skip it rather than send the
    # difference across the reset (protocol 8.4).
    NEGATIVE_ENERGY = "negative_energy"
    # `import_wh + export_wh` above the device's ceiling. See the module
    # docstring for why this replaced `both_directions_positive`.
    OVER_DEVICE_CEILING = "over_device_ceiling"
    # Production above capacity x interval, or production at night. Catches a
    # stolen credential injecting fabricated production into a signal members act
    # on, a mis-set `pure_injection`, and a mis-wired inverter.
    IMPLAUSIBLE_PRODUCTION = "implausible_production"


SCOPES: Final[dict[RejectReason, RejectScope]] = {
    RejectReason.SCHEMA_INVALID: RejectScope.MESSAGE,
    RejectReason.UNKNOWN_FIELD: RejectScope.MESSAGE,
    RejectReason.BATCH_TOO_LARGE: RejectScope.MESSAGE,
    RejectReason.DUPLICATE_TS_IN_BATCH: RejectScope.MESSAGE,
    RejectReason.DEVICE_UNKNOWN: RejectScope.MESSAGE,
    RejectReason.DEVICE_REVOKED: RejectScope.MESSAGE,
    RejectReason.COMMUNITY_MISMATCH: RejectScope.MESSAGE,
    RejectReason.TS_IN_FUTURE: RejectScope.MEASUREMENT,
    RejectReason.TS_TOO_OLD: RejectScope.MEASUREMENT,
    RejectReason.TS_NOT_ALIGNED: RejectScope.MEASUREMENT,
    RejectReason.NEGATIVE_ENERGY: RejectScope.MEASUREMENT,
    RejectReason.OVER_DEVICE_CEILING: RejectScope.MEASUREMENT,
    RejectReason.IMPLAUSIBLE_PRODUCTION: RejectScope.MEASUREMENT,
}


# Observations that are counted but never reject anything. They are here rather
# than in the enum so that `RejectReason` stays exactly the protocol's published
# list and a reader cannot mistake one for the other.
class ObservedCode(StrEnum):
    """Counted, never rejected."""

    # import_wh and export_wh both above zero in one interval. Normal for a P1
    # differencing registers across a sign change; see the module docstring. It
    # is counted because a device reporting it on EVERY interval is a different
    # story - that is a connector sending cumulative index readings rather than
    # differences, which protocol 3.1 rule 3 warns about and which no bounds
    # check can detect on its own.
    #
    # THAT RATIO IS PER DEVICE, AND `ingest.observations.total` CANNOT ANSWER IT.
    # The counter is labelled by `code` and nothing else, because a device
    # attribute would be unbounded cardinality - see `core/metrics.py`. So the
    # metric is a volume line; the diagnosis is in the logs, `operation=
    # ingest:observe`, whose `topic` field carries the device's public id.
    BOTH_DIRECTIONS_OBSERVED = "both_directions_observed"
    # An unrecognised key inside a measurement. Protocol 7 requires these to be
    # ignored AND counted - that is what lets a newer connector add a field
    # without every one of its messages being hard-rejected by an older server.
    UNKNOWN_MEASUREMENT_FIELD = "unknown_measurement_field"


def scope_of(reason: RejectReason) -> RejectScope:
    """The scope of a reason.

    A plain dict lookup, deliberately unguarded: `SCOPES` is exhaustive over the
    enum and `test_reasons.py` asserts it, so a KeyError here means a reason was
    added without deciding its scope - which is precisely the moment to fail.
    """
    return SCOPES[reason]
