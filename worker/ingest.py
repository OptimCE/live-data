"""Turning MQTT messages into rows.

================================================================================
THE INVERSION THE PLAN DOES NOT MAKE: THERE IS NO ACKNOWLEDGEMENT TO WITHHOLD.

`aiomqtt` 2.x hands every incoming QoS-1 message to paho, which PUBACKs it the
instant it lands in an in-memory deque - before any database write. The only
version with a manual `client.puback()` is v3, and v3 is alpha and MQTTv5-ONLY,
while protocol 1 freezes devices at MQTT 3.1.1 ("not MQTT 5 - the ESP32
firmwares must not need it").

So plan 7.5's "unbounded NACK stalls every other device behind the inflight
window; a blind ack is silent loss" describes a NATS-shaped world that does not
exist here. There is no nack, no redelivery, no max-deliver. The worker gets
EXACTLY ONE CHANCE per message, in-process retry is the only retry there is, and
`ingest_dead_letter` is the sole record that a message ever existed.

The corollary is this module's spine:

    aiomqtt's deque is VOLATILE and UNBOUNDED.
    The broker's persistent session is DURABLE and BOUNDED.

    Therefore, when the database keeps failing, the worker DISCONNECTS FROM THE
    BROKER ON PURPOSE.

That pushes the backlog back to Mosquitto, where `max_queued_messages` bounds it
and logs the overflow. Staying connected would grow an in-memory queue until an
OOM kill discarded thousands of messages that were acked and never written -
with no log line, because nothing observed them.

(The documented way out of "one replica" is MQTT 5 plus `$share/` shared
subscriptions, which Mosquitto supports and which would make the worker
horizontally scalable. It is blocked on aiomqtt v3 leaving alpha, not on
anything structural. "One replica" is a current constraint, not an architecture.)
================================================================================

THE ORDER OF THE FIRST THREE STEPS IS FIXED, AND EACH ALTERNATIVE FAILS SILENTLY.

    1. parse the topic
    2. load the device by public_id ALONE, deliberately UNSCOPED
    3. compare the topic's community with the device's

Phase 0 demonstrated that a device CAN publish under another community's id: the
broker ACL is `ce/+/%u/telemetry` and the `+` permits any value there. So step 3
is a real access control.

  * Scoping the SELECT by the topic's community makes an attacker's mismatch
    report as `device_unknown`, and the mismatch counter reads zero for ever.
  * Setting the tenant ContextVar from the topic before loading makes
    `with_community_scope` filter by the ATTACKER'S CLAIM: the device is not
    found and the check never runs at all.

STEP 4 IS POLICY, AND IT COMES AFTER IDENTITY: THE SUBSCRIPTION (D-12).

    4. telemetry from a community whose live-data subscription is not active is
       DISCARDED - no write, no dead letter, counted `not_subscribed`

Keyed on the DEVICE ROW's community, never the topic's. A pre-check on the
topic's claim would quietly discard a device of an active community publishing
under a switched-off community's id - a `community_mismatch` the mismatch counter
would never see. Unknown, revoked and mismatched devices are therefore still
dead-lettered whatever the subscription says: those are access-control signals.

Status messages are still PROCESSED. They carry no energy, and dropping them
leaves a device that reconnected while switched off reading OFFLINE for ever
after it is switched back on - its `online: true` would be the message lost.
"""

import datetime
import json
import logging
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from domain import buckets
from domain.protocol import StatusV1, TelemetryV1
from domain.reasons import ObservedCode, RejectReason
from domain.topics import parse_device_topic
from domain.validation import (
    DeviceContext,
    MessageRejected,
    ValidatedBatch,
    ValidationSettings,
    validate_batch,
)
from shared.const import TOPIC_STATUS_SUFFIX, TOPIC_TELEMETRY_SUFFIX, DeviceStatus

logger = logging.getLogger(__name__)

# How much of a rejected payload is kept in ingest_dead_letter. A 64 KB payload
# stored verbatim for every failure during an outage is its own incident.
_DEAD_LETTER_PAYLOAD_CAP = 2048


@dataclass(frozen=True, slots=True)
class IngestOutcome:
    """What happened to one message, for the caller's counters."""

    stored: int = 0
    rejected: list[tuple[RejectReason, str]] = None  # type: ignore[assignment]
    observations: list[tuple[ObservedCode, str]] = None  # type: ignore[assignment]
    # Seconds between the OLDEST accepted reading's `ts` and the moment it was
    # stored. None when nothing was stored.
    #
    # One number per message rather than one per reading, and the oldest rather
    # than the newest, because the question is "how far behind is this device" -
    # a property of the message. A device that reconnects after a week and
    # delivers the week is INDISTINGUISHABLE from a live one in `last_seen_at`,
    # which is the SERVER's receipt clock: both look like they reported seconds
    # ago. This is the only number that separates them.
    oldest_lateness_s: float | None = None
    # Telemetry DISCARDED because the device's community is not subscribed
    # (step 4, D-12). Not a RejectReason - the frozen protocol has thirteen and
    # the SPA renders `last_reject_reason` as one - and not a dead letter: that
    # table is the record of messages that COULD NOT be stored, not of messages
    # the owner asked not to store. When set, nothing else is.
    not_subscribed: bool = False

    def __post_init__(self) -> None:
        if self.rejected is None:
            object.__setattr__(self, "rejected", [])
        if self.observations is None:
            object.__setattr__(self, "observations", [])


_DEVICE_SQL = text(
    """
    SELECT id, id_community, status, max_wh_per_interval, capacity_kva, pure_injection
      FROM device
     WHERE public_id = :public_id
    """
)

# ON CONFLICT (id_device, ts) DO UPDATE on the PARTITIONED PARENT. Legal only
# because the primary key contains the partition key - and it is the whole of
# protocol 4.3's idempotence: a connector unsure whether a message arrived is
# SUPPOSED to re-send it, and re-sending overwrites rather than duplicating.
#
# ----------------------------------------------------------------------------
# THE DIRTY MARK IS PART OF THIS STATEMENT, NOT A SECOND ONE.
#
# A data-modifying CTE, so "a stored measurement always has its bucket marked"
# is structural rather than remembered: there is no ordering to get wrong and no
# branch that can skip it.
#
# MARKED UNCONDITIONALLY, WITH NO GUARD BAND. plan 6.3 says `rollup_dirty`
# carries buckets touched by late data OUTSIDE the recompute window, and that
# reading leaves two holes, both of which mark nothing and lose data silently:
#
#   - a bucket written at 10:59:59 is inside the 48-hour window at write time
#     and outside it when the tick fires at 11:00:02;
#   - a three-day scheduler outage leaves everything ingested during it unmarked
#     and, by the time the scheduler returns, outside the window too.
#
# The cost of marking unconditionally is one speculative INSERT per measurement
# against a table with one row per community-hour, 49 of every 50 of which
# conflict and do nothing. That is why `rollup_dirty` carries an aggressive
# autovacuum setting in schema.sql - it is a queue on the ingest hot path.
#
# The bucket expression comes from `domain.buckets.bucket_sql` and is shared with
# worker/rollups.py. If the two ever disagreed, ingest would mark one bucket and
# the tick would recompute another: the mark would never clear and the chart
# would never correct, with nothing failing anywhere.
# ----------------------------------------------------------------------------
_UPSERT_SQL = text(
    f"""
    WITH stored AS (
        INSERT INTO measurement
            (id_device, ts, id_community, interval_s, import_wh, export_wh, production_wh)
        VALUES
            (:id_device, :ts, :id_community, :interval_s, :import_wh, :export_wh, :production_wh)
        ON CONFLICT (id_device, ts) DO UPDATE SET
            interval_s    = EXCLUDED.interval_s,
            import_wh     = EXCLUDED.import_wh,
            export_wh     = EXCLUDED.export_wh,
            production_wh = EXCLUDED.production_wh,
            received_at   = NOW()
        RETURNING id_community, ts
    )
    INSERT INTO rollup_dirty (id_community, bucket)
    SELECT s.id_community, {buckets.bucket_sql("s.ts")}
      FROM stored s
    ON CONFLICT DO NOTHING
    """  # noqa: S608 - the sole interpolation is domain.buckets.bucket_sql, a constant
)

# device_last, measurement half. The `WHERE ts < EXCLUDED.ts` guard is the
# highest-visibility silent bug available here: without it, a six-hour-old
# reading arriving in a backlog is displayed as the CURRENT value, with a fresh
# timestamp, and nothing anywhere errors.
_DEVICE_LAST_MEASUREMENT_SQL = text(
    """
    INSERT INTO device_last (id_device, id_community, ts, power_w, last_seen_at)
    VALUES (:id_device, :id_community, :ts, :power_w, NOW())
    ON CONFLICT (id_device) DO UPDATE SET
        ts           = EXCLUDED.ts,
        power_w      = EXCLUDED.power_w,
        last_seen_at = NOW()
     WHERE device_last.ts IS NULL OR device_last.ts < EXCLUDED.ts
    """
)

# device_last, liveness half - guarded by the SERVER'S receipt clock, not the
# payload's.
#
# An LWT payload is composed at CONNECT time and sits on the broker until the
# device dies, so its `ts` is OLDER than every online status that followed it.
# Guarding this on the payload clock would make the LWT lose the comparison and
# leave a crashed device showing online for ever - in a row that looks perfectly
# fresh. Receipt-time ordering also makes retained-status replay on resubscribe
# correct rather than harmful.
_DEVICE_LAST_STATUS_SQL = text(
    """
    INSERT INTO device_last (id_device, id_community, status_at, online, diag, diag_since,
                             last_seen_at)
    VALUES (:id_device, :id_community, NOW(), :online, :diag, :diag_since, NOW())
    ON CONFLICT (id_device) DO UPDATE SET
        status_at    = NOW(),
        online       = EXCLUDED.online,
        diag         = EXCLUDED.diag,
        diag_since   = EXCLUDED.diag_since,
        last_seen_at = NOW()
    """
)

# Written ONLY for a device row that actually loaded. Writing it for an unknown
# device id raises a foreign-key violation that aborts the whole ingest
# transaction - which is why `device_unknown` is message-scoped and never
# reaches here.
_DEVICE_LAST_REJECT_SQL = text(
    """
    INSERT INTO device_last (id_device, id_community, last_reject_reason, last_reject_at)
    VALUES (:id_device, :id_community, :reason, NOW())
    ON CONFLICT (id_device) DO UPDATE SET
        last_reject_reason = EXCLUDED.last_reject_reason,
        last_reject_at     = NOW()
    """
)

_CONNECTOR_SQL = text(
    """
    UPDATE device
       SET connector_name = :connector_name,
           connector_version = :connector_version
     WHERE id = :id_device
    """
)

_DEAD_LETTER_SQL = text(
    """
    INSERT INTO ingest_dead_letter (topic, id_device, reason, detail, payload)
    VALUES (:topic, :id_device, :reason, :detail, :payload)
    """
)


async def handle_message(
    session: AsyncSession,
    topic: str,
    payload: bytes,
    now: datetime.datetime,
    settings: ValidationSettings | None = None,
    *,
    active_communities: frozenset[int],
) -> IngestOutcome:
    """Process one MQTT message. Never raises for a bad message.

    The caller wraps this in its own blanket except as a last resort, but every
    KNOWN failure is turned into a dead-letter row here, so that a poison message
    cannot take the connection down - `clean_session=False` would have the broker
    redeliver it on reconnect, and the worker would spin at 100% CPU with the
    heartbeat flapping green. MQTT 3.1.1 has no delivery counter to stop that.

    `active_communities` is keyword-only with NO default, deliberately. A default
    of "everything" would switch step 4 off, silently, for any caller that forgot
    it - and the switch-off it enforces would go back to stopping nothing.
    """
    parsed = parse_device_topic(topic)
    if parsed is None:
        await _dead_letter(
            session, topic, None, RejectReason.SCHEMA_INVALID, "unparseable topic", payload
        )
        return IngestOutcome(rejected=[(RejectReason.SCHEMA_INVALID, "unparseable topic")])

    device_row = (
        await session.execute(_DEVICE_SQL, {"public_id": parsed.device_public_id})
    ).first()
    if device_row is None:
        # A dynsec client exists with no database row behind it - an enrolment
        # that half committed. Distinct from `device_revoked` on purpose: same
        # symptom, opposite repair.
        await _dead_letter(
            session, topic, None, RejectReason.DEVICE_UNKNOWN, str(parsed.device_public_id), payload
        )
        return IngestOutcome(rejected=[(RejectReason.DEVICE_UNKNOWN, str(parsed.device_public_id))])

    id_device = device_row.id
    id_community = device_row.id_community

    if device_row.status == DeviceStatus.REVOKED:
        # The row is revoked and the broker client is still publishing - a
        # revocation that half completed.
        return await _reject_message(
            session,
            topic,
            id_device,
            id_community,
            RejectReason.DEVICE_REVOKED,
            str(parsed.device_public_id),
            payload,
        )

    # THE COMMUNITY CHECK. Load-bearing, not belt-and-braces: the broker's `+`
    # lets a device publish under any community id, and Phase 0 watched one
    # arrive.
    if parsed.claimed_community_id != id_community:
        return await _reject_message(
            session,
            topic,
            id_device,
            id_community,
            RejectReason.COMMUNITY_MISMATCH,
            f"topic claimed {parsed.claimed_community_id}, device is {id_community}",
            payload,
        )

    # STEP 4, never earlier: identity first, policy second (see the module
    # docstring). Keyed on the DEVICE ROW's community - after step 3 it equals
    # the claim, but the row is the authority and the claim is not. Telemetry
    # only: status is still processed.
    if parsed.kind == TOPIC_TELEMETRY_SUFFIX and id_community not in active_communities:
        logger.debug(
            "telemetry discarded: live-data subscription inactive",
            extra={
                "operation": "ingest:not-subscribed",
                "topic": topic,
                "id_community": id_community,
            },
        )
        return IngestOutcome(not_subscribed=True)

    if parsed.kind == TOPIC_STATUS_SUFFIX:
        return await _handle_status(session, topic, id_device, id_community, payload)
    if parsed.kind == TOPIC_TELEMETRY_SUFFIX:
        return await _handle_telemetry(
            session, topic, device_row, id_device, id_community, payload, now, settings
        )
    # parse_device_topic already rejects any other suffix; unreachable.
    return IngestOutcome()


async def _handle_telemetry(
    session: AsyncSession,
    topic: str,
    device_row,
    id_device: int,
    id_community: int,
    payload: bytes,
    now: datetime.datetime,
    settings: ValidationSettings | None,
) -> IngestOutcome:
    try:
        telemetry = TelemetryV1.model_validate_json(payload)
    except Exception as exc:  # pydantic ValidationError, or malformed JSON
        # `extra="forbid"` at the envelope means an unrecognised TOP-LEVEL key
        # lands here too, which is protocol 7's first rule. Unknown keys INSIDE a
        # measurement are the opposite and are handled in validation.
        reason = (
            RejectReason.UNKNOWN_FIELD
            if "extra_forbidden" in str(exc)
            else RejectReason.SCHEMA_INVALID
        )
        return await _reject_message(
            session, topic, id_device, id_community, reason, _short(exc), payload
        )

    cfg = settings or ValidationSettings()
    device = DeviceContext(
        id_device=id_device,
        id_community=id_community,
        max_wh_per_interval=(
            float(device_row.max_wh_per_interval)
            if device_row.max_wh_per_interval is not None
            else cfg.default_max_wh_per_interval
        ),
        capacity_kva=(
            float(device_row.capacity_kva) if device_row.capacity_kva is not None else None
        ),
        pure_injection=bool(device_row.pure_injection),
    )

    result = validate_batch(telemetry, device, now, cfg)
    if isinstance(result, MessageRejected):
        return await _reject_message(
            session, topic, id_device, id_community, result.reason, result.detail, payload
        )

    assert isinstance(result, ValidatedBatch)  # noqa: S101  (narrowing, not a check)
    await _store(session, result, id_device, id_community)

    if result.rejected:
        # The LAST reason wins, which is what a human debugging a device wants:
        # the most recent thing that went wrong, not the first.
        last = result.rejected[-1]
        await session.execute(
            _DEVICE_LAST_REJECT_SQL,
            {"id_device": id_device, "id_community": id_community, "reason": last.reason.value},
        )

    # `measurement.ts` is the END of the interval (see domain/buckets.py), so a
    # reading delivered the instant its interval closes has a lateness of ~0 and
    # never a negative one. A clock-skewed device can still produce a small
    # negative value here; `max(..., 0.0)` keeps it out of the histogram's
    # underflow rather than silently dropping the message from the distribution.
    lateness = None
    if result.accepted:
        oldest = min(m.ts for m in result.accepted)
        lateness = max((now - oldest).total_seconds(), 0.0)

    return IngestOutcome(
        stored=len(result.accepted),
        rejected=[(r.reason, r.detail) for r in result.rejected],
        observations=list(result.observations),
        oldest_lateness_s=lateness,
    )


async def _store(
    session: AsyncSession, batch: ValidatedBatch, id_device: int, id_community: int
) -> None:
    """Upsert the accepted measurements and advance device_last's measurement clock."""
    for measurement in batch.accepted:
        await session.execute(
            _UPSERT_SQL,
            {
                "id_device": id_device,
                "ts": measurement.ts,
                "id_community": id_community,
                "interval_s": measurement.interval_s,
                "import_wh": measurement.import_wh,
                "export_wh": measurement.export_wh,
                "production_wh": measurement.production_wh,
            },
        )

    if not batch.accepted:
        return

    # The LATEST by device clock, not the last in the array: a backlog arrives in
    # whatever order the connector buffered it.
    latest = max(batch.accepted, key=lambda m: m.ts)
    await session.execute(
        _DEVICE_LAST_MEASUREMENT_SQL,
        {
            "id_device": id_device,
            "id_community": id_community,
            "ts": latest.ts,
            "power_w": latest.power_w,
        },
    )


async def _handle_status(
    session: AsyncSession,
    topic: str,
    id_device: int,
    id_community: int,
    payload: bytes,
) -> IngestOutcome:
    """The status topic: retained, QoS 0, and also the Last Will and Testament.

    Half the ingest surface, and easy to leave out. Without it `device_last.online`
    is never true, `connector`/`version` are never written back - protocol 3.2
    promises they are read on EVERY status message - `diag` is always null, and
    revocation's retained-status clear has nothing to clear.
    """
    if not payload:
        # A zero-length retained message: the retained status being CLEARED,
        # which is what revocation publishes. Not an error, and nothing to store.
        return IngestOutcome()
    try:
        status = StatusV1.model_validate_json(payload)
    except Exception as exc:
        return await _reject_message(
            session,
            topic,
            id_device,
            id_community,
            RejectReason.SCHEMA_INVALID,
            _short(exc),
            payload,
        )

    await session.execute(
        _DEVICE_LAST_STATUS_SQL,
        {
            "id_device": id_device,
            "id_community": id_community,
            "online": status.online,
            "diag": status.diag.code.value if (status.diag and status.diag.code) else None,
            "diag_since": status.diag.since if status.diag else None,
        },
    )
    if status.connector or status.version:
        # "Which devices are running the broken 0.3.0?" is a question that only
        # ever gets asked at the worst possible moment.
        await session.execute(
            _CONNECTOR_SQL,
            {
                "id_device": id_device,
                "connector_name": status.connector,
                "connector_version": status.version,
            },
        )
    return IngestOutcome()


async def _reject_message(
    session: AsyncSession,
    topic: str,
    id_device: int | None,
    id_community: int | None,
    reason: RejectReason,
    detail: str,
    payload: bytes,
) -> IngestOutcome:
    """Record a message-scoped rejection: dead letter, and device_last if we can."""
    await _dead_letter(session, topic, id_device, reason, detail, payload)
    if id_device is not None and id_community is not None:
        await session.execute(
            _DEVICE_LAST_REJECT_SQL,
            {"id_device": id_device, "id_community": id_community, "reason": reason.value},
        )
    logger.warning(
        "ingest rejected",
        extra={"operation": "ingest:reject", "reason": reason.value, "topic": topic},
    )
    return IngestOutcome(rejected=[(reason, detail)])


async def _dead_letter(
    session: AsyncSession,
    topic: str,
    id_device: int | None,
    reason: RejectReason,
    detail: str,
    payload: bytes,
) -> None:
    await session.execute(
        _DEAD_LETTER_SQL,
        {
            "topic": topic,
            "id_device": id_device,
            "reason": reason.value,
            "detail": detail[:512],
            "payload": _truncate(payload),
        },
    )


def _truncate(payload: bytes) -> str:
    text_payload = payload.decode("utf-8", errors="replace")
    if len(text_payload) <= _DEAD_LETTER_PAYLOAD_CAP:
        return text_payload
    return text_payload[:_DEAD_LETTER_PAYLOAD_CAP] + "...[truncated]"


def _short(exc: Exception) -> str:
    """A one-line summary of a validation error.

    Pydantic's full error is a JSON document per failure; storing it for every
    message from a misconfigured connector fills the table with the same text.
    """
    message = str(exc).replace("\n", " ")
    return message[:300]


def json_safe(payload: bytes) -> object:
    """Best-effort decode, for logging only. Never used for validation."""
    try:
        return json.loads(payload)
    except (ValueError, UnicodeDecodeError):
        return {"_raw": _truncate(payload)}
