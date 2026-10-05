"""Application metrics for the live-data service.

All instruments are created at module import. The OTel API ships a
``_ProxyMeterProvider`` until ``metrics.set_meter_provider(...)`` runs
(inside ``core.tracing.setup_tracer_provider``), and instruments created
against the proxy rebind to the real provider when it is installed —
so import order between this module and tracing setup does not matter.

**What the rebind does NOT do is replay.** A ``.add()`` made before the provider
is installed is discarded silently — not even as a zero — so every entrypoint
must call ``setup_tracer_provider()`` before the first thing worth counting.
Measured against opentelemetry-sdk 1.39.1, not read from the docs.

In LOCAL ``setup_tracer_provider`` returns early and the proxy stays a
no-op, meaning every ``.add(...)``/``.record(...)`` call below becomes
a cheap function dispatch with no side effects. Tests that want to
observe values use ``InMemoryMetricReader`` + a fresh ``MeterProvider``
and re-fetch instruments from that provider.

Naming follows OTel semantic conventions: dotted lowercase, ``.total``
suffix on monotonically increasing counters, units in seconds for
duration histograms, and — the house convention the siblings share —
``.seconds`` in the NAME of a duration histogram as well as in its unit.
The backend translates these to ``health_checks_total`` etc.

--------------------------------------------------------------------------------
EVERY HISTOGRAM HERE DECLARES ITS OWN BUCKETS, AND MUST.

The SDK's default boundaries are ``(0, 5, 10, 25, 50, 75, 100, 250, 500, 750,
1000, 2500, 5000, 7500, 10000)`` — designed for MILLISECONDS. Applied to a
value in seconds they put a 5-second floor under everything: every per-message
duration lands in the first bucket, every percentile is a constant, and the
chart is a flat line that looks like a healthy, very fast system.

``explicit_bucket_boundaries_advisory`` is read by the SDK's default aggregation
and — verified against this pinned version — survives creation against the proxy
provider, which is the only reason it can be declared here at import time.
--------------------------------------------------------------------------------

ATTRIBUTE CARDINALITY IS A HARD CONSTRAINT, NOT A PREFERENCE.

Every attribute below has a value space that is bounded BY A PYTHON ENUM or a
literal tuple in this file. A device id, a community id, an EAN, an MQTT topic
or an exception message would each be unbounded: one time series per distinct
value, retained by the SDK for the process's lifetime and billed by the
collector. The one that is actively dangerous is the topic's claimed community
id — the broker ACL is ``ce/+/%u/telemetry``, so the ``+`` is attacker-chosen,
and labelling by it would let a device mint unbounded series from a basement.
"""

from __future__ import annotations

import time
from collections.abc import Iterable

from opentelemetry import metrics
from opentelemetry.metrics import CallbackOptions, Observation

_meter = metrics.get_meter("live-data")

# One database write per MQTT message, so this is a sub-second distribution and
# the interesting tail is tens of milliseconds, not seconds.
_MESSAGE_DURATION_BUCKETS = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]

# A rollup tick runs per community; ownership crosses a database boundary;
# retention DETACHes under a 5 s lock_timeout. Seconds to minutes, and the
# question worth answering is "is a job starting to approach its interval".
_JOB_DURATION_BUCKETS = [0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0, 900.0]

# Lateness spans a quarter-hour to a month: protocol 4.3 makes store-and-forward
# the NORMAL case and a device that reconnects after a week delivers the week.
# The boundaries are therefore the operationally meaningful spans, not a
# geometric series - one interval, an hour, six hours, a day, three days, a
# week, and INGEST_MAX_AGE_DAYS (35) where the acceptance window ends.
_LATENESS_BUCKETS = [
    # BELOW ONE INTERVAL FIRST. A healthy device publishes within seconds of its
    # interval closing, so starting at 900 would put "on time" and "drifting by
    # fourteen minutes" in the same bucket - and fleet-wide drift under a quarter
    # of an hour would be invisible in every percentile.
    60.0,
    300.0,
    900.0,
    # ...then the spans that matter for a backlog, which protocol 4.3 makes
    # normal traffic: an hour, six hours, a day, three days, a week, and
    # INGEST_MAX_AGE_DAYS (35) where the acceptance window ends.
    3600.0,
    21600.0,
    86400.0,
    259200.0,
    604800.0,
    3024000.0,
]

health_checks = _meter.create_counter(
    name="health.checks.total",
    description="Readiness probe component outcomes",
    unit="1",
)

# ---- ingest worker (build step 3) --------------------------------------------

ingest_messages = _meter.create_counter(
    name="ingest.messages.total",
    description=(
        "MQTT messages the worker finished with, labelled by kind "
        "(telemetry/status/unparsed) and outcome "
        "(stored/empty/rejected/not_subscribed/db_error/dropped)"
    ),
    unit="1",
)

# The CRM read behind `worker/subscriptions.SubscriptionCache`, once per TTL in
# each of the worker and the scheduler. No component label: the resource's
# `component` attribute already separates the two containers.
#
# `failed` is the one to watch. Once the cache is warm a failure is ABSORBED -
# the last set that loaded is kept and nothing else errors - so this counter is
# the only aggregate sign that switching a community off (or on) is not reaching
# the worker at all.
subscription_refreshes = _meter.create_counter(
    name="subscription.refreshes.total",
    description="CRM reads of the live-data subscription set, labelled by outcome (ok/failed)",
    unit="1",
)

ingest_measurements_stored = _meter.create_counter(
    name="ingest.measurements.stored.total",
    description="Individual readings written to `measurement`, across all batches",
    unit="1",
)

# THE COUNTER `domain/reasons.py` HAS ALWAYS POINTED AT. Its docstring described
# a `reason` attribute on exactly this instrument before the instrument existed;
# a later correction replaced that with a claim about an `ingest_reject` TABLE,
# which does not exist either. This is the real thing, and the docstring now
# points here.
#
# `reason` alone, never `reason` x `scope`: `domain.reasons.SCOPES` is a total
# map, so scope is functionally determined by reason and adding it would double
# the series count while carrying no information. 13 series, fixed by the frozen
# protocol - a new reason is a protocol version bump, not a deploy.
ingest_rejections = _meter.create_counter(
    name="ingest.rejections.total",
    description="Rejected messages and readings, labelled by protocol 4.2 reason",
    unit="1",
)

ingest_observations = _meter.create_counter(
    name="ingest.observations.total",
    description="Counted-but-accepted oddities, labelled by ObservedCode",
    unit="1",
)

# The deliberate disconnect after INGEST_DB_FAILURES_BEFORE_DISCONNECT
# consecutive database failures. Its own counter rather than a label, because it
# is the one ingest event that is simultaneously CORRECT BEHAVIOUR and an
# incident: the container stays healthy across it (the heartbeat only misses a
# tick), so nothing else distinguishes a worker doing its job from a worker
# shedding a database outage five already-PUBACKed messages at a time.
ingest_backpressure_disconnects = _meter.create_counter(
    name="ingest.backpressure.disconnects.total",
    description="Broker disconnects taken deliberately to push the backlog back to Mosquitto",
    unit="1",
)

ingest_message_duration = _meter.create_histogram(
    name="ingest.message.duration.seconds",
    description="Wall-clock time from message receipt to commit, per message",
    unit="s",
    explicit_bucket_boundaries_advisory=_MESSAGE_DURATION_BUCKETS,
)

# ONE VALUE PER MESSAGE - the OLDEST reading in the batch, not one per reading.
# The question is "how far behind is this device", which is a property of the
# message; recording all 200 readings of a backlog batch would bury that under
# the batch's own internal spread.
ingest_measurement_lateness = _meter.create_histogram(
    name="ingest.measurement.lateness.seconds",
    description="Age of the oldest reading in a stored batch, at the moment it was stored",
    unit="s",
    explicit_bucket_boundaries_advisory=_LATENESS_BUCKETS,
)

# ---- scheduler (build step 8) ------------------------------------------------

# `job` is the advisory-lock name from `shared/const.py`, so the label space is
# the lock set: rollups, ownership, partitions, retention. `maintenance` is NOT
# a value - `scheduler.run_maintenance` takes no lock of its own and delegates to
# partitions and retention, so counting it as a peer would make one nightly event
# increment three series and report a retention failure as two failed jobs.
scheduler_job_runs = _meter.create_counter(
    name="scheduler.job.runs.total",
    description="Scheduler job attempts, labelled by job and outcome (ok/failed/lock_held)",
    unit="1",
)

scheduler_job_duration = _meter.create_histogram(
    name="scheduler.job.duration.seconds",
    description="Wall-clock time inside a scheduler job, including a skipped lock acquisition",
    unit="s",
    explicit_bucket_boundaries_advisory=_JOB_DURATION_BUCKETS,
)

# The per-community failure `scheduler.run_rollups` swallows deliberately: one
# community's error must not stop the tick for the other forty. That decision is
# right and it is why the failure has no aggregate signal today - the tick logs
# the exception and reports success for the run.
rollup_communities = _meter.create_counter(
    name="rollup.communities.total",
    description="Per-community rollup transactions, labelled by outcome (ok/failed)",
    unit="1",
)

# ZERO IS THE ALARM, not a quiet period. `refresh_ownership` DELETEs a
# community's windows before re-inserting them, so a CRM read that legitimately
# returns nothing - a grant revoked on the live_data_svc role, `meter.id_community`
# nulled, `meter_data.status` moved off ACTIVE - wipes the projection and then
# reports a successful run. This counter is the full size of the recomputed
# projection, so it is a steady positive number whenever any device has an owner.
# ACTIVE communities only (D-12): a switched-off community's projection is left
# as it was and adds nothing here, and the first refresh after it is switched
# back on repairs it.
ownership_windows_written = _meter.create_counter(
    name="ownership.windows.written.total",
    description=(
        "Owner windows written by the projection refresh, summed across active communities"
    ),
    unit="1",
)

# No `table` attribute. `worker/partitions.py` builds CHILD names
# (`measurement_2026_01`), and `worker/retention.py` says outright that the
# naming is "a convention, not a contract" - parsing the parent back out of a
# child name here would re-introduce the fragility that module rejects.
partitions_created = _meter.create_counter(
    name="partitions.created.total",
    description="Monthly partitions attached by the create-ahead job",
    unit="1",
)

partitions_dropped = _meter.create_counter(
    name="partitions.dropped.total",
    description="Partitions detached and dropped by retention",
    unit="1",
)

# The other half of the retention job: `ingest_dead_letter` is not partitioned,
# so its expired rows are DELETEd in bounded batches rather than dropped. ROWS,
# not batches - the batch size is a tuning constant in `worker/retention.py`, and
# a counter of statements would change meaning every time someone tuned it.
#
# It TRAILS the dead-letter rate by exactly RETENTION_DEAD_LETTER_DAYS. A spike
# here is an incident from one window ago, not tonight's; the current rate is on
# `ingest.rejections.total`. Unlike `partitions.dropped.total` it is not
# structurally zero for a year - it moves from the first night a dead letter
# turns the window's age.
dead_letters_pruned = _meter.create_counter(
    name="dead_letters.pruned.total",
    description="Rows deleted from ingest_dead_letter by retention, once older than the window",
    unit="1",
)

# THE PLATFORM-WIDE-OUTAGE PRECURSOR, and the only one of these that should
# normally read zero for ever. A non-zero value means rows landed in a DEFAULT
# partition and had to be moved before the real one could be attached - which is
# the state that, left alone, makes `CREATE TABLE ... PARTITION OF` fail for
# every community at 00:00 UTC on the first of a month.
partition_default_rows_drained = _meter.create_counter(
    name="partition.default.rows_drained.total",
    description="Rows moved out of a DEFAULT partition so the real one could be attached",
    unit="1",
)

# ---- observable: rollup freshness --------------------------------------------

# THE WORST COMMUNITY'S LAG, not the fleet's.
#
# `MAX(bucket)` over the whole table is structurally blind to one frozen
# community among forty - which is the failure `docs/runbooks/live-data.md`
# describes verbatim, and the one where the other thirty-nine keep the fleet
# number healthy. Per-community series would be unbounded cardinality, so the
# instrument carries the WORST value as a single series: it moves when any one
# community stalls, and it cannot name which. `/ops/health` answers that, for a
# manager of that community.
#
# TWO SCOPES, both epoch seconds, both aged by the callback:
#
#   "data" - the newest rolled-up hour for the community FURTHEST behind;
#   "tick" - the most recent recompute, fleet-wide, from `computed_at`.
#
# A row exists for a bucket only if measurements landed in it, so "data" climbs
# when a meter dies just as readily as when the scheduler stops - and a dead
# meter is the commonest incident in the service. "tick" moves whenever the job
# runs at all, so the pair separates the two. Alert on "tick"; diagnose with
# "data".
#
# THE INSTANT, NOT THE LAG. Epoch seconds.
#
# Storing the lag froze the gauge at its last healthy value EXACTLY WHEN ROLLUPS
# STOPPED: the tick is the only writer, so a tick that stops writing leaves the
# callback re-exporting "4 minutes behind" for the life of the process. The one
# failure the gauge exists for would have rendered as a flat, healthy line.
#
# Storing the instant and subtracting at read time makes it self-ageing: the
# callback runs on every collection cycle, so the number climbs in real time from
# the moment the tick stops, with no writer at all.
#
# Written by the rollup tick (`worker/scheduler.run_rollups`) and read on the
# exporter's own thread. A plain dict for exactly that reason: the callback is
# synchronous and cannot await a database round trip.
rollup_newest_bucket_epoch: dict[str, float] = {}


def _rollup_lag_callback(options: CallbackOptions) -> Iterable[Observation]:
    """Age of the newest bucket, computed NOW rather than when the tick ran.

    An empty dict yields nothing rather than a zero, because zero here would read
    as "perfectly fresh" for a scheduler that has never run - and an absent series
    is what an alert on staleness should fire on.
    """
    now = time.time()
    return [
        Observation(max(now - epoch, 0.0), {"scope": scope})
        for scope, epoch in rollup_newest_bucket_epoch.items()
    ]


_meter.create_observable_gauge(
    name="rollup.lag.seconds",
    callbacks=[_rollup_lag_callback],
    description=(
        "Rollup freshness. scope=data: the worst community's newest hour. "
        "scope=tick: the last recompute"
    ),
    unit="s",
)
