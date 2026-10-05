# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Nothing is released yet. **Phase 1 is complete — all eleven build steps.** The
Angular annexe (step 10) lives in the `crm-frontend` repository under
`src/app/features/live_data/`; everything else is here.

Note that the service's version and **the MQTT wire protocol's version are not
the same thing**. `docs/live-data-protocol.md` in the OptimCE monorepo is frozen
at `v1`, and it is versioned by the rule in its own §7: a new optional field is
allowed within `v1`, while a new required field or a changed meaning is `v: 2`.
A major release here does not imply a protocol bump, and a protocol bump would
not be expressible as one — devices in basements have no idea what version this
service is.

### Added

- **The administrator screens** (in `crm-frontend`): a dashboard with the
  production and export chart, the consumption drawn from the grid, the shared
  estimate per sharing operation and the absent-term notices, a devices list
  carrying the P1-not-enabled hint inline, a fleet-status page reporting how
  stale the rollups are, and the visibility settings panel. A member gets a view
  of their own sharing operations instead.
- **The read API.** `GET /summary`, `GET /series` (quarter, hour and day
  resolutions) and `GET`/`PUT /settings`, all at MANAGER since D-14; a member
  reads only their own sharing operations, through `/mine` (below). Every
  payload is stamped `indicative: true` server-side.
- **Hourly and daily rollups**, recomputed by a 15-minute tick over an aligned
  48-hour window plus anything marked in `rollup_dirty`. Late data is the norm,
  not the exception: a device with store-and-forward reconnects after a week and
  delivers the week.
- **A third container, `live-data-scheduler`**, running the rollups, the
  ownership projection, partition create-ahead and retention. It shares the
  worker's image and nothing else — its heartbeat is ungated where the ingest
  worker's is deliberately tied to the broker connection.
- **The CRM ownership projection** (`device_owner_window`), resolving which
  member held a meter at a given instant. Overlapping windows are flagged rather
  than refused: their energy counts, their membership does not.
- **k-anonymity on community aggregates.** Suppressed per bucket on `n_members`,
  never on `n_devices`. Production is always published; the grid-exchange terms
  are withheld below the threshold and named in an `absent` list rather than
  nulled.
- **Sharing operations (D-14).** Per-operation hourly and daily rollups
  (`rollup_operation_hour`/`_day`), with a remainder row (id 0) so that a
  bucket's operation rows sum to the community exactly. Managers read them
  through `GET /operations`, `/operations/{id}/summary` and
  `/operations/{id}/series`; members through `GET /mine/operations` and
  `/mine/operations/{id}/series`, which cover only the operations they hold an
  active meter in today and carry production and, under k, export - never
  import or the shared figure. The shared figure is an estimate among monitored
  meters: `LEAST(Σ export, Σ import)` per quarter within an operation, summed
  over operations, never derived from hourly sums. k applies per operation, the
  community total is withheld unless what it reveals beyond the visible
  operations is empty or at least k members, and a day is judged on its
  least-populated hour (`n_members_min`) - all in `domain/kanon.py`, pure.
- **The forecasting extension point** — a method registry mirroring
  `allocation-key-generation/algorithms/`, and a weather Protocol. No method
  ships, and `GET /forecast` answers with an empty series and a named reason.
- **An ops surface.** `GET /ops/health` and `GET /devices/{id}/diagnostics`,
  including a third device state for "connected and reporting zeros", which is
  what a meter whose P1 port the grid operator has not activated looks like.
- **Three schema migrations** (`0001` to `0003`; schema version 4) and
  `tests/test_schema_migration_parity.py`, which builds two databases — one from
  `schema.sql`, one from schema plus migrations — and compares their catalogs.
  `0003` creates tables: apply it as `live_data_svc`, or the service cannot
  write them.
- **Telemetry ingestion over MQTT 3.1.1.** A standalone worker subscribes to
  `ce/{community}/{device}/telemetry`, validates against the frozen protocol and
  writes to a monthly-partitioned `measurement` table. Idempotent on
  `(device, ts)`: re-sending a measurement overwrites it, which is what makes
  QoS 1 sufficient and store-and-forward safe.
- A rejection taxonomy with **scope**. Thirteen reasons, each classified as
  discarding the whole message (envelope and identity faults) or a single
  reading (value and time faults), so a fortnight of backlog carrying one
  drifted timestamp loses the timestamp and not the fortnight. Message-scoped
  rejections are dead-lettered; measurement-scoped ones land on
  `device_last.last_reject_reason`.
- **One-time-token enrolment.** `POST /live-public/enroll` — the platform's only
  unauthenticated endpoint — exchanges a 128-bit Crockford base32 token for
  broker credentials, once. The token alphabet maps the read-aloud confusions
  (`I`, `L` → `1`; `O` → `0`) so a code dictated over the phone into a captive
  portal works.
- **Device administration** for community managers: list, create, re-issue a token,
  and revoke. Creating a device validates its EAN against the CRM and snapshots
  the meter's capacity in **kVA** — the AC injection ceiling, never kWc.
- **Revocation that is immediate and complete.** The broker client is disabled, the
  retained `status` message is cleared, and the client is deleted. A revoked
  device is disconnected within milliseconds and its reconnection refused.
- **Two disjoint OpenAPI documents**, `live.json` and `live-public.json`, generated
  in one pass. The gateway gates its JWT validator per service entry rather than
  per endpoint, so one service with one public route needs two entries and two
  specs; a guard asserts the public operation set equals the literal declared in
  `api/live_public/routes.py`.
- `scripts/simulate_device.py`, a connector simulator with profiles for a PV
  day, a store-and-forward backlog, one message per rejection reason, an
  oversized payload and clock drift.

- **Metrics from the ingest worker and the scheduler.** Fifteen instruments in
  `core/metrics.py` where there was one: message outcomes and per-reason
  rejections, measurement lateness, the deliberate backpressure disconnect, job
  runs and durations per scheduler job, per-community rollup failures, ownership
  windows written, partitions created and dropped, rows drained from a DEFAULT
  partition, and a gauge for the lag of the community furthest behind. Every
  attribute's value space is bounded by an enum or a literal; every histogram
  declares its own bucket boundaries, because the SDK's defaults are in
  milliseconds and would put a 5-second floor under a per-message duration.
- `docs/runbooks/live-data.md` gained a **What to alert on** table saying what a
  change in each series means, and that none of it works in dev.

### Changed

- `_scoped()` replaces `with_community_scope` for every read in this service. The
  platform helper answers a missing tenant with no rows, which for an aggregate
  means a summary reporting that the community produced nothing.
- `/health/readiness` checks the DEFAULT partition of every registry table, not
  just `measurement`'s.
- The measurement upsert marks `rollup_dirty` in the same statement, so a stored
  reading always has its bucket queued for recompute.

### Fixed

- **A new device kept its community's grid terms withheld for up to 75
  minutes.** The ownership projection ran hourly, and after the rollups, so the
  device's first hours counted no member and fell below k. The scheduler now
  refreshes ownership on the first tick after a new (community, EAN) pair, and
  runs it before the rollups.
- **Recomputing below raw retention erased history.** A dirty mark or a tick
  claim older than the raw readings recomputed its bucket from nothing:
  deleting the hour and re-deriving `rollup_community_day`, which is kept for
  ever, from no data. Every recompute path now stops at `retention.raw_floor`.
- **`/summary.power_w` published export without k.** For net meters, which
  cannot see production, the instantaneous power is the export, and it is now
  published only where the community's grid terms for that hour are visible.
- **For an hour or two after every Belgian midnight, the EAN check used
  yesterday's date.** It compared the meter's CRM window with `CURRENT_DATE`, the
  database session's date (UTC), while the CRM and the Add-device meter picker it
  feeds use the Brussels date. A meter whose window started that day was offered
  by the picker and refused with 2404 `EAN_NOT_FOUND`, and one whose window had
  ended the day before was still accepted. The check now uses the Brussels-local
  date of the request.
- **`ingest_dead_letter` grew for ever.** Nothing pruned it, so a connector that
  kept publishing what the worker cannot store - misconfigured, or revoked with
  its broker client still alive - added a row every 15 minutes for as long as it
  had power. The nightly retention job now deletes rows older than
  `RETENTION_DEAD_LETTER_DAYS` (90 days; refused at boot below 1 or above
  `RETENTION_RAW_MONTHS` x 28, so a rejected reading never outlives the accepted
  ones), under the retention advisory lock, in batches of 5,000 rows that each
  commit on their own and at most 100 batches a night. Counted as
  `dead_letters.pruned.total`, and reported on the nightly `maintenance:` line.
- **The Ops tab blamed the scheduler for a quiet fleet, and said "recomputed
  null minutes ago" on a new community.** "Recomputed N minutes ago" was the age
  of the newest DATA bucket, which a quiet fleet ages while every tick runs; and
  `/ops/health` sent unanswered fields as `null` where every other read route
  omits them. `/ops/health` and `/summary` now carry `rollup_freshness`
  (`fresh`/`stale`/`idle`/`never`, `domain/rollup_freshness.py`) and
  `rollup_lag_minutes`, judged from `computed_at` and the pending `rollup_dirty`
  marks - no new table. `/ops/health` and `/devices/{id}/diagnostics` are
  `response_model_exclude_none`, as `DeviceStatusOut` always documented. The
  dashboard's stale banner reads the same verdict, so the 30-day range no longer
  shows it permanently.
- **The "last full hour" production figure was the hour in progress** for most of
  every hour. `/summary` now reads the newest hour the tick recomputed after it
  ended; a community whose first hour has not closed says
  `no_closed_hour_yet`, not `not_measured`.
- **A revoked device read silent a day later and needed attention for ever** - and
  its frozen `last_seen_at` counted as fleet evidence, so one revoked device plus
  one dead meter read as an ingest outage and hid the meter. `classify` now takes
  the device `status` (required) and answers `revoked`; revoked devices stay listed
  but are no longer evidence for the ingest verdict.
- **Rejected readings were invisible.** A reading dropped from a stored batch
  leaves no dead letter. `/ops/health` now counts the devices it happened to in
  the last 24 h (`n_devices_readings_rejected_24h`), and the Devices tab shows each
  device's last rejection.
- **A community whose Live Data module was switched off kept being ingested and
  rolled up, for ever.** The API answered 403 and enrolment was refused, but the
  worker never read the subscription and the scheduler drove its jobs from the
  device table. The worker now discards a switched-off community's telemetry
  (the subscribed set is cached and refreshed about once a minute,
  `SUBSCRIPTION_CACHE_TTL_SECONDS`); its status messages are still processed.
  The scheduler drains the rollups already owed, then skips the community, and
  runs the ownership projection for subscribed communities only. Devices, broker
  credentials and history are kept, so switching it back on resumes ingestion
  without re-enrolment; readings that arrive while it is off are lost. The
  discard is its own message outcome, `not_subscribed`, not a rejection reason -
  the frozen protocol is unchanged. Decision D-12.
- **Blank `LOGGING_*` URLs started an exporter aimed at `localhost:4318`.** The
  config guard enforces the triple only under PRODUCTION and the staging template
  says leaving them blank "boots cleanly" - which it did, while each of three
  containers ran a 15-second export loop and an OTLP log handler against the
  exporter's default endpoint. `setup_tracer_provider` now returns early, with a
  warning, whenever there is nowhere to send.
- **The OTLP exporters had an 83-minute timeout** (below), and
  `scheduler.job.duration.seconds` folded lock-skipped runs - which return in
  microseconds - into the same distribution as the work. It now carries `outcome`.
- **`ingest.measurement.lateness.seconds` started at one full interval**, so a
  device on time and one drifting by fourteen minutes shared a bucket and
  fleet-wide drift under a quarter hour moved no percentile.
- **Two schedulers exported one stream identity.** The container is deliberately
  not pinned to one replica; the resource now carries `service.instance.id`.
- **A device could stop ingestion for the whole platform with one publish.**
  `parse_device_topic` gated on `str.isdigit()` and then called `int()`, which
  disagree on 128 code points - `"²".isdigit()` is true and `int("²")`
  raises. The community level is the `+` in the broker ACL `ce/+/%u/telemetry`,
  so a device chooses it, and the same ACL permits a RETAINED status publish that
  is redelivered on every reconnect. The parser now accepts only the canonical
  decimal spelling, which also makes the mapping injective: `ce/007/...` and
  `ce/١/...` used to resolve to the same community as `ce/7/...`.
- **The OTLP exporters had an 83-minute timeout.** `EXPORTER_TIMEOUT_MS = 5000`
  was passed to constructors that take SECONDS, so a collector that accepted a
  connection and hung would hold the export thread for the afternoon. Both now
  take `EXPORTER_TIMEOUT_SECONDS`.
- **The rollup-lag gauge froze at its last healthy value** exactly when the tick
  stopped - the tick being its only writer. It now publishes the newest bucket's
  INSTANT and the callback subtracts the clock, so the number climbs by itself
  from the moment the tick stops.
- **The scheduler exported no telemetry at all.** `worker/scheduler_main.py`
  never called `setup_tracer_provider()`, so in staging and production the third
  container installed neither a meter provider nor the OTLP log handler - it was
  invisible under every name, not merely the wrong one.
- **The shutdown flush was unbounded.** The SDK's own `atexit` handler does
  export a final batch, but with a 30-second budget against Docker's 10-second
  stop grace - so against a slow collector the container is SIGKILLed mid-flush
  and every deploy takes the full grace period. Both entrypoints now flush
  explicitly within 5 seconds.
- **All three containers reported one identity.** They now carry a `component`
  resource attribute (`api`, `ingest-worker`, `scheduler`), so "ingest stopped"
  and "the API is down" are no longer the same absence on the same series.
- **`domain/reasons.py` named a table that does not exist.** It described the
  rejection count as living in `ingest_reject`, which is in none of the schema's
  21 tables - a correction to an earlier claim about a counter that did not exist
  either. The counter exists now and the docstring names it.
- **The fleet-wide ingest-outage verdict was never wired.** `classify()` had the
  `ingest_healthy` switch from the start and no caller passed it, so
  `DeviceHealth.UNKNOWN` was unreachable, the runbook's `unknown` triage row
  described a state that could not occur, and the test covering it exercised a
  path nothing reached. `domain.device_health.ingest_looks_healthy` now produces
  the verdict from the fleet — one meter going quiet is a meter, every meter
  going quiet at once is the path they share — and both `/ops/health` and
  `/devices/{id}/diagnostics` read through it.
- **`MQTT_TLS` reached no socket.** Neither MQTT client passed `tls_params`,
  while both the staging and production templates ship `MQTT_TLS=true` against a
  broker that terminates TLS on 8883 — so the first staging deploy would have
  failed at the handshake, with an error naming nothing in this repository.
- **`INGEST_DEFAULT_MAX_WH_PER_INTERVAL` reached no code**: `worker/ingest.py`
  carried the number as a literal, so lowering the setting changed nothing and
  `schema.sql`'s comment about it was false.
- **Telemetry was attributed to another service.** `core/tracing.py` shipped
  `service.name = "administrative-document-backend"`, copied from the sibling
  template and only ever evaluated when `ENV != local` — so no dev run and no
  test could have shown it. `AUDIT_LOG_DEFAULT_SOURCE` carried the same foreign
  value into the CRM's shared `audit_log` table.
- **Every unhandled 500 answered in French**, in all four locales: the handler
  read `translate(...) if False else "Erreur interne du serveur"`, inherited from
  the template, with `ERRORS.INTERNAL` present in no locale file at all.
- `scripts/verify-krakend-public-surface.py` **did not exist**, although two
  files cited it as the third gate on the public surface. It exists now, in the
  monorepo, and runs inside `scripts/verify-live-ingest.sh`.
- A device that was created and never enrolled could not be revoked. It has no
  broker client, dynsec answers `Client not found.`, every broker error became a
  502, and the row stayed `PENDING` — with its EAN locked for ever, because the
  partial unique index excludes only revoked rows. A mistyped meter was
  unrecoverable without editing the database by hand.
