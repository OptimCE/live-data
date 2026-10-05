# live-data — working notes for Claude

OptimCE Live Data annexe: **MQTT telemetry ingestion for smart meters**
(FastAPI + a standalone MQTT worker, async SQLAlchemy 2.0, Mosquitto 2.1.2).

This file is written to be read **on its own**. The submodule's CI checks this
repository out alone, and two of the three protocol implementers never see the
monorepo at all.

## Scope — PHASE 1 IS COMPLETE. All eleven build steps.

Steps 1–11 are built and green, including step 10: the Angular annexe lives in
**`crm-frontend/src/app/features/live_data/`** (a different repository), served
at `/live-data` behind the subscription gate. The catalogue's `minRole` is MEMBER
since D-14: managers get the four admin screens, members their own operations.

**The frontend renders a KEY this service emits.** `/ops/health` and
`/devices/{id}/diagnostics` answer with `hint: "LIVE.HINT.P1_NOT_ENABLED"` and
the SPA owns the words — deliberately asymmetric with errors, which ARE
translated server-side. `domain/device_health.HINTS` is the authoritative key
list, and `crm-frontend/src/app/features/live_data/live-data-format.spec.ts`
asserts every one of them resolves in all four locales. Adding a `DeviceHealth`
member means adding a key there too, or a raw path renders under a broken
device.

**What exists:** the schema (three migrations, schema version 4), the broker and
its dynamic-security roles, the ingest worker, device creation, enrolment,
revocation, the device list, the **ownership projection**, the **rollup tick and
its scheduler container**, the **read API** (`/summary`, `/series`, `/settings`),
the **per-operation rollups and the `/operations` and `/mine` routes** (D-14),
the **forecast seam**, the **ops surface**, and the **subscription gate** on
ingest and the scheduler (D-12).

**What is deliberately absent, and must stay absent:**

| Not here | Why |
|---|---|
| any forecast METHOD; `forecasting/methods_implemented/` is empty | deviation 7 ships the extension point only. A stub would put unvalidated numbers on a screen that says "indicative" and is believed anyway |
| an adapter behind `ports/weather.py` | it arrives WITH the first method, and `httpx` arrives in `worker.txt` at the same moment |
| `httpx` in `base.txt`/`api.txt`/`worker.txt` | nothing here makes an outbound HTTP call. It **is** in `testing.txt` and must stay — fastapi's `ASGITransport` needs it, and `tests/conftest.py` imports it directly. `base.txt` carries a comment saying so; do not "tidy" either one. `tests/test_forecasting.py` asserts its absence |
| `GET /live/{map,my-consent}` | phase 2. `consent`/`consent_event` exist and are INERT |
| the **rotate route** | after the first release. The DTO shapes are frozen (protocol §5.4); `PUBLIC_OPERATIONS` in `api/live_public/routes.py` is `{("/enroll", "post")}` and adding to it is a security decision, not a refactor |
| a member view of the COMMUNITY figures | **D-5 still holds for them.** D-14 lifted it narrowly: a member reads their own sharing operations through `/mine`, at the member floor, and `/summary` and `/series` are manager-only — see below |

A stub is not cheaper than an absence. An endpoint that exists and returns
nothing is one a frontend can call.

### What a member may read (D-14, 2026-10-04 - D-5 lifted, narrowly)

ONLY the `/mine` routes: the sharing operation(s) they hold an ACTIVE meter in
today, its production, and its export where the meters cannot see production -
under k. `/summary`, `/series` and `/forecast` (the COMMUNITY) are manager-only;
so is everything under `/operations`. The member DTO (`MemberOperationPointOut`)
cannot carry import or the shared estimate - the type is the guard.

Criterion 8 ("a member of a community with production visibility disabled gets
403") is asserted on `/mine/operations`, a member-floor route that runs the same
`require_aggregate_visible`. That is why a member-floor read must exist at all:
on a manager-only route the 403 would come from the ROLE GATE whatever the
setting said, and the criterion would assert nothing.

## The decisions this build rests on

Recorded in full in the monorepo's `docs/live-data-decisions.md`; repeated here
because that file is not in this repository.

| | Decided 2026-09-14 |
|---|---|
| **D-1** | **Mosquitto 2.1.2**, pinned by digest. Not 2.0.x — `%u` ACL substitution needs 2.1.0+, and the whole per-device authorisation model is built on it. Moving the pin back would deny every device publish, silently |
| **D-2** | **`/live-public/enroll`**, no `prefix:` key. The two subtrees are then disjoint by construction |
| **D-3** | **`mqtt.optimce.be`** as `BROKER_PUBLIC_HOST`. It reaches a device and is written to NVS |
| **D-4a** | Pin the **ISRG Root X1** chain on the `mqtt.` certificate. Horizon **2030-06-04**, not the 2035 the plan carried |
| **D-4b** | A firmware **MUST embed both** X1 and X2. Was *should*, which a frozen document cannot say about the one thing that makes a device go silent |
| **D-4c** | The `{cert, key}` mTLS shape is frozen, which **forecloses device-generated CSR** for v1. One-call enrolment through a captive portal is the requirement CSR would break |
| **D-5** | The member signal screen is **held**. Phase 1 is administrator-facing |
| **D-7** | No broker in this repository's CI; `scripts/verify-live-ingest.sh` in the monorepo is the other half. And `BROKER_PUBLIC_HOST` ≠ `MQTT_HOST`, always |
| **D-14** | *(2026-10-04)* Per-operation rollups (`rollup_operation_hour/_day`, a REMAINDER row id 0), a per-quarter shared ESTIMATE summed over operations, per-operation k plus complementary suppression and a MIN day gate, members scoped to their own operations via `/mine`. Migration 0003, schema version 4 |
| **D-12** | *(2026-09-23)* Switching Live Data off for a community stops ingesting its TELEMETRY (status is still processed) and, once its pending rollups are drained, the scheduler's work for it; devices, broker credentials and history are kept; readings that arrive while it is off are lost; every `/live` endpoint is 403 `NOT_SUBSCRIBED`, no carve-out. No protocol change — see the trap below |

D-6 (DSO P1 activation for the pilot EANs) is open and is not a software
decision.

## Layout

- `domain/` — pure, session-free, no I/O and **no clock**: `protocol` (the frozen
  wire models), `reasons` (the rejection taxonomy **and its scope**),
  `validation`, `tokens`, `topics`, `partitions`, `buckets` (the hour/day
  algebra), `ownership` (window overlap and membership), `windows` (query
  snapping and the point cap), `kanon` (the k threshold), `device_health`.
- `api/live/` — the authenticated surface. `routes` → `service` (device
  lifecycle) or `read_service` (summary/series/settings/forecast/ops) →
  `repository` + `mappers`/`schemas`/`deps`/`visibility`.
- `forecasting/` — `base`/`registry`/`__init__`, mirroring
  `allocation-key-generation/algorithms/`. No methods.
- `api/live_public/` — **the platform's only unauthenticated endpoint.** Separate
  package, separate router, separate OpenAPI document. See the traps below.
- `ports/` — `broker` (Protocol + fake) / `broker_mqtt` (the real dynsec adapter),
  `crm_read` (three CRM reads: two scalar ones for enrolment, and the unscoped
  active-subscription set), `crm_core` (time-sliced CRM ownership),
  `weather` (a Protocol and nothing else), `providers`.
- `worker/` — TWO entrypoints sharing one image (`Dockerfile.worker`):
  `main` (the MQTT subscribe loop) → `ingest`, and `scheduler_main` →
  `scheduler` → `rollups`/`ownership`/`partitions`/`retention`, with
  `context` holding the advisory-lock helper and `subscriptions` the cached
  set of subscribed communities that both entrypoints read (D-12).
- `scripts/sql/schema.sql` — raw DDL, **no Alembic**. `shared/models/local_models.py`
  mirrors it by hand.
- `scripts/simulate_device.py` — a deliverable, not a fixture. It is how every
  acceptance criterion is demonstrated before a real meter exists.

## The gates: four commands, three tools

```bash
ENV=test python -m pytest -q
python -m ruff check .
python -m ruff format --check .
python -m mypy .
```

`ruff check` and `ruff format --check` are **separate gates** — a `ruff check`
that passes says nothing at all about formatting. Baseline: **959 passed** (2026-10-04, after D-14), and
all three clean.

Do NOT run pytest and `docker compose` at the same time. The suite starts its own
Postgres through pytest-docker, and a concurrent compose command makes the
fixture time out — which surfaces as ~150 unrelated "errors" that look like a
code failure and are not.

On the Windows dev machine that is `.venv/Scripts/python.exe -m …`, and pytest
additionally needs `DOCKER_CONTEXT=desktop-linux` because pytest-docker starts a
throwaway Postgres. CI on Linux needs neither.

`pytest.ini_options` sets `filterwarnings = ["error::DeprecationWarning"]`, so a
deprecation in a dependency fails the suite. That is intentional and has already
caught one.

## The simulator is simplest run inside the stack

Port 1883 is published on the host loopback only (`127.0.0.1:1883`, for
`../live-data-simulator`); the simulator is still simplest run inside the stack.
From the monorepo:

```bash
docker compose -f docker-compose.dev.yml --env-file .env.dev run --rm --no-deps \
  live-data python scripts/simulate_device.py --enroll-token ABCD-... --profile pv-day
```

Profiles: `pv-day`, `backlog --hours N` (store-and-forward and idempotence),
`malformed` (one message per rejection reason), `oversized` (the broker
**disconnects** rather than rejecting — the symptom is a flapping device),
`drift`. `--keep-alive` holds the connection open for the revoke test.
`--username/--password` also needs `--community`.

## Traps

**The scheduler is a THIRD container, and its healthcheck must be overridden in
compose.** `Dockerfile.worker` bakes one reading `/tmp/worker.alive`, which
`scheduler_main` never writes — without the override in `docker-compose.dev.yml`
the container is unhealthy from its first probe onwards while doing its job
perfectly. And its heartbeat is UNGATED, the opposite of the ingest worker's:
there is no dependency whose loss should restart a scheduler.

**Metrics are a no-op in dev, and that is `ENV=local`.** All three containers run
it, `setup_tracer_provider` returns early, and every instrument stays a
`_ProxyCounter` that discards. So `scripts/verify-live-ingest.sh` cannot test any
of this - `tests/test_metrics.py` does, against an `InMemoryMetricReader`. Three
things there are worth knowing before touching `core/metrics.py`:
instruments created at import DO rebind when the provider arrives but nothing
recorded beforehand is replayed; `set_meter_provider` is once-only and merely
WARNS on a second call, which is why the suite's provider is session-scoped and
tests read deltas; and every histogram must declare
`explicit_bucket_boundaries_advisory`, because the SDK's defaults are in
milliseconds and put a 5-second floor under a per-message duration.

**The ingest metrics are emitted in an `else:` clause, deliberately.** Inside the
`try` - where `_log_outcome` used to sit - any instrumentation error is caught as
a database failure: logged as `ingest:error` with a stack trace against a healthy
database, with the outcome it was meant to record lost. It would not reach the
backpressure disconnect as the code stands, because `consecutive_db_failures = 0`
precedes the emission and the count oscillates 0 -> 1 - but that is statement
order, not a safeguard.

**`_scoped()` is the read chokepoint, and `with_community_scope` is not used in
this service any more.** The platform helper answers a missing tenant with
`where(false())`, which for an AGGREGATE means `SUM()` returns NULL and the
summary says the community produced nothing — indistinguishable from night, with
a 200. `tests/test_route_coverage.py` AST-walks the repository and fails on any
method that builds a `select(` without it.

**`measurement.ts` is the END of the interval.** `date_trunc('hour', ts)` moves a
quarter of every hour's energy into the next hour, for ever, silently. The
correct expression lives in `domain.buckets.bucket_sql` and is shared by the
ingest upsert and the rollup tick — if those two ever disagreed, ingest would
mark one bucket dirty and the tick would recompute another, so the mark would
never clear and nothing would fail.

**The shared figure is per QUARTER, then summed - never from hourly sums.**
`LEAST(Σ export, Σ import)` of an operation's devices at one `ts`, in the tick
(`_INSERT_OPERATION_HOUR_SQL`) and in `repository.operation_quarters`. The hourly
form lets a 10:15 surplus cover a 10:45 offtake. And it is SUMMED OVER
OPERATIONS for the community: a community-wide LEAST shares energy between two
operations that cannot share. It is an estimate among monitored meters, not an
upper bound - say so wherever it is shown.

**Each (device, hour) lands in exactly ONE operation row, the remainder (0)
included.** The `attribution` CTE groups the window join back to one row per
device-bucket (`COUNT(w.id) = 1`, else 0) before any energy is summed - rollup
invariant 4 - and energies come from the device hours just written, so a
bucket's operation rows sum to the community hour exactly. That exactness is
what `kanon.community_grid_is_visible` relies on: "is anything outside the
visible operations?" is a lookup on row 0, which MAX device counts cannot answer.

**Next to its operations, the community total is a differencing risk.** Total
minus the visible operations is whatever is hidden. So the community's grid
terms are withheld unless that residual is empty or at least k members, and a
DAY is judged on `n_members_min` (every hour must have passed). Quarters inherit
their hour's verdict. All in `domain/kanon.py`, pure.

**`raw_floor` guards every recompute.** Below raw retention the readings are
gone; recomputing a bucket there DELETES its hour and re-derives its day -
`rollup_community_day`, kept for ever - from nothing. The ownership refresh's
dirty marks, the tick's claims and both backfills all stop at
`retention.raw_floor(now)`.

**Migration 0003 creates tables: apply it as `live_data_svc`.** Created by
`postgres`, they belong to `postgres` and the service cannot write them. Its
partitions MIRROR the device rollups' existing ones (the DO block), or the first
backfill lands history in `*_default` and readiness goes red.

**Production is never subject to k; the grid terms are.** Decided 2026-09-16.
`import_wh`/`export_wh` are ABSENT from the payload below the threshold and named
in `absent[]`; `production_wh` is always published. A term that is withheld must
not be `null` — `null` is what a chart renders as zero.

**A switched-off community's TELEMETRY is DISCARDED at the worker, never
REJECTED.** Decided 2026-09-23 (D-12). `worker/subscriptions.SubscriptionCache`
holds the communities whose `live-data` subscription is active, re-read from the
CRM at most once per `SUBSCRIPTION_CACHE_TTL_SECONDS` (60 s; 5 in the dev
compose, for the verify script). `handle_message` step 4 drops their telemetry,
keyed on the DEVICE row's `id_community`: after the three fixed steps, so an
unknown, revoked or mismatched device is still dead-lettered, and before anything
is written, dead-lettered or counted as a rejection. Status messages are still
processed: dropping them would leave a device reading OFFLINE after the module is
switched back on. It is NOT a `RejectReason`: §4.2 and `domain/reasons.py` must
agree in both directions, and a conformant device has done nothing wrong. It is
`ingest_messages_total{outcome="not_subscribed"}`. If the CRM read fails, the
worker keeps the last set it loaded; until its FIRST successful read it does not
connect to the broker at all, and the broker's persistent session holds the
backlog. `SubscriptionCache.get()` is called OUTSIDE the per-message `try` and
never raises once warm — keep both, or a failed refresh is misread as a database
failure, the same misattribution as the `else:` trap above. The scheduler drains
a switched-off community's pending rollups (dirty buckets, readings inside the
48 h window) and then skips it; the ownership projection runs for the active set
only. The scheduler does NOT wait for its first read the way the worker does: an
unknown set (`None`, logged `scheduler:subscriptions-unavailable`) ticks the
rollups for everyone and makes the ownership refresh refuse (counted `failed`,
retried every tick) — fail-open for rollups on purpose, not a bug to fix.
Partitions, retention and the backfill stay table-wide, and
`rollups.run_tick` stays blind to subscriptions on purpose. Devices and broker
credentials are never touched, so reactivation needs no re-enrolment — and every
`/live` route stays 403 `NOT_SUBSCRIBED` meanwhile, revocation included,
deliberately (`tests/test_route_coverage.py` locks it for every route).

**`ingest_dead_letter` has no `id_community`,** deliberately: a message whose
device could not be identified belongs to no community. Anything reading it per
community must join through `device`.

**...and the one thing that must NOT join through `device` is its prune.** Rows
older than `RETENTION_DEAD_LETTER_DAYS` (90; refused at boot below 1 day or
above `RETENTION_RAW_MONTHS` x 28) are deleted by the nightly retention job -
inside `run_retention`, under `ADVISORY_LOCK_RETENTION`, so a second replica
skips both passes and `job` stays the lock set. It is selected on `received_at`
alone: the NULL-device rows (unparseable topics, unknown devices) are exactly
what a connector publishing for ever writes. Bounded twice - 5,000 rows per
transaction, 100 batches per night, oldest first - because maintenance runs in
the scheduler's one loop; a capped run that leaves rows logs `per-run cap`.
Counted as `dead_letters.pruned.total` and on the nightly `maintenance:` line.
90 days is for a person reading the table late (a gap found at the monthly
billing run); `/ops/health` itself reads 24 h. `tests/test_dead_letter_retention.py`.

**The day is 25 hours once a year and 23 hours once a year.** `day + 24h` and
`bucket::date` are both wrong on those two days, by exactly one hour of community
energy, and neither raises.

**`CURRENT_DATE` is the session's date, and the session is UTC.** `meter_data`'s
dates are Brussels calendar dates, as is the CRM's "today". So a comparison with
them either binds `domain.ownership.local_date_of(now)` from Python
(`find_active_meter`'s `:today`) or converts in SQL with `AT TIME ZONE :tz` (the
rollup and ownership SQL). A bare `CURRENT_DATE` was wrong for an hour or two after
every Belgian midnight, and it raised nothing. It refused a meter the CRM's
Add-device picker offered, with 2404. `tests/test_active_meter.py`.

**`DETACH PARTITION CONCURRENTLY` cannot be used here at all** — Postgres forbids
it on a partitioned table with a DEFAULT partition, and §6.2 requires the
default. `worker/retention.py` uses the plain form under `lock_timeout`.

**The two OpenAPI documents.** `scripts/export_openapi.py --split` writes
`live.json` and `live-public.json`, and asserts the public operation set
**equals** `PUBLIC_OPERATIONS`. Equals, not contains: a new route that lands in
the public document is the failure this guards, and the dangerous direction is
the one a "contains" check would miss. `tests/test_public_surface.py` is the
second lock. The gateway's JWT validator is gated **per service entry**, never
per endpoint, which is why one service needs two entries.

**The `x-user-*` headers still arrive on the public leg.** KrakenD's
`input_headers` is global with no per-service override, so `auth: false` removes
the *producer* of those headers, not the headers. nginx's exact-match
`= /api/live-public/enroll` location blanks all five. Nothing in this repository
can fix that, and `api/live_public/service.py` must never read the ContextVar —
it passes `id_community` explicitly, from the token's device row.

**`MeasurementV1` is `extra="allow"`, `TelemetryV1` is `extra="forbid"`.** Not a
slip. An unknown key inside a measurement is **counted, never rejected** (protocol
§7 — that is what makes adding an optional field safe), and `model_extra` is
`None` under `"ignore"`, so the counter would be blind. An unknown key at the
envelope level *is* a rejection: the sender disagrees with the server about the
message's shape.

**Rejection scope is the design.** `domain/reasons.py` maps every reason to
`MESSAGE` or `MEASUREMENT`. Message-scoped discards everything; measurement-scoped
drops one reading and stores the rest of the batch. A device sending a fortnight
of backlog with one drifted timestamp must not lose the fortnight. `scope_of()`
is an unguarded dict lookup on purpose — a new reason with no scope should raise.

**`aiomqtt` 2.x PUBACKs before application code sees the message.** There is no
ack-after-commit. v3 exposes manual acknowledgement but is alpha **and
MQTTv5-only**, which protocol §1 forbids. The worker disconnects after N
consecutive database failures instead, and the device's own store-and-forward is
the retry.

**`measurement` has a DEFAULT partition**, so a range miss does not raise — rows
land in `measurement_default` and every query keeps working. `/health/readiness`
counts that table, and a non-empty default is what turns the container unhealthy.
Readiness also reads `schema_version`, not `SELECT 1`: the database is created
before any schema is applied, so `SELECT 1` is green over zero tables.

**`createClient` is not idempotent.** The enrolment retry path needs its
already-exists branch (`set_device_password`) — without it, a 3000 ms gateway cut
on a command that succeeded server-side burns exactly the credential the claim
lease exists to protect.

**One dynsec command per publish, never a batch.** A batch runs command 3 after
command 2 has failed, with no rollback.

**Correlate broker responses on `correlationData`.** The response topic is
*broadcast*, and a malformed payload answers with **no** correlationData at all —
which is why the 800 ms timeout is load-bearing rather than defensive.

**Do not add a token-existence check to the enrolment leg.** It is a token
oracle: a distinguishable answer tells an attacker a guessed token was real. An
expired, consumed and unknown token must produce the *byte-identical* response
(2411). `has_live_claim` is narrow for this reason.

**`BROKER_PUBLIC_HOST` is not `MQTT_HOST`.** `MQTT_HOST` is what the API dials
(`mosquitto`); `BROKER_PUBLIC_HOST` is what a device is *told* and writes to NVS.
Collapsing them into one variable would work perfectly in dev and require a site
visit to every device enrolled afterwards.

**kVA, never kWc.** `device.capacity_kva` is the AC injection ceiling snapshotted
from the CRM at creation. A 5 kWc array behind a 3 kVA inverter cannot export
above 3, and `over_device_ceiling` uses this number.

## Testing against the broker

There is **no broker in this repository's CI**, and that is D-7, not an omission:
GitHub Actions creates `services:` containers *before* `actions/checkout`, so a
repo-tracked `mosquitto.conf` can never be their bind-mount source — and this
broker needs one. (None of the eight `tests/docker-compose.test.yml` files in
the monorepo mounts anything, this repo's own included.) The broker adapter is
tested against a fake transport here; the
end-to-end half lives in the monorepo as **`scripts/verify-live-ingest.sh`**
(**122 assertions**: a step-0 precondition and sections A–N, needs the dev stack,
with crm-backend on 127.0.0.1:8089). Step 0 subscribes Test Community through
crm-backend's real endpoint, because every `/live` route is gated and no seed
subscribes it. Sections J–L cover the rollups, the read API and the forecast
seam — none of which this repository's suite can reach, because none of them
exists without a stack. Section M switches the subscription off and on again and
asserts that the telemetry published meanwhile is never stored while the
device's status still is. The sibling services draw the same line for NATS.

## The protocol is frozen

`docs/live-data-protocol.md` in the monorepo is **frozen at v1 (2026-09-14)**.
Three independent implementers code against it from outside this repository, and
a firmware stores the enrolment response in NVS and never asks again. §4.2's
table and `domain/reasons.py` must agree in both directions — names *and* scopes.
A new optional field is allowed; a new required field or a changed meaning is
`v: 2`.

## Handoff: this is a local repository with no commits

`live-data/` is hidden from the monorepo through `.git/info/exclude`, **not**
`.gitignore` — the tracked `.gitignore` must stay clean, because `git submodule
add` silently refuses a path that is ignored, and the workaround (`-f`) commits
the files into the parent instead of adding a submodule. That failure is quiet
and looks like success.

### Order, and why it is an order

1. **Remove the `live-data/` line from the monorepo's `.git/info/exclude`.** First,
   or step 3 fails.
2. **Create the GitHub repository, and make it PUBLIC.** Every sibling is public,
   and the monorepo's submodule-update receiver checks out with `submodules:
   recursive` using a `GITHUB_TOKEN` scoped to the monorepo alone. A private
   submodule here breaks that checkout for **every** annexe, not just this one.
3. **Push `lint.yml` and `test.yml` first.** Both are verified green. Note that a
   first push diffs against the empty tree, so every `paths:` filter matches and
   *all* workflows present would fire at once — which is why the rest wait.
4. **Enable Pages** (Settings → Pages → Source = "GitHub Actions") before
   `update-documentation.yml` runs. `configure-pages` defaults `enablement: false`
   and cannot switch it on with `GITHUB_TOKEN`, so the job dies at that step,
   before Python runs, with an error that does not mention documentation.
5. **Add `secrets.MONOREPO_TOKEN`** — a token with write access to
   `OptimCE/monorepo`. It is the only real secret in the set; everything else uses
   the automatic `GITHUB_TOKEN`.
6. **`git submodule add <url> live-data`** from the monorepo root. **Never `-f`.**
7. **`notify_monorepo_update.yml` last**, strictly after step 6. Fired before the
   gitlink exists, it turns the *monorepo's* job red on every push here while this
   repository's own check stays green. Hand-editing `.gitmodules` is not a
   substitute — the receiver needs a real gitlink (mode 160000).

### The six workflows

All six exist and are linted (`actionlint`, 0 findings). Three are the sibling
file unchanged apart from a comment; three diverge deliberately, each saying why
in its own header:

| | |
|---|---|
| `lint.yml` | the sibling file. Three gates, not two |
| `test.yml` | the sibling file. **No broker service container — that is D-7**, and `conftest.py`'s `if os.getenv("CI")` branch is written for the Postgres one |
| `build.yml` | builds `Dockerfile.production`. Its `paths:` omits `worker/**`, which the siblings list — ours does not ship it |
| `build-worker.yml` | `paths:` omits `api/**`, which the siblings list — our worker image has no `COPY api/` |
| `notify_monorepo_update.yml` | checks the HTTP status. The siblings' bare `curl` exits 0 on a 401, so a dead token reports success |
| `update-documentation.yml` | `--split`, so the two disjoint specs stay disjoint. The single-file path runs none of the split's guards and would publish `/enroll` merged with the admin surface |

### The OSS files

`README.md` plus `docs/README.{fr,de,nl}.md` (heading parity is checked, and the
three lang badges in each resolve), `CHANGELOG.md` (Keep a Changelog, Unreleased
only — no release yet), and `.github/dependabot.yml`.

`CONTRIBUTING.md`, `SECURITY.md` and `NOTICE` arrived as verbatim copies of
administrative-document's and were adapted: CONTRIBUTING told contributors to
clone the wrong repository, and NOTICE was a third-party rights carve-out over
CWaPE forms this service does not ship. **NOTICE now carries a plain Apache
attribution — no sibling except administrative-document has one at all, so
deleting it outright is also defensible and is a licence call, not a code one.**

**Do not merge a Dependabot PR before the repo is wired.** Its own header says
why: every merge pushes to main, `notify_monorepo_update.yml` exits 1 without
MONOREPO_TOKEN, and `update-documentation.yml`'s `github-actions[bot]` guard does
not exclude `dependabot[bot]`, so it re-runs and dies at `configure-pages` until
Pages is enabled. The pip entry also has an acceptance check written into it:
confirm the first job log lists all six `requirements/*.txt`, or switch
`directory` to `/requirements`.

### The env templates

`.env.exemple`, `.env.staging.exemple` and `.env.production.exemple` now exist,
so the reference in `core/config.py` resolves. Each was verified by booting the
real `Settings` against it: local boots as shipped; staging and production fail
naming exactly their blank secrets, and boot once those are filled.

Two things worth knowing before you fill one in, both measured rather than read:

- **The blanks surface one per restart.** `validate_env_config` is a single
  model validator that raises on the first failing check, so production takes
  four boots — `MQTT_ADMIN_PASSWORD`, then `LOGGING_TOKEN`, then
  `LOGGING_LOGS_URL`, then `LOGGING_METRICS_URL`.
- **Its three logging errors say "required for staging/production" but sit under
  `if self.ENV == PRODUCTION`.** Staging boots without them. The message is
  wrong, not the gate. `LOGGING_TRACES_URL` is validated nowhere at all, and
  neither are `MQTT_INGEST_USERNAME` / `_PASSWORD` — the worker simply fails to
  connect to a broker with `allow_anonymous false`, and only its liveness probe
  notices.

**`.env.staging` was missing from `.gitignore`.** `.env.local` and
`.env.production` were listed, so the one file the staging template tells you to
create was the one git would have committed. Fixed here; the four sibling
annexes still have the gap.
