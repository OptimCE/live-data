<p align="center">
  <img src="docs/logo.svg" alt="OptimCE logo" width="160">
</p>

# OptimCE — Live Data

[![Website](https://img.shields.io/badge/Website-optimce.be-2e7d32.svg)](https://www.optimce.be/en/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![en](https://img.shields.io/badge/lang-en-43a047.svg)](README.md)
[![fr](https://img.shields.io/badge/lang-fr-lightgrey.svg)](docs/README.fr.md)
[![de](https://img.shields.io/badge/lang-de-lightgrey.svg)](docs/README.de.md)
[![nl](https://img.shields.io/badge/lang-nl-lightgrey.svg)](docs/README.nl.md)

Quarter-hourly telemetry from smart meters, over MQTT.

A member's meter data reaches OptimCE today through the distribution system
operator, months late and in a monthly file. That is enough to bill on and
useless for anything else: a community cannot see whether its solar production
is being consumed locally *now*, and a member cannot be told that running the
dishwasher in the next hour is free. This service is the other path — a
connector in the meter cupboard publishing readings as they happen.

It is an **annexe**: it owns its own database, reads the central CRM read-only,
and is assembled with the rest of the platform in the
[monorepo](https://github.com/OptimCE/monorepo).

> **Scope.** Phase 1 is complete — all eleven build steps: ingestion,
> enrolment, device administration, the ownership projection, the rollups and
> their scheduler, the read API, the ops surface and the forecasting *seam*.
> **No forecasting method ships**, deliberately: `forecasting/` is the extension
> point and `forecasting/methods_implemented/` is empty. See
> [CHANGELOG.md](CHANGELOG.md).

## The protocol is a frozen, published contract

Most of this repository can be changed freely. The wire format cannot.

`docs/live-data-protocol.md` in the monorepo is **frozen at `v1`**. Three
independent connectors implement it from outside this repository, and a firmware
stores its enrolment response in flash and never asks again — there is no
remote update for a box in a basement. §7 of that document governs what may
change: a new *optional* field is allowed within `v1`; a new required field, or a
changed meaning for an existing one, is `v: 2`.

The rejection taxonomy here and §4.2 there must agree in both directions, names
and scopes. If you change one, change the other.

## How a reading arrives

A connector publishes to two topics, and subscribes to nothing:

```
ce/{community_id}/{device_id}/telemetry    QoS 1
ce/{community_id}/{device_id}/status       QoS 0, retained, also the Last Will
```

The flow is one-way by design. There is no command topic and no acknowledgement
a connector can read, which is what lets it run on hardware with no return
channel — and why the rejection counters below exist.

A telemetry message carries up to 200 measurements in one array, so a device
that has been offline sends its backlog in a single publish. Storage is
idempotent on `(device, ts)`: **re-sending a measurement overwrites it**. That
promise is what makes QoS 1 sufficient and makes "I am not sure that arrived,
send it again" the correct behaviour for a connector rather than a risk.

## Rejections have a scope

A malformed message is rejected silently, counted under a reason, and logged.
Every reason is classified by what it discards:

| Scope | Discards | Reasons |
|---|---|---|
| message | everything in the publish | `schema_invalid`, `unknown_field`, `batch_too_large`, `duplicate_ts_in_batch`, `device_unknown`, `device_revoked`, `community_mismatch` |
| measurement | one reading; the rest of the batch is stored | `ts_in_future`, `ts_too_old`, `ts_not_aligned`, `negative_energy`, `over_device_ceiling`, `implausible_production` |

The split is the point. A fortnight of backlog containing one reading with a
drifted clock must lose the reading, not the fortnight. Message-scoped
rejections are written to `ingest_dead_letter`; measurement-scoped ones land on
`device_last.last_reject_reason`.

Two conditions are counted but never rejected: an interval carrying both an
import and an export (ordinary on a cloudy afternoon), and an unrecognised key
*inside* a measurement — §7 forbids rejecting those, which is what makes adding
an optional field safe.

## Enrolment

A device is created by a community manager, who gets a short code. Someone at
the meter types that code into the connector, once:

```
POST /live-public/enroll     { "token": "…", "connector": { … } }
-> { "broker": {…}, "credentials": {…}, "topics": {…} }
```

This is the platform's **only unauthenticated endpoint**. The token is 128 bits
of CSPRNG output in Crockford base32, stored as a SHA-256 hash and usable once.
The alphabet maps the read-aloud confusions — `I` and `L` to `1`, `O` to `0` —
because the realistic case is a code dictated over the phone into a captive
portal on a cold evening.

**The password is shown once and is never recoverable.** OptimCE does not store
it; the broker keeps only a hash. A lost secret means re-enrolment, which is why
the unique index over a community's meters is partial — a revoked device frees
its EAN.

An expired token, a consumed token and a token that never existed produce the
*identical* response. A distinguishable answer would tell an attacker that a
guess was real.

## Revocation

Immediate and server-side: the broker client is disabled, the retained `status`
message is cleared, and the client is deleted. A revoked device is disconnected
within milliseconds and its reconnection refused. There is no device-side action
and no notification — from the connector's point of view its credentials simply
stop working.

Clearing the retained status is the step that is easy to omit and expensive to
omit: a `status` that outlives its device replays to the ingest worker on every
reconnect, so the offline alert fires on every deploy until the team stops
reading it.

## Endpoints

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/live/version` | build and schema version |
| `GET` | `/live/devices` | the community's devices |
| `POST` | `/live/devices` | create one — validates the EAN against the CRM |
| `POST` | `/live/devices/{id}/token` | issue an enrolment code (invalidates any unused one) |
| `POST` | `/live/devices/{id}/revoke` | revoke |
| `GET` | `/live/devices/{id}/diagnostics` | why a device is quiet — health, `diag`, connector |
| `GET` | `/live/summary` | the community's current signal. **Member floor** |
| `GET` | `/live/series` | a bucketed series. **Member floor** |
| `GET` | `/live/settings` | visibility and k. Returns defaults without writing |
| `PUT` | `/live/settings` | replace them |
| `GET` | `/live/forecast` | empty **with a named reason** until a method ships |
| `GET` | `/live/forecast/methods` | the registry, currently empty |
| `GET` | `/live/ops/health` | the fleet, and how stale the rollups are |
| `POST` | `/live-public/enroll` | **public.** Exchange a code for credentials |

`/summary` and `/series` sit at the MEMBER floor on purpose. Manager-only would
mean "a member of a community with production visibility disabled gets 403" was
answered by the role gate for every member whatever the setting said — and the
acceptance criterion would pass against a service with no visibility logic at
all.

### Authentication

Everything under `/live` is authenticated at the gateway, which validates the
JWT and forwards `x-user-id`, `x-community-id` and `x-user-orgs`. The service
trusts those headers and scopes every query to the caller's community; the
feature is additionally gated on an active community subscription. While that
subscription is inactive the worker also discards the community's telemetry
(status messages are still processed) and the scheduler skips it once its
pending rollups are done; devices and history are kept, so switching it back on
needs no re-enrolment.

`/live-public/enroll` is the exception and is reached through a separate gateway
entry with no validator. The gateway cannot strip the trust headers for that
route alone, so the reverse proxy blanks them before the request arrives — a
device holds an enrolment token, never a session.

## Configuration

| | |
|---|---|
| `CRM_DATABASE_URL` | the central CRM, read-only |
| `LOCAL_DATABASE_URL` | this service's own database |
| `SUBSCRIPTION_CACHE_TTL_SECONDS` | how long the worker and the scheduler trust their copy of which communities are subscribed (default 60, 1–3600) |
| `MQTT_HOST`, `MQTT_PORT`, `MQTT_TLS` | the broker **this service dials** |
| `MQTT_ADMIN_USERNAME` / `_PASSWORD` | the dynamic-security control connection |
| `MQTT_INGEST_USERNAME` / `_PASSWORD` | the worker's subscriber identity |
| `BROKER_PUBLIC_HOST`, `_PORT`, `_TLS` | the address a **device is told** |

The last two rows are two different things and must stay that way. A device
writes `BROKER_PUBLIC_HOST` to flash and never asks again, so collapsing them
into one variable would work perfectly in development and require a visit to
every device enrolled afterwards.

Ingest thresholds (`INGEST_MAX_FUTURE_SECONDS`, `INGEST_MAX_AGE_DAYS`,
`INGEST_MAX_BATCH`, …) default to the values the frozen protocol states. Changing
one changes what conformant connectors are told, so change the protocol first.

## Running it

The broker is not optional and not ordinary: Mosquitto with the
**dynamic-security plugin**, because this service creates and deletes broker
clients at runtime. Use the development stack, which wires the broker, both
databases and the gateway together:

```bash
git clone --recurse-submodules https://github.com/OptimCE/monorepo.git
cd monorepo
./docker-stack.sh start
```

The API is then on <http://localhost:8008>, with a `live-data-worker` container
alongside it consuming MQTT.

There is a connector simulator for working without hardware. The broker's port
is published on the host loopback only, but the simulator is simplest run inside
the stack:

```bash
docker compose -f docker-compose.dev.yml --env-file .env.dev run --rm --no-deps \
  live-data python scripts/simulate_device.py --enroll-token ABCD-… --profile pv-day
```

Profiles cover a PV day, a store-and-forward backlog, one message per rejection
reason, an oversized payload and clock drift.

## Tests

```bash
pytest                    # needs a Docker PostgreSQL on port 5433
ruff check .
ruff format --check .
mypy .
```

`ruff check` and `ruff format --check` are separate gates: a 110-character line
is a lint error that the formatter considers already formatted, and mismatched
quoting is the reverse.

**No test contacts a broker, and that is deliberate.** GitHub Actions creates
service containers *before* checking the repository out, so a repo-tracked
`mosquitto.conf` can never be their bind-mount source — and without that config
the broker has no dynamic security at all. The broker adapter is therefore
tested against a fake transport here, and the half that needs a real broker
lives in the monorepo as `scripts/verify-live-ingest.sh`: it asserts that a
revoked device is genuinely disconnected, that a retained status is genuinely
cleared, and that a re-sent backlog genuinely overwrites.

## Schema

`scripts/sql/schema.sql` is raw DDL for a FRESH database; `scripts/sql/migrations/`
holds the numbered, forward-only, re-runnable files for a live one. There is no
Alembic and no runner — migrations are applied by hand or by `provision.sh`, in
name order. The same statements live in both places, and
`tests/test_schema_migration_parity.py` provisions one database from each and
compares normalised catalog snapshots, which is what keeps them equal.
`shared/models/local_models.py` mirrors the schema by hand, so a change to one
is a change to both. `measurement` is range-partitioned by month with a DEFAULT
partition; a non-empty default means the partition job has stopped, and
`/health/readiness` reports it.

## Contributing

Contributions are welcome! Please read the
[contributing guidelines](CONTRIBUTING.md) and our
[Code of Conduct](CODE_OF_CONDUCT.md) before opening an issue or pull request.

## Security

To report a security vulnerability, please follow the
[security policy](SECURITY.md) — do not open a public issue.

## License

This project is licensed under the [Apache License 2.0](LICENSE).
