# Contributing to OptimCE — live-data

Thank you for your interest in contributing! Issues and pull requests are
welcome from everyone. By participating in this project, you agree to abide by
our [Code of Conduct](CODE_OF_CONDUCT.md).

This repository is the **live data** microservice of the OptimCE platform. It
ingests quarter-hourly telemetry from smart meters over MQTT, enrols the devices
that send it, and stores the readings. It is one of several repositories under
the [OptimCE organization](https://github.com/OptimCE); the full platform is
assembled in the [monorepo](https://github.com/OptimCE/monorepo).

One thing to know before you start: the MQTT wire format is a **published,
frozen contract** with implementers outside this repository, and a device stores
its enrolment response in flash and never asks again. Changes to payloads,
topics or the enrolment response are governed by §7 of
`docs/live-data-protocol.md` in the monorepo, not by this repository alone.

## Setting Up a Development Environment

This service depends on PostgreSQL and on a **Mosquitto broker with the
dynamic-security plugin enabled** — it creates and deletes broker clients at
runtime, so a plain broker will not do. It uses no NATS and no object store.
The only practical way to run it with its dependencies is the **OptimCE
development stack**, which wires everything together with Docker Compose:

```bash
git clone --recurse-submodules https://github.com/OptimCE/monorepo.git
cd monorepo
./docker-stack.sh start
```

In that stack this service runs as `live-data`, alongside a `live-data-worker`
for MQTT ingestion and a `mosquitto` broker (see the monorepo README).

For working on the service code in isolation, you need **Python 3.12**:

```bash
git clone https://github.com/OptimCE/live-data.git
cd live-data
python -m venv .venv
# Windows: .venv\Scripts\activate  |  Unix: source .venv/bin/activate
pip install -r requirements/testing.txt
cp .env.exemple .env.local
```

Apply the local schema and reference seeds, then start the API:

```bash
psql "$LOCAL_DATABASE_URL" -f scripts/sql/schema.sql
psql "$LOCAL_DATABASE_URL" -f scripts/sql/seeds/0001_wal_deadline_rules.sql
uvicorn main:app --reload
```

It still needs reachable NATS, MinIO, and PostgreSQL instances — the monorepo
stack is the simplest way to provide them.

### A note on the schema

There is no migration runner. `scripts/sql/schema.sql` is the source of truth for
the local database and is mirrored by hand in `shared/models/local_models.py`;
when you change one, change the other and add a forward-only file under
`scripts/sql/migrations/`.

## Reporting Bugs and Suggesting Features

Open a
[GitHub issue](https://github.com/OptimCE/live-data/issues).
For bugs, include what you did, what you expected, and what happened instead —
logs and reproduction steps help a lot.

For security vulnerabilities, **do not open a public issue**; follow the
[security policy](SECURITY.md) instead.

## Submitting Pull Requests

1. Fork the repository and create a feature branch from `main`.
2. Make your changes. Keep each pull request focused on a single topic.
3. Run the checks below and make sure they pass.
4. Open a pull request against `main`, describing **what** you changed and
   **why**.

### Checks Before Opening a Pull Request

These mirror the continuous integration in `.github/workflows/`:

```bash
pytest            # test suite (spins up PostgreSQL via pytest-docker)
ruff check .      # linting
ruff format --check .
mypy .            # type checking
```

Small documentation fixes are welcome as direct pull requests; for larger
changes, opening an issue first to discuss the approach can save you time.

## Commit Messages

Use short, imperative commit messages, preferably following the
[Conventional Commits](https://www.conventionalcommits.org/) style:

```
feat: reject a measurement whose interval is not 900 seconds
fix: tolerate an absent broker client when revoking a device
chore: pin paho-mqtt so a transitive bump cannot fail the suite
docs: record why the enrolment response can never be re-issued
```

## License

This project is licensed under the [Apache License 2.0](LICENSE). By
contributing, you agree that your contributions will be licensed under the same
license.
