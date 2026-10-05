"""Liveness and readiness.

Never reaches the gateway: `scripts/export_openapi.py` drops every path starting
`/health`, so KrakenD has no route to any of these. The compose healthcheck hits
`http://localhost:8000/health/readiness` directly.

----------------------------------------------------------------------------
TWO DELIBERATE DIFFERENCES FROM THE SIBLING ANNEXES.

1.  `check_local_db` READS `schema_version`. It does not run `SELECT 1`.

    `postgres/provision/10-databases.sql` creates `live_data_local` BEFORE any
    schema is applied, and `provision.sh` applies a schema only when the database
    has no relations — logging "schema skipped" and exiting 0 when it cannot find
    the file. So `SELECT 1` succeeds against a database with ZERO TABLES, the
    healthcheck goes green, and the service sits there healthy and unable to
    store anything.

    That is not a hypothetical: the schema arrives through a compose bind mount,
    and if the path is missing Docker CREATES AN EMPTY DIRECTORY there. The
    sibling annexes' own `.gitignore` files carry an unanchored `[Ss]cripts`
    pattern that would make `scripts/sql/schema.sql` invisible to git, producing
    exactly that.

    Comparing against `shared.const.LOCAL_SCHEMA_VERSION` collapses all of it —
    missing mount, directory mount, untracked file, half-applied schema, stale
    schema — into one 503 that the existing healthcheck already gates on.

2.  READINESS DOES NOT GATE ON THE BROKER.

    Compose's healthcheck reads this endpoint. If a broker restart made it
    unhealthy, compose would roll the API out of service and take the whole admin
    surface — device lists, settings, everything that needs no broker at all —
    down with it. The broker's state is reported under `detail`, where a human
    and a dashboard can see it, and where nothing restarts a container over it.

    The WORKER takes the OPPOSITE decision, deliberately: its heartbeat stops
    when the broker connection drops, because a worker that cannot reach the
    broker is doing nothing and should be restarted. Both halves are commented,
    in both places, because each looks like a mistake from the other's side.
----------------------------------------------------------------------------
"""

import asyncio
from typing import Any

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from core import metrics as app_metrics
from core.database.database import crm_engine, local_engine
from domain.partitions import PARTITIONED_TABLES, default_partition_name
from shared.const import LOCAL_SCHEMA_VERSION

health_router = APIRouter()


async def check_crm_db() -> dict[str, Any]:
    """The CRM database is readable.

    `SELECT 1` is right here and wrong for the local database: this service does
    not own the CRM schema and has no version of it to assert against.
    """
    try:
        async with crm_engine.connect() as connection:
            await connection.execute(text("SELECT 1"))
        return {"ok": True}
    except SQLAlchemyError as exc:
        return {"ok": False, "error": str(exc)}


async def check_local_db() -> dict[str, Any]:
    """The owned database exists AND carries the schema this build expects."""
    try:
        async with local_engine.connect() as connection:
            result = await connection.execute(text("SELECT max(version) FROM schema_version"))
            version = result.scalar()
    except SQLAlchemyError as exc:
        # An UndefinedTable here is the signal, not an anomaly: the database
        # exists (10-databases.sql created it) and the schema did not arrive.
        return {"ok": False, "error": str(exc)}
    if version is None:
        return {"ok": False, "error": "schema_version is empty - schema not applied"}
    if int(version) != LOCAL_SCHEMA_VERSION:
        return {
            "ok": False,
            "error": (f"schema version {version} but this build expects {LOCAL_SCHEMA_VERSION}"),
        }
    return {"ok": True, "version": int(version)}


async def check_default_partition() -> dict[str, Any]:
    """EVERY DEFAULT partition is empty - not just `measurement`'s.

    A non-empty default means the create-ahead job has stopped (plan 6.2 rule 2).
    Nothing fails at write time when that happens - rows land in the default and
    queries keep working - so the only way it ever surfaces is here, or months
    later when creating a partition fails over a range the default already holds
    rows in, at 00:00 UTC on the first of a month.

    ---- why this iterates the registry ----
    This probe named `measurement_default` alone until build step 8, which
    repeated INSIDE THE PROBE the exact failure plan 6.2 exists to prevent: "a job
    that names only `measurement` lets the rollups freeze about four months in
    while ingestion goes on looking perfectly healthy". Ingest would have stayed
    green while `rollup_device_hour_default` filled up.

    `domain.partitions.PARTITIONED_TABLES` rather than a query against
    `live_partitioned_table`, so a missing or empty registry table cannot make
    this probe pass by finding nothing to check. `tests/test_partitions.py`
    asserts the Python tuple and the SQL registry agree.

    Counted rather than sampled: the tables are expected to be empty, so the
    count is free, and `EXISTS` would hide how far one has run away.
    """
    offenders: dict[str, int] = {}
    counts: dict[str, int] = {}
    try:
        async with local_engine.connect() as connection:
            for table in PARTITIONED_TABLES:
                name = default_partition_name(table)
                result = await connection.execute(text(f"SELECT count(*) FROM {name}"))  # noqa: S608
                rows = int(result.scalar() or 0)
                counts[name] = rows
                if rows:
                    offenders[name] = rows
    except SQLAlchemyError as exc:
        return {"ok": False, "error": str(exc)}
    if offenders:
        detail = ", ".join(f"{name}: {rows}" for name, rows in sorted(offenders.items()))
        return {
            "ok": False,
            "error": f"rows in a default partition - the partition job has stopped ({detail})",
            "rows": counts,
        }
    return {"ok": True, "rows": counts}


@health_router.get("/liveness", tags=["Health"])
async def liveness():
    return {"status": "alive"}


@health_router.get("/readiness", tags=["Health"])
async def readiness() -> JSONResponse:
    crm_result, local_result, partition_result = await asyncio.gather(
        check_crm_db(),
        check_local_db(),
        check_default_partition(),
    )

    checks = {
        "crm_database": crm_result,
        "database": local_result,
        "default_partition": partition_result,
    }

    # One counter point per component per probe, so the metrics backend can plot
    # the failure rate (rate(health_checks_total{ok="false"}[5m])) and alert on
    # it without parsing log lines.
    for component, result in checks.items():
        app_metrics.health_checks.add(
            1,
            {"component": component, "ok": "true" if result["ok"] else "false"},
        )

    is_ready = all(result["ok"] for result in checks.values())

    payload = {
        "status": "healthy" if is_ready else "unhealthy",
        "checks": checks,
    }

    return JSONResponse(
        status_code=200 if is_ready else 503,
        content=payload,
    )


@health_router.get("/health", tags=["Health"])
async def health():
    return await readiness()
