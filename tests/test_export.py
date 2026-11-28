"""The live-data part of a data export (api/data_export.py).

Through the kit's own contract check (`run_contract_check`): the answer matches the contract,
every file listed was written, and each JSON file is checked here against its item schema
(core/export/schemas/files/live-data/). The curves are CSV and are read back as text.
"""

from __future__ import annotations

import csv
import datetime
import io
import json
import pathlib
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from api.data_export import CURVE_HEADER, export_handler, held_spans
from core.export import ExportHandler, run_contract_check
from tests.factories.device_factory import create_device, create_measurement
from tests.factories.operation_factory import create_member, create_operation, link_user_to_member
from tests.factories.subscription_factory import create_community

SERVICE = "live-data"
SCHEMAS = pathlib.Path(__file__).resolve().parent.parent / f"core/export/schemas/files/{SERVICE}"
EAN = "541448200000000011"
D = datetime.date


def handler_for(session: AsyncSession) -> ExportHandler:
    @asynccontextmanager
    async def shared() -> AsyncIterator[AsyncSession]:
        yield session

    return export_handler(local=shared, crm=shared)


def _validators() -> dict[str, Draft202012Validator]:
    schemas = [json.loads(p.read_text(encoding="utf-8")) for p in SCHEMAS.glob("*.schema.json")]
    registry: Registry = Registry().with_resources(
        (schema["$id"], Resource.from_contents(schema)) for schema in schemas
    )
    return {
        schema["title"].split("/")[1].removesuffix(".json item"): Draft202012Validator(
            schema, registry=registry, format_checker=FormatChecker()
        )
        for schema in schemas
        if schema.get("title", "").endswith(" item")
    }


async def export(
    session: AsyncSession, *, scope: dict[str, str], requester: str
) -> tuple[dict[str, Any], dict[str, list[dict[str, Any]]], dict[str, list[list[str]]]]:
    """Runs one part; returns part.done, each JSON file's items (schema-checked), each CSV."""
    result = await run_contract_check(
        handler_for(session), service=SERVICE, scope=scope, requester_id=requester
    )
    assert result.outcome == "done", result.message
    validators = _validators()
    files: dict[str, list[dict[str, Any]]] = {}
    curves: dict[str, list[list[str]]] = {}
    for file in result.message["files"]:
        body = result.store.text(file["key"])
        if file["path"].endswith(".csv"):
            curves[file["path"]] = list(csv.reader(io.StringIO(body)))
            continue
        document = json.loads(body)
        entity = document["entity"]
        errors = [
            f"{entity}[{i}]: {error.message}"
            for i, item in enumerate(document["items"])
            for error in validators[entity].iter_errors(item)
        ]
        assert not errors, errors
        files[entity] = document["items"]
    return result.message, files, curves


def counters(done: dict[str, Any]) -> dict[str, int]:
    prefix = f"export.{SERVICE}."
    return {item["labelKey"].removeprefix(prefix): item["count"] for item in done["summary"]}


async def account(session: AsyncSession, community: Any, sub: str, role: str) -> None:
    user = int(
        (
            await session.execute(
                text("INSERT INTO app_user (auth_user_id, email) VALUES (:s, :e) RETURNING id"),
                {"s": sub, "e": f"{sub}@example.test"},
            )
        ).scalar_one()
    )
    await session.execute(
        text("INSERT INTO community_user (id_community, id_user, role) VALUES (:c, :u, :r)"),
        {"c": community.id, "u": user, "r": role},
    )
    await session.flush()


async def window(
    session: AsyncSession, community: Any, member: int, lo: datetime.date, hi: datetime.date | None
) -> None:
    await session.execute(
        text(
            "INSERT INTO device_owner_window (ean, id_community, id_member, valid_from, valid_to,"
            " ambiguous, refreshed_at) VALUES (:e, :c, :m, :lo, :hi, false, now())"
        ),
        {"e": EAN, "c": community.id, "m": member, "lo": lo, "hi": hi},
    )


def utc(year: int, month: int, day: int, hour: int, minute: int) -> datetime.datetime:
    return datetime.datetime(year, month, day, hour, minute, tzinfo=datetime.UTC)


async def world(session: AsyncSession) -> dict[str, Any]:
    """One meter held by alice's member (20/12 - 10/01, with a third holder over 4-5/01), then
    bob's (from 11/01); five readings across the two years and the three holders."""
    community = await create_community(session)
    alice = await create_member(session, id_community=community.id, name="Alice")
    bob = await create_member(session, id_community=community.id, name="Bob")
    third = await create_member(session, id_community=community.id, name="Third")
    await link_user_to_member(session, auth_user_id="user-alice", id_member=alice)
    await link_user_to_member(session, auth_user_id="user-bob", id_member=bob)
    await account(session, community, "user-admin", "ADMIN")
    await account(session, community, "user-manager", "MANAGER")
    await window(session, community, alice, D(2025, 12, 20), D(2026, 1, 10))
    await window(session, community, bob, D(2026, 1, 11), None)
    await window(session, community, third, D(2026, 1, 4), D(2026, 1, 5))
    device = await create_device(session, id_community=community.id, ean=EAN)
    readings = {
        "dec21": utc(2025, 12, 21, 12, 0),
        # Local 01/01 00:00 ENDS the 31/12 23:45 interval: a 2025 reading.
        "new_year": utc(2025, 12, 31, 23, 0),
        "jan03": utc(2026, 1, 3, 10, 0),
        "jan05": utc(2026, 1, 5, 10, 0),  # the third holder's window covers it
        "jan12": utc(2026, 1, 12, 10, 0),  # bob's
    }
    for i, ts in enumerate(readings.values(), start=1):
        await create_measurement(
            session, id_device=device, id_community=community.id, ts=ts, import_wh=float(i)
        )
    await session.flush()
    return {"community": community, "readings": readings}


def community_scope(w: dict[str, Any]) -> dict[str, str]:
    return {"type": "community", "id": w["community"].auth_community_id}


def ts_of(rows: list[list[str]]) -> list[str]:
    return [row[0] for row in rows[1:]]


def z(ts: datetime.datetime) -> str:
    return ts.isoformat().replace("+00:00", "Z")


class TestHeldSpans:
    def test_another_holder_s_days_are_cut_out(self):
        assert held_spans([(D(2026, 1, 1), D(2026, 1, 10))], [(D(2026, 1, 4), D(2026, 1, 5))]) == [
            (D(2026, 1, 1), D(2026, 1, 3)),
            (D(2026, 1, 6), D(2026, 1, 10)),
        ]

    def test_an_open_window_and_an_open_overlap(self):
        assert held_spans([(D(2026, 1, 1), None)], [(D(2026, 3, 1), None)]) == [
            (D(2026, 1, 1), D(2026, 2, 28))
        ]

    def test_a_holder_before_or_after_takes_nothing(self):
        mine = [(D(2026, 1, 11), None)]
        assert held_spans(mine, [(D(2025, 12, 20), D(2026, 1, 10))]) == mine

    def test_the_requester_s_own_windows_are_joined(self):
        assert held_spans([(D(2026, 1, 1), D(2026, 1, 5)), (D(2026, 1, 6), D(2026, 1, 9))], []) == [
            (D(2026, 1, 1), D(2026, 1, 9))
        ]


class TestUserExport:
    async def test_the_full_curve_of_the_days_held_per_ean_and_brussels_year(self, db_session):
        w = await world(db_session)
        r = w["readings"]
        done, files, curves = await export(
            db_session, scope={"type": "user", "id": "user-alice"}, requester="user-alice"
        )

        assert set(curves) == {
            f"{SERVICE}/curves/{EAN}/2025.csv",
            f"{SERVICE}/curves/{EAN}/2026.csv",
        }
        y2025 = curves[f"{SERVICE}/curves/{EAN}/2025.csv"]
        assert y2025[0] == list(CURVE_HEADER)
        assert ts_of(y2025) == [z(r["dec21"]), z(r["new_year"])]
        # jan05 is the third holder's day too, jan12 is bob's.
        assert ts_of(curves[f"{SERVICE}/curves/{EAN}/2026.csv"]) == [z(r["jan03"])]
        (meter,) = files["my-meters"]
        assert meter["ean"] == EAN
        assert meter["held"] == [
            {"from": "2025-12-20", "to": "2026-01-03"},
            {"from": "2026-01-06", "to": "2026-01-10"},
        ]
        assert [d["type"] for d in meter["devices"]] == ["PRODUCTION"]
        assert [c["readings"] for c in meter["curves"]] == [2, 1]
        assert files["my-consents"] == []
        assert counters(done) == {
            "periods_withheld": 1,
            "readings": 3,
            "curve_files": 2,
            "meters": 1,
            "consents": 0,
        }

    async def test_the_next_holder_gets_their_own_days_only(self, db_session):
        w = await world(db_session)
        _done, files, curves = await export(
            db_session, scope={"type": "user", "id": "user-bob"}, requester="user-bob"
        )
        assert {path: ts_of(rows) for path, rows in curves.items()} == {
            f"{SERVICE}/curves/{EAN}/2026.csv": [z(w["readings"]["jan12"])]
        }
        assert files["my-meters"][0]["held"] == [{"from": "2026-01-11", "to": None}]

    async def test_someone_linked_to_no_member_gets_no_file(self, db_session):
        await world(db_session)
        done, files, curves = await export(
            db_session, scope={"type": "user", "id": "user-admin"}, requester="user-admin"
        )
        assert files == {}
        assert curves == {}
        assert done["files"] == []


async def rollup_day(
    session: AsyncSession, community: Any, day: datetime.date, n_members_min: int
) -> None:
    await session.execute(
        text(
            "INSERT INTO rollup_community_day (id_community, bucket, import_wh, export_wh,"
            " production_wh, n_devices, n_devices_production, n_members, n_devices_unattributed,"
            " n_hours, computed_at, n_members_min) VALUES (:c,"
            " CAST(:d AS date)::timestamp AT TIME ZONE 'Europe/Brussels', 10, 20, 30, 6, 6, :n,"
            " 0, 24, now(), :n)"
        ),
        {"c": community.id, "d": day, "n": n_members_min},
    )


async def operation_day(
    session: AsyncSession, community: Any, operation: int, day: datetime.date, n_members_min: int
) -> None:
    await session.execute(
        text(
            "INSERT INTO rollup_operation_day (id_community, id_sharing_operation, bucket,"
            " import_wh, export_wh, production_wh, shared_wh, n_devices, n_devices_production,"
            " n_members, n_members_min, n_hours, computed_at) VALUES (:c, :o,"
            " CAST(:d AS date)::timestamp AT TIME ZONE 'Europe/Brussels', 10, 20, 30, :s, 6, 6,"
            " :n, :n, 24, now())"
        ),
        # The remainder (operation 0) shares nothing: its shared_wh is NULL (a CHECK).
        {
            "c": community.id,
            "o": operation,
            "d": day,
            "n": n_members_min,
            "s": None if operation == 0 else 5.0,
        },
    )


async def aggregates(session: AsyncSession, w: dict[str, Any]) -> int:
    """3/01: six members, all in one operation (published). 4/01: two, and devices in no
    operation (the remainder row): withheld at k=5."""
    community = w["community"]
    operation = await create_operation(session, id_community=community.id, name="Solar")
    await rollup_day(session, community, D(2026, 1, 3), 6)
    await operation_day(session, community, operation, D(2026, 1, 3), 6)
    await rollup_day(session, community, D(2026, 1, 4), 2)
    await operation_day(session, community, operation, D(2026, 1, 4), 2)
    await operation_day(session, community, 0, D(2026, 1, 4), 0)
    await session.flush()
    return operation


class TestCommunityExport:
    async def test_daily_aggregates_judged_as_the_app_does_and_never_a_curve(self, db_session):
        w = await world(db_session)
        operation = await aggregates(db_session, w)
        done, files, curves = await export(
            db_session, scope=community_scope(w), requester="user-admin"
        )

        assert curves == {}
        assert set(files) == {"settings", "devices", "consents", "days", "operation-days"}
        published, withheld = files["days"]
        assert published == {
            "day": "2026-01-03",
            "production_wh": 30.0,
            "n_devices": 6,
            "n_hours": 24,
            "import_wh": 10.0,
            "export_wh": 20.0,
            "shared_wh": 5.0,
            "grid": "published",
        }
        assert withheld == {
            "day": "2026-01-04",
            "production_wh": 30.0,
            "n_devices": 6,
            "n_hours": 24,
            "grid": "withheld",
        }
        # The remainder row (devices in no operation) is never an operation of its own.
        assert [
            (o["id_sharing_operation"], o["day"], o["grid"]) for o in files["operation-days"]
        ] == [
            (operation, "2026-01-03", "published"),
            (operation, "2026-01-04", "withheld"),
        ]
        assert files["operation-days"][0]["operation"] == "Solar"
        assert files["settings"] == [
            {
                "members_see_production": True,
                "members_see_aggregate": True,
                "k": 5,
                "is_default": True,
            }
        ]
        assert [d["ean"] for d in files["devices"]] == [EAN]
        assert done["omitted"] == [
            {"what": "live-data.days.grid", "reason": "below_threshold"},
            {"what": "live-data.operation-days.grid", "reason": "below_threshold"},
        ]
        assert counters(done)["days_withheld"] == 1

    async def test_the_community_s_own_k_is_the_threshold(self, db_session):
        w = await world(db_session)
        await aggregates(db_session, w)
        await db_session.execute(
            text(
                "INSERT INTO community_live_settings (id_community, members_see_production,"
                " members_see_aggregate, k) VALUES (:c, true, true, 3)"
            ),
            {"c": w["community"].id},
        )
        await db_session.execute(
            text("UPDATE rollup_community_day SET n_members_min = 3 WHERE id_community = :c"),
            {"c": w["community"].id},
        )
        # Without devices outside the operation: the community is its one operation.
        await db_session.execute(
            text(
                "DELETE FROM rollup_operation_day"
                " WHERE id_community = :c AND id_sharing_operation = 0"
            ),
            {"c": w["community"].id},
        )
        await db_session.execute(
            text(
                "UPDATE rollup_operation_day SET n_members_min = 3, n_members = 3"
                " WHERE id_community = :c AND id_sharing_operation <> 0"
            ),
            {"c": w["community"].id},
        )
        done, files, _curves = await export(
            db_session, scope=community_scope(w), requester="user-admin"
        )
        assert [d["grid"] for d in files["days"]] == ["published", "published"]
        assert files["settings"][0]["k"] == 3
        assert done["omitted"] == []

    async def test_consents_are_listed_with_their_journal(self, db_session):
        w = await world(db_session)
        consent = int(
            (
                await db_session.execute(
                    text(
                        "INSERT INTO consent (id_community, id_member, field, granted, decided_at)"
                        " VALUES (:c, 1, 'share_with_community', true, now()) RETURNING id"
                    ),
                    {"c": w["community"].id},
                )
            ).scalar_one()
        )
        await db_session.execute(
            text(
                "INSERT INTO consent_event (id_consent, id_community, actor, action, recorded_at)"
                " VALUES (:i, :c, 'user-alice', 'granted', now())"
            ),
            {"i": consent, "c": w["community"].id},
        )
        _done, files, _curves = await export(
            db_session, scope=community_scope(w), requester="user-admin"
        )
        (item,) = files["consents"]
        assert item["field"] == "share_with_community"
        assert [e["action"] for e in item["events"]] == ["granted"]

    async def test_only_an_admin_exports_the_community(self, db_session):
        w = await world(db_session)
        result = await run_contract_check(
            handler_for(db_session),
            service=SERVICE,
            scope=community_scope(w),
            requester_id="user-manager",
        )
        assert result.outcome == "failed"
        assert result.message["errorCode"] == "forbidden"

    async def test_an_unknown_community_is_scope_not_found(self, db_session):
        await world(db_session)
        result = await run_contract_check(
            handler_for(db_session),
            service=SERVICE,
            scope={"type": "community", "id": str(uuid.uuid4())},
            requester_id="user-admin",
        )
        assert result.outcome == "failed"
        assert result.message["errorCode"] == "scope_not_found"
