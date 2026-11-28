"""The live-data part of a data export (monorepo contracts/export/README.md, §7).

User scope — the full curves of the requester's own meters:
- ``curves/<ean>/<year>.csv``: every reading kept of each meter one of their members held, for
  the days they held it (the ownership windows the scheduler projects from the CRM's
  ``meter_data``, Brussels calendar days, the interval's own day). A day another holder's window
  also covers is left out: whose consumption it was is not established, and it may be someone
  else's. Years are Brussels years; ``interval_end`` is the END of the interval, in UTC.
- ``my-meters.json``: each meter (community, EAN), the days held, its devices and its curve files;
- ``my-consents.json``: their members' consent decisions, with their journal.

Community scope (an ADMIN, re-checked in crm_db by the kit) — what the app shows a manager, and
never an individual curve:
- ``days.json`` and ``operation-days.json``: the daily community and per-operation aggregates,
  each day judged as the app judges it (``domain/kanon.py``, the community's k): production
  always, the grid terms (import, export, the shared estimate) only when the day passes, and an
  omission ``below_threshold`` in the manifest when any day does not;
- ``devices.json``, ``settings.json`` and ``consents.json``.

The module's subscription is not checked: an export is not a use of the module. NATS is this
service's only use of the broker: the consumer connects in the background and retries, so a
broker outage never stops the API.
"""

from __future__ import annotations

import asyncio
import contextlib
import datetime
import logging
import math
from collections.abc import AsyncIterator, Callable, Iterable, Sequence
from contextlib import AbstractAsyncContextManager
from decimal import Decimal
from enum import IntEnum
from typing import Any

from sqlalchemy import ColumnElement, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from api.live import visibility as vis
from core.config import settings
from core.export import (
    ExportHandler,
    ExportStorageSettings,
    PartContext,
    Requester,
    S3PartStore,
    SqlCrmLookup,
    resolve_requester,
)
from domain import kanon
from shared.const import DeviceStatus, DeviceType, ProductionChain
from shared.models.local_models import (
    CommunityLiveSettingsModel,
    ConsentEventModel,
    ConsentModel,
    DeviceModel,
)

logger = logging.getLogger(__name__)

SERVICE = "live-data"
SessionFactory = Callable[[], AbstractAsyncContextManager[AsyncSession]]
_RETRY_SECONDS = 15.0
CURVE_HEADER = ("interval_end", "interval_s", "import_wh", "export_wh", "production_wh", "device")

Span = tuple[datetime.date, datetime.date | None]
"""Closed Brussels calendar days; ``None`` is open-ended, as in ``device_owner_window``."""


# ---- held days: the user's windows minus every other holder's ------------------------------


def _merge(spans: Iterable[Span]) -> list[Span]:
    """Sorted, overlapping or touching spans joined."""
    out: list[Span] = []
    for lo, hi in sorted(spans, key=lambda s: s[0]):
        if out:
            last_lo, last_hi = out[-1]
            if last_hi is None or lo <= last_hi + datetime.timedelta(days=1):
                end = None if last_hi is None or hi is None else max(last_hi, hi)
                out[-1] = (last_lo, end)
                continue
        out.append((lo, hi))
    return out


def _overlap(a: Span, b: Span) -> bool:
    return (a[1] is None or b[0] <= a[1]) and (b[1] is None or a[0] <= b[1])


def held_spans(mine: Iterable[Span], others: Iterable[Span]) -> list[Span]:
    """The days the requester's members held a meter that no other holder's window covers.

    Two windows over the same day are what the scheduler flags ``ambiguous``: the CRM says two
    holders at once. Those days stay out of the export — the curve may be the other holder's.
    """
    held = _merge(mine)
    for o_lo, o_hi in _merge(others):
        cut: list[Span] = []
        for lo, hi in held:
            if (o_hi is not None and o_hi < lo) or (hi is not None and o_lo > hi):
                cut.append((lo, hi))
                continue
            if o_lo > lo:
                cut.append((lo, o_lo - datetime.timedelta(days=1)))
            if o_hi is not None and (hi is None or o_hi < hi):
                cut.append((o_hi + datetime.timedelta(days=1), hi))
        held = cut
    return held


# ---- values ---------------------------------------------------------------------------------


def _iso(value: datetime.datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=datetime.UTC)
    return value.astimezone(datetime.UTC).isoformat().replace("+00:00", "Z")


def _day(value: datetime.date | None) -> str | None:
    return value.isoformat() if value is not None else None


def _num(value: float | None) -> float | None:
    """JSON cannot carry NaN or infinity: such a value is written as null."""
    if value is None:
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _name(enum: type[IntEnum], value: int | None) -> str | int | None:
    """A code by its name; a code this version does not know keeps its number."""
    if value is None:
        return None
    try:
        return enum(value).name
    except ValueError:
        return value


def _decimal(value: Decimal | float | None) -> str | None:
    if value is None:
        return None
    return format(Decimal(str(value)), "f")


def _device(row: Any) -> dict[str, Any]:
    return {
        "id": str(row.public_id),
        "name": row.name,
        "ean": row.ean,
        "type": _name(DeviceType, row.type),
        "status": _name(DeviceStatus, row.status),
        "pure_injection": row.pure_injection,
        "capacity_kva": _decimal(row.capacity_kva),
        "production_chain": _name(ProductionChain, row.production_chain),
        "enrolled_at": _iso(row.enrolled_at),
        "revoked_at": _iso(row.revoked_at),
        "created_at": _iso(row.created_at),
        "updated_at": _iso(row.updated_at),
    }


# A reading's Brussels day is its interval's own: `ts` is the END of the interval, so one second
# before it (domain/buckets.py). `:cs`/`:los`/`:his` are the held spans, one community, first and
# last day (NULL = open) per row. Written out in full twice rather than composed: a query is a
# literal, never assembled.
_YEARS_SQL = (
    "SELECT DISTINCT EXTRACT(YEAR FROM ((m.ts - interval '1 second') AT TIME ZONE :tz)::date)::int"
    " AS year FROM measurement m JOIN device d ON d.id = m.id_device"
    " WHERE d.ean = :ean AND EXISTS (SELECT 1 FROM unnest(CAST(:cs AS integer[]),"
    " CAST(:los AS date[]), CAST(:his AS date[])) AS h(c, lo, hi) WHERE d.id_community = h.c"
    " AND ((m.ts - interval '1 second') AT TIME ZONE :tz)::date"
    " BETWEEN h.lo AND COALESCE(h.hi, 'infinity'))"
    " ORDER BY year"
)
_CURVE_SQL = (
    "SELECT m.ts, m.interval_s, m.import_wh, m.export_wh, m.production_wh, d.public_id"
    " FROM measurement m JOIN device d ON d.id = m.id_device"
    " WHERE d.ean = :ean AND EXISTS (SELECT 1 FROM unnest(CAST(:cs AS integer[]),"
    " CAST(:los AS date[]), CAST(:his AS date[])) AS h(c, lo, hi) WHERE d.id_community = h.c"
    " AND ((m.ts - interval '1 second') AT TIME ZONE :tz)::date"
    " BETWEEN h.lo AND COALESCE(h.hi, 'infinity'))"
    " AND EXTRACT(YEAR FROM ((m.ts - interval '1 second') AT TIME ZONE :tz)::date) = :year"
    " ORDER BY m.ts, d.id"
)


class _Part:
    def __init__(self, ctx: PartContext, *, local: AsyncSession, crm: AsyncSession) -> None:
        self.ctx = ctx
        self.local = local
        self.crm = crm
        self.tz = settings.INGEST_TIMEZONE

    async def _rows(self, sql: str, **params: Any) -> Sequence[Any]:
        return (await self.local.execute(text(sql), params)).all()

    async def community_names(self, ids: Iterable[int]) -> dict[int, str | None]:
        wanted = sorted(set(ids))
        if not wanted:
            return {}
        result = await self.crm.execute(
            text("SELECT id, name FROM community WHERE id = ANY(:ids)"), {"ids": wanted}
        )
        return {int(row[0]): row[1] for row in result}

    async def devices(self, where: ColumnElement[bool]) -> list[DeviceModel]:
        query = select(DeviceModel).where(where).order_by(DeviceModel.id)
        return list((await self.local.execute(query)).scalars())

    async def consents(self, where: ColumnElement[bool]) -> list[dict[str, Any]]:
        query = select(ConsentModel).where(where).order_by(ConsentModel.id)
        decisions = list((await self.local.execute(query)).scalars())
        ids = [row.id for row in decisions]
        events: dict[int, list[dict[str, Any]]] = {i: [] for i in ids}
        if ids:
            journal = (
                select(ConsentEventModel)
                .where(ConsentEventModel.id_consent.in_(ids))
                .order_by(ConsentEventModel.id)
            )
            for event in (await self.local.execute(journal)).scalars():
                events[event.id_consent].append(
                    {
                        "actor": event.actor,
                        "action": event.action,
                        "recorded_at": _iso(event.recorded_at),
                    }
                )
        return [
            {
                "id": row.id,
                "id_member": row.id_member,
                "field": row.field,
                "granted": row.granted,
                "effective_from": _iso(row.effective_from),
                "decided_at": _iso(row.decided_at),
                "events": events[row.id],
            }
            for row in decisions
        ]

    # ---- user scope --------------------------------------------------------------------------

    async def user_export(self, requester: Requester) -> None:
        members = list(requester.member_ids)
        if not members:
            return  # no member, no meter: nothing of this service is theirs (files: [])
        windows = await self._rows(
            "SELECT w.id_community, w.ean, w.id_member, w.valid_from, w.valid_to "
            "FROM device_owner_window w "
            "WHERE w.ean IN (SELECT ean FROM device_owner_window WHERE id_member = ANY(:m)) "
            "ORDER BY w.id_community, w.ean, w.valid_from",
            m=members,
        )
        mine: dict[tuple[int, str], list[Span]] = {}
        others: dict[tuple[int, str], list[Span]] = {}
        for w in windows:
            key = (int(w.id_community), str(w.ean))
            bucket = mine if w.id_member in members else others
            bucket.setdefault(key, []).append((w.valid_from, w.valid_to))
        held = {key: held_spans(spans, others.get(key, [])) for key, spans in mine.items()}
        # Another holder's window over the requester's days: those days are left out.
        self.ctx.count(
            "periods_withheld",
            sum(
                1
                for key, spans in mine.items()
                for other in others.get(key, [])
                if any(_overlap(other, span) for span in spans)
            ),
        )

        devices: dict[tuple[int, str], list[dict[str, Any]]] = {key: [] for key in held}
        if held:
            for device in await self.devices(DeviceModel.ean.in_(sorted({e for _c, e in held}))):
                if (device.id_community, device.ean) in devices:
                    devices[(device.id_community, device.ean)].append(_device(device))

        # One curve file per EAN and Brussels year, whatever the community.
        by_ean: dict[str, list[tuple[int, Span]]] = {}
        for (community, ean), spans in held.items():
            by_ean.setdefault(ean, []).extend((community, span) for span in spans)
        curves: dict[str, list[dict[str, Any]]] = {}
        readings = 0
        for ean, community_spans in sorted(by_ean.items()):
            params = {
                "tz": self.tz,
                "ean": ean,
                "cs": [c for c, _s in community_spans],
                "los": [span[0] for _c, span in community_spans],
                "his": [span[1] for _c, span in community_spans],
            }
            for (year,) in await self._rows(_YEARS_SQL, **params):
                path = f"curves/{ean}/{year}.csv"
                written = await self.ctx.write_csv(
                    path, CURVE_HEADER, self._curve(params, int(year))
                )
                readings += written
                curves.setdefault(ean, []).append(
                    {"year": int(year), "path": f"{SERVICE}/{path}", "readings": written}
                )
        self.ctx.count("readings", readings)
        self.ctx.count("curve_files", sum(len(c) for c in curves.values()))

        names = await self.community_names(c for c, _e in held)
        meters = [
            {
                "id_community": community,
                "community": names.get(community),
                "ean": ean,
                "held": [{"from": _day(lo), "to": _day(hi)} for lo, hi in spans],
                "devices": devices[(community, ean)],
                "curves": curves.get(ean, []),
            }
            for (community, ean), spans in sorted(held.items())
        ]
        self.ctx.count("meters", await self.ctx.write_json("my-meters", meters))
        mine_consents = await self.consents(ConsentModel.id_member.in_(members))
        self.ctx.count("consents", await self.ctx.write_json("my-consents", mine_consents))

    async def _curve(self, params: dict[str, Any], year: int) -> AsyncIterator[Sequence[Any]]:
        result = await self.local.stream(text(_CURVE_SQL), {**params, "year": year})
        async for end, interval_s, imp, exp, prod, device in result:
            yield (_iso(end), interval_s, _num(imp), _num(exp), _num(prod), str(device))

    # ---- community scope ---------------------------------------------------------------------

    async def community_export(self, id_community: int) -> None:
        row = await self.local.get(CommunityLiveSettingsModel, id_community)
        visibility = vis.resolve(row)
        k = vis.k_for(visibility)
        await self.ctx.write_json(
            "settings",
            [
                {
                    "members_see_production": visibility.members_see_production,
                    "members_see_aggregate": visibility.members_see_aggregate,
                    "k": k,
                    "is_default": visibility.is_default,
                }
            ],
        )
        devices = await self.devices(DeviceModel.id_community == id_community)
        self.ctx.count(
            "devices", await self.ctx.write_json("devices", (_device(d) for d in devices))
        )
        consents = await self.consents(ConsentModel.id_community == id_community)
        self.ctx.count("consents", await self.ctx.write_json("consents", consents))

        operations = await self._rows(
            "SELECT id_sharing_operation, (bucket AT TIME ZONE :tz)::date AS day, import_wh, "
            "export_wh, production_wh, shared_wh, n_devices, n_members, n_members_min, n_hours "
            "FROM rollup_operation_day WHERE id_community = :c "
            "ORDER BY bucket, id_sharing_operation",
            tz=self.tz,
            c=id_community,
        )
        by_day: dict[datetime.date, list[Any]] = {}
        for op in operations:
            by_day.setdefault(op.day, []).append(op)
        days = await self._rows(
            "SELECT (bucket AT TIME ZONE :tz)::date AS day, import_wh, export_wh, production_wh, "
            "n_devices, n_members_min, n_hours "
            "FROM rollup_community_day WHERE id_community = :c ORDER BY bucket",
            tz=self.tz,
            c=id_community,
        )

        withheld = 0

        def community_day(day: Any) -> dict[str, Any]:
            nonlocal withheld
            ops = by_day.get(day.day, [])
            scopes = [
                kanon.ScopeMembers(o.id_sharing_operation, o.n_members_min, o.n_members)
                for o in ops
            ]
            item: dict[str, Any] = {
                "day": _day(day.day),
                "production_wh": _num(day.production_wh),
                "n_devices": day.n_devices,
                "n_hours": day.n_hours,
            }
            if kanon.community_grid_is_visible(day.n_members_min, scopes, k):
                shared = sum(
                    o.shared_wh or 0.0 for o in ops if o.id_sharing_operation != kanon.REMAINDER
                )
                item.update(
                    import_wh=_num(day.import_wh),
                    export_wh=_num(day.export_wh),
                    shared_wh=_num(shared),
                )
                item["grid"] = "published"
            else:
                withheld += 1
                item["grid"] = "withheld"
            return item

        self.ctx.count("days", await self.ctx.write_json("days", (community_day(d) for d in days)))
        self.ctx.count("days_withheld", withheld)
        if withheld:
            self.ctx.omit("days.grid", "below_threshold")

        names = await self._operation_names(
            id_community, {o.id_sharing_operation for o in operations} - {kanon.REMAINDER}
        )
        op_withheld = 0

        def operation_day(op: Any) -> dict[str, Any]:
            nonlocal op_withheld
            scope = kanon.ScopeMembers(op.id_sharing_operation, op.n_members_min, op.n_members)
            item: dict[str, Any] = {
                "id_sharing_operation": op.id_sharing_operation,
                "operation": names.get(op.id_sharing_operation),
                "day": _day(op.day),
                "production_wh": _num(op.production_wh),
                "n_devices": op.n_devices,
                "n_hours": op.n_hours,
            }
            if kanon.operation_grid_is_visible(scope, k):
                item.update(
                    import_wh=_num(op.import_wh),
                    export_wh=_num(op.export_wh),
                    shared_wh=_num(op.shared_wh),
                )
                item["grid"] = "published"
            else:
                op_withheld += 1
                item["grid"] = "withheld"
            return item

        self.ctx.count(
            "operation_days",
            await self.ctx.write_json(
                "operation-days",
                (operation_day(o) for o in operations if o.id_sharing_operation != kanon.REMAINDER),
            ),
        )
        self.ctx.count("operation_days_withheld", op_withheld)
        if op_withheld:
            self.ctx.omit("operation-days.grid", "below_threshold")

    async def _operation_names(self, id_community: int, ids: set[int]) -> dict[int, str | None]:
        if not ids:
            return {}
        result = await self.crm.execute(
            text(
                "SELECT id, name FROM sharing_operation WHERE id_community = :c AND id = ANY(:ids)"
            ),
            {"c": id_community, "ids": sorted(ids)},
        )
        return {int(row[0]): row[1] for row in result}


def export_handler(*, local: SessionFactory, crm: SessionFactory) -> ExportHandler:
    """The handler the kit calls for each part of this service."""

    async def handle(ctx: PartContext) -> None:
        async with crm() as crm_session, local() as local_session:
            requester = await resolve_requester(SqlCrmLookup(crm_session), ctx.request)
            part = _Part(ctx, local=local_session, crm=crm_session)
            if requester.community_id is None:
                await part.user_export(requester)
            else:
                await part.community_export(requester.community_id)

    return handle


async def _consume(stop: asyncio.Event, storage: ExportStorageSettings) -> None:
    """Connects to NATS (retried in the background), then runs the kit's consumer until stop."""
    import nats

    from core.database.database import AsyncSessionCRMFactory, AsyncSessionLocalFactory
    from core.export.consumer import run_export_consumer

    servers = [u.strip() for u in settings.NATS_URL.split(",") if u.strip()]
    client = None
    while client is None and not stop.is_set():
        try:
            client = await nats.connect(
                servers=servers,
                reconnect_time_wait=2,
                max_reconnect_attempts=-1,
                connect_timeout=5,
            )
        except Exception as err:
            logger.warning(
                "Data export: NATS unreachable; retrying",
                extra={"operation": "export:nats_retry", "error": type(err).__name__},
            )
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=_RETRY_SECONDS)
    if client is None:
        return
    try:
        await run_export_consumer(
            client.jetstream(),
            service=SERVICE,
            handler=export_handler(local=AsyncSessionLocalFactory, crm=AsyncSessionCRMFactory),
            store=S3PartStore(storage),
            stop=stop,
            expected_bucket=storage.bucket,
        )
    finally:
        with contextlib.suppress(Exception):
            await client.drain()


def start_export_consumer(stop: asyncio.Event) -> asyncio.Task[None] | None:
    """Starts this service's export consumer, or logs why it cannot (and the API runs on).

    Needs NATS_URL and the export account (EXPORT_STORAGE_*), never another storage account.
    """
    try:
        if not settings.NATS_URL.strip():
            raise RuntimeError("NATS_URL is not set")
        storage = ExportStorageSettings.from_env()
    except RuntimeError as err:
        logger.warning(
            "Data export disabled: %s",
            err,
            extra={"operation": "export:consumer_disabled", "service": SERVICE},
        )
        return None
    return asyncio.create_task(_consume(stop, storage), name="live-data-export-consumer")


async def stop_export_consumer(
    task: asyncio.Task[None] | None, stop: asyncio.Event, grace: float = 10.0
) -> None:
    """Stops pulling; a part in progress is aborted and nak'ed (redelivered later)."""
    stop.set()
    if task is None:
        return
    try:
        await asyncio.wait_for(task, timeout=grace)
    except TimeoutError:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
