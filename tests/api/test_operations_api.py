"""The sharing-operation read surface (D-14), through the real HTTP stack.

What these tests exist for, in order of how quietly each would fail:
  * a member reading anything beyond their own operation(s);
  * the community total, next to its operations, exposing a hidden one by
    subtraction;
  * a shared figure computed across operations, or from hourly sums;
  * a withheld term arriving as `null` - which a chart draws as zero.
"""

import datetime

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from api.live.schemas import MemberOperationPointOut
from ports.crm_core import SqlAlchemyCrmCoreRead
from tests.conftest import gateway_headers
from tests.factories.device_factory import create_device, create_hour_of_measurements
from tests.factories.meter_factory import create_owned_meter
from tests.factories.operation_factory import create_member, create_operation, link_user_to_member
from worker import rollups
from worker.ownership import refresh_community

MEMBER_SUB = "auth-member-1"


def _bucket() -> datetime.datetime:
    """A closed hour inside every default window."""
    now = datetime.datetime.now(datetime.UTC)
    return now.replace(minute=0, second=0, microsecond=0) - datetime.timedelta(hours=2)


async def _k(session: AsyncSession, id_community: int, k: int = 3) -> None:
    await session.execute(
        text(
            "INSERT INTO community_live_settings (id_community, members_see_production, "
            "members_see_aggregate, k) VALUES (:c, TRUE, TRUE, :k)"
        ),
        {"c": id_community, "k": k},
    )
    await session.flush()


async def _site(
    session: AsyncSession,
    id_community: int,
    *,
    member: int,
    operation: int | None,
    import_wh: float = 0.0,
    export_wh: float = 0.0,
    production_wh: float | None = None,
    bucket: datetime.datetime,
) -> str:
    """One metered site: a meter in `operation`, a device, one full hour."""
    ean = await create_owned_meter(
        session, id_community=id_community, id_member=member, id_sharing_operation=operation
    )
    device = await create_device(session, id_community=id_community, ean=ean)
    await create_hour_of_measurements(
        session,
        id_device=device,
        id_community=id_community,
        bucket=bucket,
        import_wh=import_wh,
        export_wh=export_wh,
        production_wh=production_wh,
    )
    return ean


async def _roll(session: AsyncSession, id_community: int, bucket: datetime.datetime) -> None:
    now = bucket + datetime.timedelta(hours=2)
    await refresh_community(
        session, SqlAlchemyCrmCoreRead(session), id_community=id_community, now=now
    )
    await rollups.tick_community(session, id_community=id_community, now=now)


def _point(series: dict, bucket: datetime.datetime) -> dict:
    stamp = bucket.strftime("%Y-%m-%dT%H")
    points: list[dict] = [p for p in series["points"] if p["bucket"].startswith(stamp)]
    (point,) = points
    return point


class TestTheManagersOperations:
    async def test_the_list_names_each_operation_with_its_coverage(
        self, client, db_session, community, manager_headers
    ):
        """`n_meters` counts the CRM's ACTIVE meters, monitored or not: the
        estimate covers `n_devices` of them, and the dashboard says so."""
        bucket = _bucket()
        op = await create_operation(db_session, id_community=community.id, name="Solar")
        await _site(db_session, community.id, member=1, operation=op, bucket=bucket)
        await create_owned_meter(
            db_session, id_community=community.id, id_member=2, id_sharing_operation=op
        )
        await _roll(db_session, community.id, bucket)

        response = await client.get("/operations", headers=manager_headers)

        assert response.status_code == 200
        assert response.json()["data"] == [
            {"id": op, "name": "Solar", "n_devices": 1, "n_meters": 2}
        ]

    async def test_an_operation_at_k_shows_every_term_with_the_shared_estimate(
        self, client, db_session, community, manager_headers
    ):
        await _k(db_session, community.id)
        bucket = _bucket()
        op = await create_operation(db_session, id_community=community.id, name="Op")
        await _site(
            db_session, community.id, member=1, operation=op, export_wh=200.0, bucket=bucket
        )
        for member in (2, 3):
            await _site(
                db_session, community.id, member=member, operation=op, import_wh=30.0, bucket=bucket
            )
        await _roll(db_session, community.id, bucket)

        series = (
            await client.get(
                f"/operations/{op}/series", headers=manager_headers, params={"resolution": "hour"}
            )
        ).json()["data"]

        point = _point(series, bucket)
        assert point["import_wh"] == pytest.approx(240.0)
        assert point["export_wh"] == pytest.approx(800.0)
        # Four quarters of LEAST(200, 60).
        assert point["shared_wh"] == pytest.approx(240.0)

    async def test_below_k_the_grid_and_shared_terms_are_absent_not_null(
        self, client, db_session, community, manager_headers
    ):
        await _k(db_session, community.id)
        bucket = _bucket()
        op = await create_operation(db_session, id_community=community.id, name="Op")
        for member in (1, 2):
            await _site(
                db_session,
                community.id,
                member=member,
                operation=op,
                import_wh=10.0,
                production_wh=50.0,
                bucket=bucket,
            )
        await _roll(db_session, community.id, bucket)

        series = (
            await client.get(
                f"/operations/{op}/series", headers=manager_headers, params={"resolution": "hour"}
            )
        ).json()["data"]

        point = _point(series, bucket)
        assert {"import_wh", "export_wh", "shared_wh"}.isdisjoint(point)
        assert point["production_wh"] == pytest.approx(400.0), "production is never subject to k"
        assert series["suppressed_buckets"] == 1
        assert {"import_wh", "export_wh", "shared_wh"} <= {a["term"] for a in series["absent"]}

    async def test_operation_zero_is_not_addressable(self, client, community, manager_headers):
        """The remainder row is never an operation of its own."""
        response = await client.get("/operations/0/series", headers=manager_headers)
        assert response.status_code == 422


class TestTheCommunityTotal:
    async def test_one_household_outside_every_operation_hides_the_total(
        self, client, db_session, community, manager_headers
    ):
        """Four members, k = 3: the community alone would pass. But total minus
        the visible operation is the one household in no operation, so the
        total's grid terms are withheld - in the series and in the summary."""
        await _k(db_session, community.id)
        bucket = _bucket()
        op = await create_operation(db_session, id_community=community.id, name="Op")
        for member in (1, 2, 3):
            await _site(
                db_session, community.id, member=member, operation=op, import_wh=10.0, bucket=bucket
            )
        await _site(
            db_session, community.id, member=4, operation=None, import_wh=99.0, bucket=bucket
        )
        await _roll(db_session, community.id, bucket)

        series = (
            await client.get("/series", headers=manager_headers, params={"resolution": "hour"})
        ).json()["data"]
        assert {"import_wh", "export_wh", "shared_wh"}.isdisjoint(_point(series, bucket))
        summary = (await client.get("/summary", headers=manager_headers)).json()["data"]
        assert {"import_wh", "export_wh", "shared_wh"}.isdisjoint(summary)
        # The operation itself is still shown: three members, its own k.
        op_series = (
            await client.get(
                f"/operations/{op}/series", headers=manager_headers, params={"resolution": "hour"}
            )
        ).json()["data"]
        assert _point(op_series, bucket)["import_wh"] == pytest.approx(120.0)

    @pytest.mark.parametrize("resolution", ["hour", "quarter"])
    async def test_two_operations_never_share_with_each_other(
        self, client, db_session, community, manager_headers, resolution
    ):
        """POSITIVE CONTROL for the total, and the reason it is a SUM over
        operations: one operation only produces, the other only consumes. A
        community-wide LEAST would report energy shared; nothing was."""
        await _k(db_session, community.id)
        bucket = _bucket()
        producers = await create_operation(db_session, id_community=community.id, name="P")
        consumers = await create_operation(db_session, id_community=community.id, name="C")
        for member in (1, 2, 3):
            await _site(
                db_session,
                community.id,
                member=member,
                operation=producers,
                export_wh=50.0,
                bucket=bucket,
            )
        for member in (4, 5, 6):
            await _site(
                db_session,
                community.id,
                member=member,
                operation=consumers,
                import_wh=50.0,
                bucket=bucket,
            )
        await _roll(db_session, community.id, bucket)

        series = (
            await client.get("/series", headers=manager_headers, params={"resolution": resolution})
        ).json()["data"]

        points = [p for p in series["points"] if "import_wh" in p]
        assert points, "every operation is at k and nothing is outside them: the total shows"
        assert all(p["shared_wh"] == 0.0 for p in points)
        assert all(p["import_wh"] > 0 and p["export_wh"] > 0 for p in points)

    async def test_power_never_publishes_export_below_k(
        self, client, db_session, community, manager_headers
    ):
        """Net meters cannot see production, and `power_w` used to fall back to
        their EXPORT with no k check at all."""
        await _k(db_session, community.id)
        bucket = _bucket()
        for member in (1, 2):
            await _site(
                db_session,
                community.id,
                member=member,
                operation=None,
                export_wh=80.0,
                bucket=bucket,
            )
        await _roll(db_session, community.id, bucket)

        summary = (await client.get("/summary", headers=manager_headers)).json()["data"]

        assert "power_w" not in summary


class TestTheMembersOwnOperation:
    """A member sees ONLY the operation(s) they hold an ACTIVE meter in."""

    async def _member_in(
        self, session: AsyncSession, id_community: int, operation: int, bucket, **site
    ) -> None:
        member = await create_member(session, id_community=id_community)
        await link_user_to_member(session, auth_user_id=MEMBER_SUB, id_member=member)
        await _site(
            session, id_community, member=member, operation=operation, bucket=bucket, **site
        )

    @pytest.fixture
    def headers(self, community):
        return gateway_headers(community.auth_community_id, role="MEMBER", user_id=MEMBER_SUB)

    async def test_the_member_lists_only_the_operation_they_hold(
        self, client, db_session, community, headers
    ):
        bucket = _bucket()
        mine = await create_operation(db_session, id_community=community.id, name="Mine")
        other = await create_operation(db_session, id_community=community.id, name="Other")
        await self._member_in(db_session, community.id, mine, bucket, production_wh=50.0)
        await _site(db_session, community.id, member=999, operation=other, bucket=bucket)
        await _roll(db_session, community.id, bucket)

        response = await client.get("/mine/operations", headers=headers)

        assert response.status_code == 200
        assert response.json()["data"] == [{"id": mine, "name": "Mine"}]

    async def test_the_series_carries_export_for_net_meters_and_never_a_grid_import(
        self, client, db_session, community, headers
    ):
        """Prosumers: no production term (protocol 3.4), so the export is the
        figure - under the operation's k. Import and the shared estimate never
        appear, anywhere in the body."""
        await _k(db_session, community.id)
        bucket = _bucket()
        op = await create_operation(db_session, id_community=community.id, name="Op")
        await self._member_in(db_session, community.id, op, bucket, import_wh=5.0, export_wh=20.0)
        # Ids no real `member` row can take in this run - the identity sequence is
        # not rolled back between tests, so a small literal can collide.
        for member in (9002, 9003):
            await _site(
                db_session,
                community.id,
                member=member,
                operation=op,
                import_wh=5.0,
                export_wh=20.0,
                bucket=bucket,
            )
        await _roll(db_session, community.id, bucket)

        response = await client.get(
            f"/mine/operations/{op}/series", headers=headers, params={"resolution": "hour"}
        )

        assert response.status_code == 200
        body = response.text
        assert "import_wh" not in body and "shared_wh" not in body
        assert _point(response.json()["data"], bucket)["export_wh"] == pytest.approx(240.0)

    async def test_below_k_the_member_gets_no_export(self, client, db_session, community, headers):
        await _k(db_session, community.id)
        bucket = _bucket()
        op = await create_operation(db_session, id_community=community.id, name="Op")
        await self._member_in(db_session, community.id, op, bucket, export_wh=20.0)
        await _roll(db_session, community.id, bucket)

        data = (
            await client.get(
                f"/mine/operations/{op}/series", headers=headers, params={"resolution": "hour"}
            )
        ).json()["data"]

        assert "export_wh" not in _point(data, bucket)
        assert data["suppressed_buckets"] == 1

    async def test_an_operation_the_member_does_not_hold_is_not_found(
        self, client, db_session, community, headers
    ):
        """404, the answer a non-existent operation gets - never a 403 that
        confirms it exists."""
        bucket = _bucket()
        mine = await create_operation(db_session, id_community=community.id, name="Mine")
        other = await create_operation(db_session, id_community=community.id, name="Other")
        await self._member_in(db_session, community.id, mine, bucket, production_wh=50.0)
        await _site(db_session, community.id, member=999, operation=other, bucket=bucket)
        await _roll(db_session, community.id, bucket)

        response = await client.get(f"/mine/operations/{other}/series", headers=headers)

        assert response.status_code == 404
        assert response.json()["error_code"] == 2446

    async def test_the_member_cannot_reach_any_manager_read(
        self, client, db_session, community, headers
    ):
        bucket = _bucket()
        op = await create_operation(db_session, id_community=community.id, name="Mine")
        await self._member_in(db_session, community.id, op, bucket, production_wh=50.0)
        await _roll(db_session, community.id, bucket)

        for url in ("/operations", f"/operations/{op}/series", f"/operations/{op}/summary"):
            response = await client.get(url, headers=headers)
            assert response.status_code == 403, url
            assert response.json()["error_code"] == 2, url

    def test_the_member_point_cannot_carry_import_or_shared(self):
        """The guard is the TYPE: a member payload cannot express these terms,
        so no future branch can forget to clear them."""
        assert set(MemberOperationPointOut.model_fields) == {
            "bucket",
            "production_wh",
            "export_wh",
            "n_devices",
        }
