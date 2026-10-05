"""The authenticated device surface: list, create, token, revoke.

The tenancy cases are the ones worth having. Everything else here is a thin
orchestration over a repository; a cross-tenant read or revoke is the failure
that would not announce itself.
"""

import uuid

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from api.live.deps import get_device_broker, get_live_data_service
from api.live.repository import LiveRepository
from api.live.service import LiveDataService
from main import app
from ports.broker import BrokerCommandFailedError, FakeDeviceBroker
from ports.crm_read import FakeCrmRead, MeterSnapshot
from shared.const import DeviceStatus
from shared.models.local_models import DeviceModel
from tests.conftest import gateway_headers
from tests.factories.subscription_factory import create_community, create_subscription

EAN = "541448200000000001"
OTHER_EAN = "541448200000000002"


def _meter(ean: str, id_community: int, capacity: float | None = 3.0) -> MeterSnapshot:
    return MeterSnapshot(
        ean=ean,
        id_community=id_community,
        capacity_kva=capacity,
        injection_status=1,
        production_chain=1,
    )


def _wire(db_session: AsyncSession, broker: FakeDeviceBroker, crm: FakeCrmRead) -> None:
    def _service() -> LiveDataService:
        return LiveDataService(
            local_session=db_session,
            crm_session=db_session,
            repository=LiveRepository(db_session),
            crm_read=crm,
            broker=broker,
        )

    app.dependency_overrides[get_live_data_service] = _service
    app.dependency_overrides[get_device_broker] = lambda: broker


@pytest.fixture
def broker() -> FakeDeviceBroker:
    return FakeDeviceBroker()


@pytest.fixture
def crm(community) -> FakeCrmRead:
    return FakeCrmRead(
        meters={
            (EAN, community.id): _meter(EAN, community.id),
            (OTHER_EAN, community.id): _meter(OTHER_EAN, community.id, capacity=None),
        }
    )


async def _create(client: AsyncClient, headers: dict, *, ean: str = EAN, name: str = "Roof PV"):
    return await client.post(
        "/devices",
        json={"name": name, "ean": ean, "type": 1, "pure_injection": True},
        headers=headers,
    )


class TestCreate:
    async def test_it_snapshots_the_capacity_from_the_crm(
        self, client: AsyncClient, db_session, community, manager_headers, broker, crm
    ):
        """Deviation 4: the CRM column is kVA - the AC INJECTION ceiling, not the
        DC panel peak - so the snapshot column is named with its unit and the
        ingest check clips against it rather than trusting it."""
        _wire(db_session, broker, crm)
        response = await _create(client, manager_headers)

        assert response.status_code == 200
        assert response.json()["data"]["capacity_kva"] == 3.0
        assert response.json()["data"]["status"] == int(DeviceStatus.PENDING)

    async def test_an_unknown_ean_is_refused(
        self, client: AsyncClient, db_session, community, manager_headers, broker, crm
    ):
        """`device.ean` is a plain column in another database and never a
        foreign key, so this is the ONLY thing that will ever validate it. A
        typo'd EAN otherwise produces a device that ingests happily and is
        attributed to nobody - found months later with real data against it."""
        _wire(db_session, broker, crm)
        response = await _create(client, manager_headers, ean="000000000000000000")

        assert response.status_code == 422
        assert response.json()["error_code"] == 2404

    async def test_a_meter_with_no_declared_capacity_is_still_allowed(
        self, client: AsyncClient, db_session, community, manager_headers, broker, crm
    ):
        """NULL capacity is common - not every meter_data row declares one - and
        it must not become a blanket refusal. Ingest skips the ceiling half of
        `implausible_production` for such a device."""
        _wire(db_session, broker, crm)
        response = await _create(client, manager_headers, ean=OTHER_EAN)

        assert response.status_code == 200
        assert response.json()["data"]["capacity_kva"] is None

    async def test_a_second_live_device_for_one_meter_is_refused(
        self, client: AsyncClient, db_session, community, manager_headers, broker, crm
    ):
        """Mirrors `uq_device_community_ean_live`, so the clash is a clean 409
        rather than an IntegrityError surfacing as a 400."""
        _wire(db_session, broker, crm)
        assert (await _create(client, manager_headers)).status_code == 200
        second = await _create(client, manager_headers)

        assert second.status_code == 409
        assert second.json()["error_code"] == 2405

    async def test_a_member_cannot_create_a_device(
        self, client: AsyncClient, db_session, community, member_headers, broker, crm
    ):
        """Phase 1 is administrator-facing (D-5). Every route on this surface is
        manager-only, and there is deliberately no member-visible endpoint."""
        _wire(db_session, broker, crm)
        assert (await _create(client, member_headers)).status_code == 403


class TestTokens:
    async def test_a_token_comes_back_once_with_a_qr(
        self, client: AsyncClient, db_session, community, manager_headers, broker, crm
    ):
        """The QR is inline rather than a second endpoint: a URL carrying a
        credential lands in nginx's and KrakenD's access logs."""
        _wire(db_session, broker, crm)
        device_id = (await _create(client, manager_headers)).json()["data"]["device_id"]

        response = await client.post(f"/devices/{device_id}/token", headers=manager_headers)

        assert response.status_code == 200
        data = response.json()["data"]
        assert data["token"]
        assert data["qr_svg"].startswith("data:image/svg+xml;base64,")

    async def test_reissuing_invalidates_the_previous_token(
        self, client: AsyncClient, db_session, community, manager_headers, broker, crm
    ):
        """`uq_enrollment_token_device_unconsumed` allows at most one open token
        per device, so a reissue must close the old one rather than collide with
        it."""
        from shared.models.local_models import EnrollmentTokenModel

        _wire(db_session, broker, crm)
        device_id = (await _create(client, manager_headers)).json()["data"]["device_id"]
        await client.post(f"/devices/{device_id}/token", headers=manager_headers)
        await client.post(f"/devices/{device_id}/token", headers=manager_headers)

        rows = (await db_session.execute(select(EnrollmentTokenModel))).scalars().all()
        open_tokens = [r for r in rows if r.consumed_at is None]
        assert len(open_tokens) == 1, "more than one token is redeemable at once"
        assert len(rows) >= 2, "the superseded token was deleted rather than closed"


class TestRevoke:
    async def test_it_disables_clears_the_retained_status_then_deletes(
        self, client: AsyncClient, db_session, community, manager_headers, broker, crm
    ):
        """The ORDER, and the middle step is the reason for it.

        Phase 0 measured that `deleteClient` alone DOES cut a live connection
        (47 ms) and refuse reconnection, so the plan's stated justification was
        false. The order survives because the retained-status clear has to
        happen while there is still a device to clear it for - a retained
        `status` that outlives its device replays on every worker reconnect, so
        the offline alert fires on every deploy until the team stops reading it.
        """
        _wire(db_session, broker, crm)
        device_id = (await _create(client, manager_headers)).json()["data"]["device_id"]

        response = await client.post(f"/devices/{device_id}/revoke", headers=manager_headers)

        assert response.status_code == 200
        assert response.json()["data"]["status"] == int(DeviceStatus.REVOKED)
        verbs = [verb for verb, _ in broker.calls]
        assert verbs == ["disable_device", "clear_retained_status", "delete_device"]
        assert broker.cleared_topics == [f"ce/{community.id}/{device_id}/status"]

    async def test_revoking_twice_is_refused(
        self, client: AsyncClient, db_session, community, manager_headers, broker, crm
    ):
        _wire(db_session, broker, crm)
        device_id = (await _create(client, manager_headers)).json()["data"]["device_id"]
        await client.post(f"/devices/{device_id}/revoke", headers=manager_headers)

        again = await client.post(f"/devices/{device_id}/revoke", headers=manager_headers)
        assert again.status_code == 409

    async def test_the_meter_can_be_re_enrolled_after_a_revoke(
        self, client: AsyncClient, db_session, community, manager_headers, broker, crm
    ):
        """`uq_device_community_ean_live` is PARTIAL for this reason: the
        password is shown once and is never recoverable, so re-enrolling the
        same meter is the normal recovery path."""
        _wire(db_session, broker, crm)
        device_id = (await _create(client, manager_headers)).json()["data"]["device_id"]
        await client.post(f"/devices/{device_id}/revoke", headers=manager_headers)

        assert (await _create(client, manager_headers)).status_code == 200

    async def test_a_device_that_was_never_enrolled_can_still_be_revoked(
        self, client: AsyncClient, db_session, community, manager_headers, broker, crm
    ):
        """A typo'd EAN must be recoverable without hand-editing the database.

        `POST /devices` writes the row and nothing else - there is no broker
        client until a device enrols - so revoking here asks dynsec to disable a
        username it has never heard of, and dynsec answers `Client not found.`.
        That used to be a 502: the row stayed PENDING, and because
        `uq_device_community_ean_live` excludes only REVOKED rows, the EAN was
        locked for ever. Every route to fixing a mistyped meter went through
        psql.

        Found by scripts/verify-live-ingest.sh against the real broker. The
        suite could not have found it: FakeDeviceBroker used to allow disabling
        a client it had never created.
        """
        _wire(db_session, broker, crm)
        device_id = (await _create(client, manager_headers)).json()["data"]["device_id"]
        assert broker.clients == {}, "a created device must have no broker client yet"

        response = await client.post(f"/devices/{device_id}/revoke", headers=manager_headers)

        assert response.status_code == 200
        assert response.json()["data"]["status"] == int(DeviceStatus.REVOKED)
        # Both client commands were attempted and both found nothing; the
        # retained clear ran between them regardless.
        assert [verb for verb, _ in broker.calls] == [
            "disable_device",
            "clear_retained_status",
            "delete_device",
        ]
        # And the point of the whole thing: the meter is usable again.
        assert (await _create(client, manager_headers)).status_code == 200

    async def test_a_real_broker_failure_is_still_a_502(
        self, client: AsyncClient, db_session, community, manager_headers, broker, crm
    ):
        """The counterpart, and the reason the fix is per-command.

        Tolerating a missing client widens what revoke swallows. A fix that
        swallowed every BrokerError would pass the test above and leave the
        worse half-failure in place: the row marked REVOKED while the client is
        still alive and publishing, which is precisely what revoke's
        broker-first ordering exists to prevent.
        """
        _wire(db_session, broker, crm)
        device_id = (await _create(client, manager_headers)).json()["data"]["device_id"]
        broker.fail_with["disable_device"] = BrokerCommandFailedError(
            "disableClient", "Internal error"
        )

        response = await client.post(f"/devices/{device_id}/revoke", headers=manager_headers)

        assert response.status_code == 502
        device = await db_session.scalar(
            select(DeviceModel).where(DeviceModel.public_id == uuid.UUID(device_id))
        )
        assert device is not None
        assert device.status != int(
            DeviceStatus.REVOKED
        ), "a broker failure must not leave the row claiming the device is gone"


class TestTenancy:
    """The failures that would not announce themselves."""

    async def test_the_list_shows_only_the_callers_community(
        self, client: AsyncClient, db_session, community, manager_headers, broker, crm
    ):
        """The list read that `with_community_scope` exists for. A tenancy bug
        shows up HERE - as another community's meters - long before it shows up
        on a by-id lookup."""
        _wire(db_session, broker, crm)
        await _create(client, manager_headers)

        other = await create_community(db_session)
        await create_subscription(db_session, id_community=other.id, is_active=True)
        other_headers = gateway_headers(other.auth_community_id, role="MANAGER")
        other_crm = FakeCrmRead(meters={(EAN, other.id): _meter(EAN, other.id)})
        _wire(db_session, broker, other_crm)
        await _create(client, other_headers, name="Their PV")

        mine = (await client.get("/devices", headers=manager_headers)).json()["data"]
        theirs = (await client.get("/devices", headers=other_headers)).json()["data"]

        assert [d["name"] for d in mine] == ["Roof PV"]
        assert [d["name"] for d in theirs] == ["Their PV"]

    async def test_another_communitys_device_is_404_and_not_403(
        self, client: AsyncClient, db_session, community, manager_headers, broker, crm
    ):
        """404, deliberately.

        The read is community-scoped, so another community's device is
        indistinguishable from one that does not exist. A 403 would CONFIRM it
        exists, which is a membership oracle over the whole platform.
        """
        _wire(db_session, broker, crm)
        device_id = (await _create(client, manager_headers)).json()["data"]["device_id"]

        other = await create_community(db_session)
        await create_subscription(db_session, id_community=other.id, is_active=True)
        other_headers = gateway_headers(other.auth_community_id, role="MANAGER")

        response = await client.post(f"/devices/{device_id}/revoke", headers=other_headers)

        assert response.status_code == 404
        assert response.json()["error_code"] == 2402
        # And nothing reached the broker.
        assert not broker.calls

    async def test_an_unknown_device_id_is_404(
        self, client: AsyncClient, db_session, community, manager_headers, broker, crm
    ):
        _wire(db_session, broker, crm)
        response = await client.post(f"/devices/{uuid.uuid4()}/revoke", headers=manager_headers)
        assert response.status_code == 404
