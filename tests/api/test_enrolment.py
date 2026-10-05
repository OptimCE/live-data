"""The public enrolment leg.

Driven through the real HTTP stack so `GatewayScopeMiddleware`, the router's
(absent) dependencies and the error handlers are all exercised - the things that
would make a public endpoint behave differently from a unit-tested service.
"""

import datetime
import uuid

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from api.live.deps import get_device_broker, get_enrolment_service
from api.live.repository import EnrolmentRepository
from api.live_public.service import EnrolmentService
from domain import tokens
from main import app
from ports.broker import (
    BrokerUnavailableError,
    DeviceCredentials,
    FakeDeviceBroker,
)
from ports.crm_read import CrmReadPort, FakeCrmRead, SqlAlchemyCrmRead
from shared.const import DeviceStatus, DeviceType, FeatureName
from shared.models.local_models import DeviceModel, EnrollmentTokenModel

ENROL = "/enroll"
CONNECTOR = {"name": "optimce-connector", "version": "0.3.1"}


async def _make_device(
    session: AsyncSession, *, id_community: int, status: int = int(DeviceStatus.PENDING)
) -> DeviceModel:
    device = DeviceModel(
        public_id=uuid.uuid4(),
        id_community=id_community,
        type=int(DeviceType.PRODUCTION),
        ean=f"5414482{uuid.uuid4().int % 10**11:011d}",
        name="Roof PV",
        status=status,
        pure_injection=True,
        capacity_kva=3.0,
    )
    session.add(device)
    await session.flush()
    return device


async def _make_token(
    session: AsyncSession,
    device: DeviceModel,
    *,
    expires_in_hours: int = 72,
    consumed: bool = False,
    claimed_until: datetime.datetime | None = None,
) -> str:
    plaintext = tokens.generate()
    normalised = tokens.normalise(plaintext)
    assert normalised is not None
    now = datetime.datetime.now(datetime.UTC)
    session.add(
        EnrollmentTokenModel(
            id_device=device.id,
            id_community=device.id_community,
            token_hash=tokens.hash_token(normalised),
            expires_at=now + datetime.timedelta(hours=expires_in_hours),
            consumed_at=now if consumed else None,
            claimed_until=claimed_until,
        )
    )
    await session.flush()
    return plaintext


def _wire(db_session: AsyncSession, broker: FakeDeviceBroker, crm: CrmReadPort) -> None:
    """Point the public route at the fakes, keeping the real service."""

    def _service() -> EnrolmentService:
        return EnrolmentService(
            local_session=db_session,
            crm_session=db_session,
            repository=EnrolmentRepository(db_session),
            crm_read=crm,
            broker=broker,
            lease_seconds=30,
        )

    app.dependency_overrides[get_enrolment_service] = _service
    app.dependency_overrides[get_device_broker] = lambda: broker


@pytest.fixture
def broker() -> FakeDeviceBroker:
    return FakeDeviceBroker()


@pytest.fixture
def crm(community) -> FakeCrmRead:
    return FakeCrmRead(subscribed={(community.id, FeatureName.LIVE_DATA.value)})


class TestTheHappyPath:
    async def test_a_valid_token_returns_everything_the_device_needs(
        self, client: AsyncClient, db_session, community, broker, crm
    ):
        """protocol 5.2. One call, because the device may be an ESP32 being
        configured through a captive portal by someone holding a phone."""
        device = await _make_device(db_session, id_community=community.id)
        token = await _make_token(db_session, device)
        _wire(db_session, broker, crm)

        response = await client.post(ENROL, json={"token": token, "connector": CONNECTOR})

        assert response.status_code == 200
        data = response.json()["data"]
        assert data["device_id"] == str(device.public_id)
        assert data["credentials"]["username"] == str(device.public_id)
        assert data["credentials"]["password"]
        assert data["topics"]["telemetry"] == f"ce/{community.id}/{device.public_id}/telemetry"
        assert data["topics"]["status"] == f"ce/{community.id}/{device.public_id}/status"

    async def test_the_broker_host_is_the_public_one(
        self, client: AsyncClient, db_session, community, broker, crm
    ):
        """The address goes into the device's NVS and is never asked for again.

        Handing out the compose service name would brick every enrolled device
        permanently - there is no OTA, and the fix is a site visit to a box with
        no keyboard.
        """
        from core.config import settings

        device = await _make_device(db_session, id_community=community.id)
        token = await _make_token(db_session, device)
        _wire(db_session, broker, crm)

        data = (await client.post(ENROL, json={"token": token, "connector": CONNECTOR})).json()[
            "data"
        ]

        assert data["broker"]["host"] == settings.BROKER_PUBLIC_HOST
        assert data["broker"]["host"] != settings.MQTT_HOST

    async def test_the_client_id_is_pinned_at_the_broker(
        self, client: AsyncClient, db_session, community, broker, crm
    ):
        """Phase 0 Q1d, which the plan never states.

        A dynsec client created with `clientid` set cannot connect with any
        other id. That enforces protocol 4.3 AT THE BROKER rather than by asking
        connector authors nicely.
        """
        device = await _make_device(db_session, id_community=community.id)
        token = await _make_token(db_session, device)
        _wire(db_session, broker, crm)

        await client.post(ENROL, json={"token": token, "connector": CONNECTOR})

        created = broker.clients[str(device.public_id)]
        assert created.client_id == str(device.public_id)
        assert broker.roles[str(device.public_id)] == "device"

    async def test_the_device_becomes_active_and_records_its_connector(
        self, client: AsyncClient, db_session, community, broker, crm
    ):
        """protocol 3.2: connector and version are recorded, so "which devices
        run the broken 0.3.0?" has an answer."""
        device = await _make_device(db_session, id_community=community.id)
        token = await _make_token(db_session, device)
        _wire(db_session, broker, crm)

        await client.post(ENROL, json={"token": token, "connector": CONNECTOR})

        await db_session.refresh(device)
        assert device.status == int(DeviceStatus.ACTIVE)
        assert device.enrolled_at is not None
        assert device.connector_name == "optimce-connector"
        assert device.connector_version == "0.3.1"

    async def test_a_token_typed_without_hyphens_still_works(
        self, client: AsyncClient, db_session, community, broker, crm
    ):
        """The whole reason for Crockford base32."""
        device = await _make_device(db_session, id_community=community.id)
        token = await _make_token(db_session, device)
        _wire(db_session, broker, crm)

        response = await client.post(
            ENROL, json={"token": token.replace("-", "").lower(), "connector": CONNECTOR}
        )
        assert response.status_code == 200


class TestSingleUse:
    async def test_the_same_token_twice_is_refused_and_creates_no_second_client(
        self, client: AsyncClient, db_session, community, broker, crm
    ):
        """§14 criterion 4: a consumed token is 4xx and creates NO dynsec client.

        Counting the clients is the assertion that matters - a 4xx with a client
        created anyway would be the worst of both.
        """
        device = await _make_device(db_session, id_community=community.id)
        token = await _make_token(db_session, device)
        _wire(db_session, broker, crm)

        first = await client.post(ENROL, json={"token": token, "connector": CONNECTOR})
        assert first.status_code == 200
        clients_after_first = dict(broker.clients)

        second = await client.post(ENROL, json={"token": token, "connector": CONNECTOR})

        assert 400 <= second.status_code < 500
        assert broker.clients == clients_after_first

    async def test_an_expired_token_is_refused(
        self, client: AsyncClient, db_session, community, broker, crm
    ):
        device = await _make_device(db_session, id_community=community.id)
        token = await _make_token(db_session, device, expires_in_hours=-1)
        _wire(db_session, broker, crm)

        response = await client.post(ENROL, json={"token": token, "connector": CONNECTOR})

        assert response.status_code == 400
        assert not broker.clients

    async def test_unknown_expired_and_consumed_are_indistinguishable(
        self, client: AsyncClient, db_session, community, broker, crm
    ):
        """One opaque answer for all of them.

        They share a remediation - ask for a new code - and distinguishing them
        turns this endpoint into a token oracle that tells an attacker when they
        have part of it right.
        """
        device = await _make_device(db_session, id_community=community.id)
        expired = await _make_token(db_session, device, expires_in_hours=-1)
        other = await _make_device(db_session, id_community=community.id)
        consumed = await _make_token(db_session, other, consumed=True)
        unknown = tokens.generate()
        _wire(db_session, broker, crm)

        answers = [
            (await client.post(ENROL, json={"token": t, "connector": CONNECTOR})).json()
            for t in (expired, consumed, unknown)
        ]
        codes = {a["error_code"] for a in answers}
        messages = {a["data"] for a in answers}

        assert len(codes) == 1, f"the three states are distinguishable: {codes}"
        assert len(messages) == 1, f"the three states leak through the message: {messages}"

    async def test_a_malformed_token_answers_the_same_way(
        self, client: AsyncClient, db_session, community, broker, crm
    ):
        """Even the SHAPE must not be confirmable."""
        _wire(db_session, broker, crm)
        unknown = (
            await client.post(ENROL, json={"token": tokens.generate(), "connector": CONNECTOR})
        ).json()
        malformed = (
            await client.post(ENROL, json={"token": "NOT-A-REAL-CODE", "connector": CONNECTOR})
        ).json()

        assert malformed["error_code"] == unknown["error_code"]


class TestTheClaimLease:
    async def test_a_live_claim_is_refused_with_its_own_code(
        self, client: AsyncClient, db_session, community, broker, crm
    ):
        """A concurrent enrolment holds the lease.

        Distinct from the token errors because the remediation is different -
        wait a moment - and because there is nothing left to leak: whoever sent
        this already holds a valid token.
        """
        device = await _make_device(db_session, id_community=community.id)
        future = datetime.datetime.now(datetime.UTC) + datetime.timedelta(seconds=30)
        token = await _make_token(db_session, device, claimed_until=future)
        _wire(db_session, broker, crm)

        response = await client.post(ENROL, json={"token": token, "connector": CONNECTOR})

        assert response.status_code == 409
        assert response.json()["error_code"] == 2412
        assert not broker.clients

    async def test_an_expired_claim_can_be_re_claimed(
        self, client: AsyncClient, db_session, community, broker, crm
    ):
        """THE POINT OF THE LEASE.

        A 504 between the broker command and the response leaves a claim that
        simply expires, rather than burning a credential the member never
        received. The next attempt must succeed.
        """
        device = await _make_device(db_session, id_community=community.id)
        past = datetime.datetime.now(datetime.UTC) - datetime.timedelta(seconds=1)
        token = await _make_token(db_session, device, claimed_until=past)
        _wire(db_session, broker, crm)

        response = await client.post(ENROL, json={"token": token, "connector": CONNECTOR})

        assert response.status_code == 200


class TestTheAlreadyExistsBranch:
    async def test_a_retry_over_an_existing_client_sets_a_password_and_succeeds(
        self, client: AsyncClient, db_session, community, broker, crm
    ):
        """§8.2's CORRECTED retry path, and the one the plan originally missed.

        `createClient` is NOT idempotent - it answers "Client already exists".
        The client exists here because a previous attempt got far enough to
        create it: a 3000 ms gateway cut on a command that succeeded
        server-side. Without this branch the retry fails permanently and burns
        exactly the credential the lease exists to protect.
        """
        device = await _make_device(db_session, id_community=community.id)
        token = await _make_token(db_session, device)
        # Set the scene FAITHFULLY: the client is already there, so the fake
        # raises already-exists the same way dynsec does - and, crucially, the
        # subsequent setClientPassword has something to act on. Priming
        # `fail_with` instead produces a state that cannot occur in production
        # (refused as existing, yet absent), and the test would then be
        # asserting against a fiction.
        broker.clients[str(device.public_id)] = DeviceCredentials(
            username=str(device.public_id), password="stale", client_id=str(device.public_id)
        )
        _wire(db_session, broker, crm)

        response = await client.post(ENROL, json={"token": token, "connector": CONNECTOR})

        assert response.status_code == 200
        assert ("set_device_password", str(device.public_id)) in broker.calls
        # And the device really is enrolled, not merely answered.
        await db_session.refresh(device)
        assert device.status == int(DeviceStatus.ACTIVE)


class TestTheOrderingRule:
    async def test_a_broker_failure_leaves_the_token_unconsumed(
        self, client: AsyncClient, db_session, community, broker, crm
    ):
        """Broker first, commit second - and this is why.

        Of the two half-failures only one is recoverable. A device that believes
        it is enrolled, holding a secret that authenticates nothing, with its
        one-time token consumed, is a site visit to a keyboard-less box in a
        basement. So nothing is consumed until the broker has answered.
        """
        device = await _make_device(db_session, id_community=community.id)
        token = await _make_token(db_session, device)
        broker.fail_with["create_device"] = BrokerUnavailableError("broker down")
        _wire(db_session, broker, crm)

        response = await client.post(ENROL, json={"token": token, "connector": CONNECTOR})

        assert response.status_code == 503
        row = (
            await db_session.execute(
                select(EnrollmentTokenModel).where(EnrollmentTokenModel.id_device == device.id)
            )
        ).scalar_one()
        assert row.consumed_at is None, "the token was burned by a failed broker call"
        await db_session.refresh(device)
        assert device.status == int(DeviceStatus.PENDING)


class TestTheSubscriptionCheck:
    async def test_a_lapsed_community_cannot_enrol(
        self, client: AsyncClient, db_session, unsubscribed_community, broker
    ):
        """§8.4 trap 3. `require_feature` cannot run on this leg at all.

        It calls `require_community()` (401 with no community header) and its
        query goes through `with_community_scope`, which yields `WHERE false`
        when the ContextVar is unset - so a naive check rejects EVERY enrolment
        or accepts every lapsed one, silently, depending on which way round it
        is written.
        """
        device = await _make_device(db_session, id_community=unsubscribed_community.id)
        token = await _make_token(db_session, device)
        _wire(db_session, broker, FakeCrmRead(subscribed=set()))

        response = await client.post(ENROL, json={"token": token, "connector": CONNECTOR})

        assert response.status_code == 403
        assert response.json()["error_code"] == 2430
        assert not broker.clients
        # And the token SURVIVES the refusal (D-12). It is claimed, not consumed,
        # so once the module is back on - and the claim lease has run out - the
        # same token enrols, with no new one to mint and hand over.
        row = (
            await db_session.execute(
                select(EnrollmentTokenModel).where(EnrollmentTokenModel.id_device == device.id)
            )
        ).scalar_one()
        assert row.consumed_at is None, "a refused enrolment burned its token"

    async def test_a_switched_off_community_cannot_enrol_either(
        self, client: AsyncClient, db_session, deactivated_community, broker
    ):
        """The `is_active = false` row crm-backend's unsubscribe leaves, read
        through the REAL adapter rather than a fake - "a row exists" is the check
        that would wave this one through."""
        device = await _make_device(db_session, id_community=deactivated_community.id)
        token = await _make_token(db_session, device)
        _wire(db_session, broker, SqlAlchemyCrmRead(db_session))

        response = await client.post(ENROL, json={"token": token, "connector": CONNECTOR})

        assert response.status_code == 403
        assert response.json()["error_code"] == 2430
        assert not broker.clients
        row = (
            await db_session.execute(
                select(EnrollmentTokenModel).where(EnrollmentTokenModel.id_device == device.id)
            )
        ).scalar_one()
        assert row.consumed_at is None

    async def test_the_check_uses_the_device_community_not_a_header(
        self, client: AsyncClient, db_session, community, broker
    ):
        """A forged X-Community-ID must not decide the answer.

        The header reaches this handler - `auth: false` removes the producer of
        the `x-user-*` headers, not the gateway's input-header allow-list - so
        the only thing that stops it mattering is that nothing here reads it.
        """
        device = await _make_device(db_session, id_community=community.id)
        token = await _make_token(db_session, device)
        crm = FakeCrmRead(subscribed={(community.id, FeatureName.LIVE_DATA.value)})
        _wire(db_session, broker, crm)

        response = await client.post(
            ENROL,
            json={"token": token, "connector": CONNECTOR},
            headers={"X-Community-Id": "999999", "X-User-Id": "attacker"},
        )

        assert response.status_code == 200
        # The subscription was checked against the DEVICE's community.
        assert ("is_feature_active_unscoped", (community.id, "live-data")) in crm.calls
        assert not any(
            call == ("is_feature_active_unscoped", (999999, "live-data")) for call in crm.calls
        )
