"""The dynsec adapter's surface, without a broker.

There is no Mosquitto container here, and that is D-7 rather than an omission:
GitHub Actions creates `services:` containers BEFORE `actions/checkout`, so a
repo-tracked mosquitto.conf cannot be their bind-mount source. The house policy
already says as much for NATS - the sibling services' `test_nats_resilience.py`
states outright that the real reconnection behaviour "can only be verified
end-to-end with a real broker", and unit-tests only the connect kwargs and the
publish timeout.

So what is pinned here is the SURFACE WE EXPOSE TO THE BROKER, which is where
every Phase 0 finding lives. The end-to-end half runs against the dev stack from
the monorepo's `scripts/verify-live-ingest.sh`.
"""

import asyncio
import json
from typing import Any, cast

import pytest

from ports.broker import (
    BrokerClientExistsError,
    BrokerCommandFailedError,
    BrokerTimeoutError,
    BrokerUnavailableError,
    DeviceCredentials,
)
from ports.broker_mqtt import MqttDeviceBroker
from shared.const import DYNSEC_REQUEST_TOPIC


class FakeMqttClient:
    """Records publishes. Answers when the test tells it to."""

    def __init__(self) -> None:
        self.published: list[tuple[str, str]] = []
        self.retained: list[tuple[str, bytes, bool]] = []

    async def publish(self, topic, payload=None, qos=0, retain=False):
        if isinstance(payload, bytes):
            self.retained.append((str(topic), payload, retain))
        else:
            self.published.append((str(topic), payload))

    @property
    def commands(self) -> list[dict]:
        """The decoded command objects, one per publish."""
        return [json.loads(p)["commands"][0] for _, p in self.published]


@pytest.fixture
def broker() -> MqttDeviceBroker:
    """An adapter wired to a fake transport and marked ready.

    `start()` is not called: the reconnect supervisor is the one thing here that
    genuinely needs a broker, and the sibling services draw the same line.
    """
    adapter = MqttDeviceBroker()
    # ONE cast, here, rather than a `type: ignore` at every call site: the
    # adapter's `_client` is an `aiomqtt.Client`, and substituting a
    # stand-in is the whole seam this file tests.
    adapter._client = cast(Any, FakeMqttClient())
    adapter._ready.set()
    return adapter


def _fake(broker: MqttDeviceBroker) -> FakeMqttClient:
    """The stand-in transport, typed."""
    return cast(FakeMqttClient, broker._client)


def _answer(broker: MqttDeviceBroker, *, index: int = 0, **fields) -> None:
    """Deliver a dynsec response correlated to the Nth command published."""
    command = _fake(broker).commands[index]
    body = {
        "responses": [
            {"command": command["command"], "correlationData": command["correlationData"], **fields}
        ]
    }
    broker._dispatch(json.dumps(body).encode())


async def _run(coro, broker: MqttDeviceBroker, *, answers: list[dict] | None = None):
    """Drive a command and feed it its answers once it has published."""
    task = asyncio.create_task(coro)
    for i, fields in enumerate(answers or [{}]):
        # Yield until the publish has happened, then correlate to it.
        for _ in range(50):
            if len(_fake(broker).published) > i:
                break
            await asyncio.sleep(0)
        _answer(broker, index=i, **fields)
    return await task


class TestCorrelation:
    async def test_a_response_resolves_only_its_own_command(self, broker):
        """The response topic is BROADCAST.

        Phase 0 watched a second admin connection receive the first connection's
        responses. Without correlationData matching, two API replicas resolve
        each other's futures - which is why the matching is mandatory rather
        than stylistic, and why "one replica" is commented rather than enforced.
        """
        await _run(broker.set_device_password("dev-a", "pw"), broker)
        assert _fake(broker).commands[0]["command"] == "setClientPassword"
        assert _fake(broker).commands[0]["correlationData"]

    async def test_an_uncorrelated_response_is_counted_not_dropped(self, broker):
        """An orphan IS the finding, not an anomaly: either correlationData was
        not echoed, or this is another connection's response."""
        broker._dispatch(json.dumps({"command": "createClient", "error": "x"}).encode())
        assert broker.orphan_count == 1
        assert len(broker.orphans) == 1

    async def test_an_unparseable_payload_does_not_crash_the_reader(self, broker):
        """A reader that dies on one bad frame takes the whole control
        connection with it, and every later enrolment 503s."""
        broker._dispatch(b"{not json")
        assert len(broker.unparseable) == 1
        # And the adapter still works.
        await _run(broker.disable_device("dev-a"), broker)

    async def test_the_debug_rings_are_bounded(self, broker):
        """This process lives for weeks. An unbounded list is a slow leak."""
        for i in range(500):
            broker._dispatch(json.dumps({"command": "x", "error": str(i)}).encode())
        assert len(broker.orphans) <= 64
        assert broker.orphan_count == 500


class TestTheHardTimeout:
    async def test_a_command_with_no_answer_raises_rather_than_hanging(self, broker, monkeypatch):
        """MANDATORY, not defensive coding.

        Phase 0: a malformed payload DOES get a dynsec response, but with NO
        correlationData at all - so the caller's future can never resolve. An
        unbounded await would deadlock inside a request KrakenD cuts at 3000 ms,
        and the caller would see a gateway 500 with no explanation anywhere.
        """
        from core.config import settings

        monkeypatch.setattr(settings, "MQTT_COMMAND_TIMEOUT_MS", 20)
        with pytest.raises(BrokerTimeoutError):
            await broker.disable_device("dev-a")

    async def test_the_timeout_message_says_it_may_have_taken_effect(self, broker, monkeypatch):
        """A TIMEOUT IS NOT A FAILURE.

        dynsec has no transaction and no rollback, so a command that timed out
        may well have succeeded server-side. That is exactly why the enrolment
        retry path branches on already-exists instead of assuming nothing
        happened, and the message has to say so or the next person assumes the
        opposite.
        """
        from core.config import settings

        monkeypatch.setattr(settings, "MQTT_COMMAND_TIMEOUT_MS", 20)
        with pytest.raises(BrokerTimeoutError, match="may still have taken effect"):
            await broker.disable_device("dev-a")

    async def test_a_timed_out_command_leaves_no_pending_entry(self, broker, monkeypatch):
        """Otherwise the dict grows by one per timeout, for ever."""
        from core.config import settings

        monkeypatch.setattr(settings, "MQTT_COMMAND_TIMEOUT_MS", 20)
        with pytest.raises(BrokerTimeoutError):
            await broker.disable_device("dev-a")
        assert broker._pending == {}


class TestErrorsArriveInThePayload:
    async def test_a_refused_command_raises_even_though_the_publish_succeeded(self, broker):
        """Phase 0's load-bearing finding.

        A rejected command is PUBACKed normally and the refusal is in the BODY.
        An adapter that checks the publish result reports success for every
        rejected command.
        """
        with pytest.raises(BrokerCommandFailedError, match="Unknown command"):
            await _run(
                broker.disable_device("dev-a"), broker, answers=[{"error": "Unknown command"}]
            )

    async def test_already_exists_has_its_own_type(self, broker):
        """The ONE dynsec error with a recovery path rather than a report.

        `createClient` is not idempotent, and a retry inside the enrolment claim
        lease must fall through to `setClientPassword` - without that branch it
        burns exactly the credential the lease exists to protect.
        """
        credentials = DeviceCredentials(username="dev-a", password="pw", client_id="dev-a")
        with pytest.raises(BrokerClientExistsError):
            await _run(
                broker.create_device(credentials, "device"),
                broker,
                answers=[{"error": "Client already exists"}],
            )

    async def test_the_match_survives_a_reworded_message(self, broker):
        """Matched on a substring, case-insensitively, and deliberately.

        It is a human-readable string from a C program, not a code. Pinning the
        whole sentence would make a harmless upstream rewording turn a
        recoverable retry into a burned credential.
        """
        credentials = DeviceCredentials(username="dev-a", password="pw", client_id="dev-a")
        with pytest.raises(BrokerClientExistsError):
            await _run(
                broker.create_device(credentials, "device"),
                broker,
                answers=[{"error": "A client with that username ALREADY EXISTS."}],
            )


class TestOneCommandPerPublish:
    async def test_create_device_sends_two_separate_publishes(self, broker):
        """NEVER a batch.

        Phase 0: `{"commands":[ok, fails, ok]}` executed the third command after
        the second failed, with no rollback - leaving a half-configured client
        and no way to know which half.
        """
        credentials = DeviceCredentials(username="dev-a", password="pw", client_id="dev-a")
        await _run(broker.create_device(credentials, "device"), broker, answers=[{}, {}])

        assert len(_fake(broker).published) == 2
        for _, payload in _fake(broker).published:
            assert len(json.loads(payload)["commands"]) == 1

    async def test_the_client_id_is_pinned_in_the_create(self, broker):
        """Phase 0 Q1d. A client created with `clientid` set cannot connect with
        any other id, which enforces protocol 4.3 at the broker."""
        credentials = DeviceCredentials(username="dev-a", password="pw", client_id="dev-a")
        await _run(broker.create_device(credentials, "device"), broker, answers=[{}, {}])

        create = _fake(broker).commands[0]
        assert create["command"] == "createClient"
        assert create["clientid"] == "dev-a"
        assert _fake(broker).commands[1]["command"] == "addClientRole"

    async def test_commands_go_to_the_control_topic(self, broker):
        await _run(broker.delete_device("dev-a"), broker)
        assert _fake(broker).published[0][0] == DYNSEC_REQUEST_TOPIC


class TestRetainedStatusClear:
    async def test_it_publishes_an_empty_retained_message(self, broker):
        """A retained `status` that outlives its device replays on every worker
        reconnect, so the offline alert fires on every deploy until the team
        stops reading it.

        Zero-length + retain=True is how MQTT deletes a retained message; there
        is nothing to await, because a denied publish is still PUBACKed.
        """
        await broker.clear_retained_status("ce/1/dev-a/status")
        topic, payload, retain = _fake(broker).retained[0]
        assert topic == "ce/1/dev-a/status"
        assert payload == b""
        assert retain is True


class TestUnavailable:
    async def test_a_command_with_no_connection_is_unavailable_not_a_timeout(self):
        """503, and distinct from a timeout on purpose: here the command was
        never attempted, so there is nothing that might have taken effect."""
        adapter = MqttDeviceBroker()
        with pytest.raises(BrokerUnavailableError):
            await adapter.disable_device("dev-a")

    async def test_is_ready_reports_the_connection_state(self, broker):
        """Surfaced under /health/readiness's detail and never gated on:
        compose's healthcheck reads readiness, and a broker restart must not
        roll the API - and the whole admin surface with it - out of service."""
        assert await broker.is_ready() is True
        assert await MqttDeviceBroker().is_ready() is False
