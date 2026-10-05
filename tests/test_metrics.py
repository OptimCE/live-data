"""What the worker and the scheduler tell an operator, and what they cannot.

Both containers emitted NOTHING before this: `core/metrics.py` carried a single
instrument, `health_checks`, used only by `api/health/routes.py`. The scheduler
was worse than uninstrumented - `worker/scheduler_main.py` never called
`setup_tracer_provider()` at all, so in staging and production it installed no
meter provider and no OTLP log handler and exported nothing under any name.

These tests are about the PROPERTIES the design turns on, not about the existence
of counters:

  * `ingest.messages.total`'s `outcome` is a real partition - exactly one point
    per message, whatever happened inside it;
  * every attribute value space is bounded, so a device cannot mint time series;
  * a failing database is counted, and counting it does not itself look like a
    failing database;
  * a job that skipped because another replica held its lock is distinguishable
    from a job that had nothing to do;
  * one frozen community among forty moves the lag gauge.

Values are read as DELTAS (`metric_delta`): the meter provider is session-scoped
because `set_meter_provider` is once-only, so every earlier test's counts are
still in the sum.
"""

import ast
import asyncio
import datetime
import importlib
import inspect
import json
import time
from typing import ClassVar

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core import metrics as app_metrics
from core.config import settings
from domain.reasons import SCOPES, RejectReason
from domain.topics import parse_device_topic
from shared.const import (
    ADVISORY_LOCK_OWNERSHIP,
    ADVISORY_LOCK_PARTITIONS,
    ADVISORY_LOCK_RETENTION,
    ADVISORY_LOCK_ROLLUPS,
    TOPIC_STATUS_WILDCARD,
    TOPIC_TELEMETRY_WILDCARD,
)
from tests.conftest import sessionmaker_for as _sessionmaker_for
from tests.conftest import static_subscriptions
from tests.factories.device_factory import create_device
from tests.factories.meter_factory import create_owned_meter
from worker import scheduler
from worker.context import advisory_lock, local_sessionmaker
from worker.ingest import handle_message
from worker.subscriptions import (
    SubscriptionCache,
    SubscriptionsUnavailable,
    crm_subscription_loader,
)

NOW = datetime.datetime(2026, 7, 1, 12, 0, tzinfo=datetime.UTC)


def _reading(ts: str, production_wh: float = 100.0) -> dict:
    """One protocol-3.1 measurement, with every required field.

    `import_wh` and `export_wh` are required; `production_wh` is
    required-but-NULLABLE, because a P1 port sees only the grid exchange and a
    connector that guesses there understates community production for ever.
    """
    return {
        "ts": ts,
        "interval_s": 900,
        "import_wh": 0.0,
        "export_wh": production_wh,
        "production_wh": production_wh,
    }


def _histogram_point(reader, name: str, attr: tuple):
    """The raw histogram point, so bucket OCCUPANCY can be asserted.

    `read_metrics` flattens a histogram to its count, which is right for "did
    this happen" and useless for "can these boundaries tell two values apart" -
    the property that actually matters, and the one a comparison against the
    module's own constant cannot check."""
    for rm in reader.get_metrics_data().resource_metrics:
        for sm in rm.scope_metrics:
            for metric in sm.metrics:
                if metric.name != name:
                    continue
                for point in metric.data.data_points:
                    if attr in tuple(point.attributes.items()):
                        return point
    raise AssertionError(f"no {name} point with {attr}")


def _points(delta: dict, name: str) -> dict[tuple, float]:
    return {attrs: value for (metric, attrs), value in delta.items() if metric == name}


def _total(delta: dict, name: str) -> float:
    return sum(_points(delta, name).values())


class TestTheOutcomeLabelIsAPartition:
    """Exactly one `ingest.messages.total` point per message, always.

    A telemetry batch routinely comes back with `stored > 0` AND a non-empty
    `rejected` list - that is the whole point of measurement scope: one drifted
    timestamp loses the timestamp, not the fortnight. So "stored or rejected" is
    not a question with one answer, and a label that tried to be both would
    double-count some messages and not others.
    """

    async def test_a_stored_batch_is_one_point(
        self, db_session: AsyncSession, community, metric_delta
    ):
        ean = await create_owned_meter(db_session, id_community=community.id, id_member=1)
        device = await create_device(db_session, id_community=community.id, ean=ean)
        public_id = await db_session.scalar(
            text("SELECT public_id FROM device WHERE id = :d"), {"d": device}
        )
        topic = f"ce/{community.id}/{public_id}/telemetry"
        payload = json.dumps(
            {
                "v": 1,
                "measurements": [_reading("2026-07-01T11:45:00Z")],
            }
        ).encode()

        outcome = await handle_message(
            db_session, topic, payload, NOW, None, active_communities=frozenset({community.id})
        )
        assert outcome.stored == 1

        from worker.main import _record_outcome, _topic_kind

        _record_outcome(_topic_kind(topic), outcome, 0.01)
        points = _points(metric_delta(), "ingest.messages.total")
        assert len(points) == 1, f"one message must be one point, got {points}"
        assert sum(points.values()) == 1
        attrs = next(iter(points))
        assert ("kind", "telemetry") in attrs
        assert ("outcome", "stored") in attrs

    def test_a_partly_rejected_batch_is_still_one_point(self, metric_delta):
        """And it counts as `stored`, because something WAS stored.

        The individual dropped readings are on `ingest.rejections.total`. Summing
        the two instruments is meaningless by construction, which is better than
        a total that is quietly wrong.
        """
        from worker.ingest import IngestOutcome
        from worker.main import _record_outcome

        outcome = IngestOutcome(
            stored=199,
            rejected=[(RejectReason.TS_NOT_ALIGNED, "one bad ts")],
            observations=[],
        )
        _record_outcome("telemetry", outcome, 0.01)
        delta = metric_delta()

        points = _points(delta, "ingest.messages.total")
        assert len(points) == 1
        # THE PRECEDENCE, which is the whole thesis of this class. Swap the
        # `if`/`elif` in `_record_outcome` and every other assertion here still
        # passes, while in production the partition inverts to ~100% `rejected`
        # for messages that are each writing 199 readings.
        assert next(iter(points)) == (("kind", "telemetry"), ("outcome", "stored"))
        assert _total(delta, "ingest.measurements.stored.total") == 199
        assert _total(delta, "ingest.rejections.total") == 1

    def test_an_empty_commit_is_not_silence(self, metric_delta):
        """The zero-length retained publish revocation uses to clear a status.

        It stores nothing and rejects nothing. Left unlabelled it would be
        indistinguishable from a message that never arrived - and a revoke that
        stops clearing retained statuses is exactly the failure that makes the
        offline alert fire on every deploy until the team stops reading it.
        """
        from worker.ingest import IngestOutcome
        from worker.main import _record_outcome

        _record_outcome("status", IngestOutcome(), 0.01)
        points = _points(metric_delta(), "ingest.messages.total")
        assert list(points) == [(("kind", "status"), ("outcome", "empty"))]

    def test_every_disposition_is_reachable(self, metric_delta):
        """The guard against a label that can never take one of its values.

        A disposition that cannot occur makes a flat line look like health, and
        the four here are asserted together so that adding a fifth without a
        path to it fails rather than sits.
        """
        from worker.ingest import IngestOutcome
        from worker.main import _record_outcome

        _record_outcome("telemetry", IngestOutcome(stored=1), 0.01)
        _record_outcome(
            "telemetry", IngestOutcome(rejected=[(RejectReason.SCHEMA_INVALID, "")]), 0.01
        )
        _record_outcome("status", IngestOutcome(), 0.01)
        # Both at once: ordinary telemetry, and the case that decides the
        # precedence above.
        _record_outcome(
            "telemetry",
            IngestOutcome(stored=1, rejected=[(RejectReason.TS_NOT_ALIGNED, "")]),
            0.01,
        )
        # Telemetry of a switched-off community (D-12). Reached from the loop in
        # TestTheLoopItself too, against a real device.
        _record_outcome("telemetry", IngestOutcome(not_subscribed=True), 0.01)

        seen = {
            dict(attrs)["outcome"] for attrs in _points(metric_delta(), "ingest.messages.total")
        }
        assert seen == {"stored", "rejected", "empty", "not_subscribed"}, seen
        # `db_error` and `dropped` come from the LOOP, not from this function,
        # and are asserted where they are produced - see TestTheLoopItself.
        # Emitting them here as literals is what made this test vacuous for
        # exactly the two dispositions nothing else reached.

    def test_a_discarded_message_is_not_counted_as_empty(self, metric_delta):
        """`not_subscribed` is checked FIRST in `_record_outcome`.

        A discard stores nothing and rejects nothing, so without its own branch
        it falls through to `empty` - the label of revocation's retained clear -
        and a switched-off community's fleet reads as a burst of healthy empty
        commits. And it is not a rejection: nothing reaches the reason counter.
        """
        from worker.ingest import IngestOutcome
        from worker.main import _record_outcome

        _record_outcome("telemetry", IngestOutcome(not_subscribed=True), 0.01)
        delta = metric_delta()
        assert _points(delta, "ingest.messages.total") == {
            (("kind", "telemetry"), ("outcome", "not_subscribed")): 1
        }
        names = {name for name, _ in delta}
        assert "ingest.rejections.total" not in names
        assert "ingest.measurements.stored.total" not in names
        assert "ingest.measurement.lateness.seconds" not in names


class TestAHostileTopicCannotStopIngestion:
    """`_topic_kind` runs OUTSIDE the per-message `try`, so it must be total.

    ---------------------------------------------------------------------------
    THE REGRESSION THIS CLASS EXISTS FOR.

    Labelling messages by `kind` meant calling `parse_device_topic` from the
    consume loop rather than only from inside `handle_message`. That moved a
    topic-derived computation UPSTREAM of the guard whose entire property, stated
    in `worker/ingest.py`, is that "a poison message cannot take the connection
    down".

    And the parser was not total. It gated on `str.isdigit()` and then called
    `int()`, which disagree on 128 code points - `"²".isdigit()` is True and
    `int("²")` raises. The community level is the `+` in the broker ACL
    `ce/+/%u/telemetry`, so a device CHOOSES it. One publish of
    `ce/²/<uuid>/status` - retained, which the same ACL permits - would have
    unwound out of `_consume` on every reconnect: no counter, no dead letter, no
    `consecutive_db_failures`, just `ingest:crash` and a backoff pinned at 30 s,
    for the whole fleet, for ever.
    ---------------------------------------------------------------------------
    """

    # Every one of these is `isdigit()`-true. The first three additionally made
    # `int()` raise; the rest are decimal but ALIAS an existing community.
    HOSTILE: ClassVar[list[str]] = [
        "\u00b2",  # SUPERSCRIPT TWO
        "\u2460",  # CIRCLED DIGIT ONE
        "\u1369",  # ETHIOPIC DIGIT ONE
        "\u0661",  # ARABIC-INDIC ONE - decimal, and int()s to 1
        "\U0001d7ce",  # MATHEMATICAL BOLD DIGIT ZERO
        "007",  # decimal, and int()s to 7
    ]

    @pytest.mark.parametrize("level", HOSTILE)
    def test_the_parser_returns_none_rather_than_raising(self, level):
        topic = f"ce/{level}/2b0b8a3c-0000-0000-0000-000000000000/telemetry"
        assert parse_device_topic(topic) is None

    @pytest.mark.parametrize("level", HOSTILE)
    def test_the_label_helper_is_total(self, level):
        from worker.main import _topic_kind

        topic = f"ce/{level}/2b0b8a3c-0000-0000-0000-000000000000/status"
        assert _topic_kind(topic) == "unparsed"

    def test_exactly_one_topic_string_maps_to_a_community(self):
        """The mismatch check downstream compares an int to an int, so two
        spellings reaching the same id would be two identities for one
        community. `parse_device_topic`'s own comment forbade this before the
        code did."""
        uid = "2b0b8a3c-0000-0000-0000-000000000000"
        canonical = parse_device_topic(f"ce/7/{uid}/telemetry")
        assert canonical is not None and canonical.claimed_community_id == 7
        for alias in ("007", "\u0667", " 7", "+7", "7_0", "0x7"):
            assert parse_device_topic(f"ce/{alias}/{uid}/telemetry") is None, alias

    def test_the_guard_can_fail(self):
        """The negative control. `isdigit()` is what the parser used, and this
        pins the disagreement that made it dangerous - if a future Python made
        them agree, the comment in `domain/topics.py` would be stale."""
        assert "\u00b2".isdigit()
        assert not "\u00b2".isdecimal()
        with pytest.raises(ValueError, match="invalid literal"):
            int("\u00b2")


class TestTheLoopItself:
    """Drives `_consume`. Nothing else in the suite does, and that is the gap
    that let the topic-parse crash through.

    Every other test here calls `_record_outcome`, a pure function over an
    `IngestOutcome`. That proves what a disposition maps to and NOTHING about
    whether the loop reaches it - so `db_error` and `dropped` were "shown
    reachable" by emitting them as literals inside the test that claims to check
    reachability.
    """

    @staticmethod
    def _client(messages):
        """The smallest thing `_consume` will accept in place of aiomqtt."""

        class FakeMessage:
            def __init__(self, topic, payload):
                self.topic = topic
                self.payload = payload

        class FakeClient:
            def __init__(self):
                self.subscribed: list[tuple[str, int]] = []

            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return False

            async def subscribe(self, topic, qos=0):
                self.subscribed.append((topic, qos))

            @property
            async def messages(self):  # pragma: no cover - replaced below
                raise AssertionError

        client = FakeClient()

        async def _iter():
            for topic, payload in messages:
                yield FakeMessage(topic, payload)

        type(client).messages = property(lambda self: _iter())
        return client

    async def _run_loop(self, monkeypatch, messages, subscriptions=None):
        import worker.main as worker_main

        client = self._client(messages)
        monkeypatch.setattr(worker_main, "_client", lambda: client)
        shutdown = asyncio.Event()
        await worker_main._consume(shutdown, subscriptions or static_subscriptions())
        return client

    async def test_one_point_per_message_whatever_happens(
        self, monkeypatch, db_session, community, metric_delta
    ):
        """The class docstring of `TestTheOutcomeLabelIsAPartition` states this in
        prose and asserts it nowhere: it is a property of the LOOP, not of
        `_record_outcome`."""
        import worker.main as worker_main

        uid = "2b0b8a3c-0000-0000-0000-000000000000"
        monkeypatch.setattr(worker_main, "AsyncSessionLocalFactory", _sessionmaker_for(db_session))
        await self._run_loop(
            monkeypatch,
            [
                # a device that does not exist -> a rejection, committed
                (f"ce/{community.id}/{uid}/telemetry", b'{"v":1,"measurements":[]}'),
                # an unparseable topic
                ("ce/not-a-number/x/telemetry", b"{}"),
                # the hostile one: `isdigit()` true, `int()` used to raise here
                (f"ce/²/{uid}/status", b""),
            ],
            static_subscriptions(community.id),
        )
        delta = metric_delta()
        points = _points(delta, "ingest.messages.total")
        assert sum(points.values()) == 3, f"three messages, three points: {points}"
        # And one duration observation per message, on the same paths.
        assert _total(delta, "ingest.message.duration.seconds") == 3

    async def test_a_failing_database_is_counted_and_disconnects(self, monkeypatch, metric_delta):
        """THE PATH NOTHING REACHED. Every message here is lost for good - paho
        PUBACKed it before the handler saw it and the dead-letter INSERT was in
        the transaction that rolled back - and the container stays healthy
        throughout, because the heartbeat only misses a tick."""
        import worker.main as worker_main
        from core.config import settings as app_settings

        monkeypatch.setattr(app_settings, "INGEST_DB_FAILURES_BEFORE_DISCONNECT", 2)

        def exploding_factory():
            raise RuntimeError("the database is gone")

        monkeypatch.setattr(worker_main, "AsyncSessionLocalFactory", exploding_factory)
        uid = "2b0b8a3c-0000-0000-0000-000000000000"
        await self._run_loop(
            monkeypatch, [(f"ce/1/{uid}/telemetry", b'{"v":1,"measurements":[]}')] * 5
        )

        delta = metric_delta()
        points = _points(delta, "ingest.messages.total")
        assert points == {(("kind", "telemetry"), ("outcome", "db_error")): 2}, points
        assert _total(delta, "ingest.backpressure.disconnects.total") == 1
        # It disconnected at the second failure, so the remaining three messages
        # were never taken delivery of - that is the whole point of the design.
        assert "ingest.measurements.stored.total" not in {name for name, _ in delta}

    async def test_a_message_in_hand_at_shutdown_is_counted_as_lost(
        self, monkeypatch, metric_delta
    ):
        """One per SIGTERM, on every deploy. It is in no database, no dead-letter
        table and no broker session - and until this counter existed, on no
        instrument either."""
        import worker.main as worker_main

        client = self._client([("ce/1/2b0b8a3c-0000-0000-0000-000000000000/telemetry", b"{}")])
        monkeypatch.setattr(worker_main, "_client", lambda: client)
        shutdown = asyncio.Event()
        shutdown.set()
        await worker_main._consume(shutdown, static_subscriptions())

        points = _points(metric_delta(), "ingest.messages.total")
        assert points == {(("kind", "telemetry"), ("outcome", "dropped")): 1}, points

    async def test_both_topics_are_subscribed_in_one_call(self, monkeypatch):
        """Not a metric, but this loop is now driven and the property is free:
        subscribing to telemetry at QoS 0 would DOWNGRADE delivery whatever the
        publisher used."""
        client = await self._run_loop(monkeypatch, [])
        assert client.subscribed == [
            (TOPIC_TELEMETRY_WILDCARD, 1),
            (TOPIC_STATUS_WILDCARD, 0),
        ]

    # ---- the live-data subscription (D-12) ---------------------------------

    @staticmethod
    async def _device_topic(db_session, id_community: int) -> str:
        ean = await create_owned_meter(db_session, id_community=id_community, id_member=4)
        device = await create_device(db_session, id_community=id_community, ean=ean)
        public_id = await db_session.scalar(
            text("SELECT public_id FROM device WHERE id = :d"), {"d": device}
        )
        return f"ce/{id_community}/{public_id}/telemetry"

    @staticmethod
    def _telemetry() -> bytes:
        """One reading stamped on the CURRENT interval, because `_consume`
        validates against the wall clock rather than the suite's fixed NOW - and
        with no production, so the night window cannot reject it."""
        now = datetime.datetime.now(datetime.UTC)
        ts = now.replace(minute=now.minute - now.minute % 15, second=0, microsecond=0)
        reading = {
            "ts": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "interval_s": 900,
            "import_wh": 50.0,
            "export_wh": 0.0,
            "production_wh": 0.0,
        }
        return json.dumps({"v": 1, "measurements": [reading]}).encode()

    async def test_a_deactivated_community_is_counted_not_stored(
        self, monkeypatch, db_session, deactivated_community, metric_delta
    ):
        """End to end through the loop, with the set read from the REAL CRM
        table: an `is_active = false` row, which is what an unsubscribe leaves.

        One `not_subscribed` point and nothing else - not a rejection, not a
        stored reading, and not the `empty` a missing branch would fall to.
        """
        import worker.main as worker_main

        monkeypatch.setattr(worker_main, "AsyncSessionLocalFactory", _sessionmaker_for(db_session))
        topic = await self._device_topic(db_session, deactivated_community.id)
        cache = SubscriptionCache(
            crm_subscription_loader(_sessionmaker_for(db_session)), ttl_seconds=60
        )
        await self._run_loop(monkeypatch, [(topic, self._telemetry())], cache)

        delta = metric_delta()
        assert _points(delta, "ingest.messages.total") == {
            (("kind", "telemetry"), ("outcome", "not_subscribed")): 1
        }
        names = {name for name, _ in delta}
        assert "ingest.rejections.total" not in names
        assert "ingest.measurements.stored.total" not in names
        assert _points(delta, "subscription.refreshes.total") == {(("outcome", "ok"),): 1}
        stored = await db_session.scalar(
            text("SELECT count(*) FROM measurement WHERE id_community = :c"),
            {"c": deactivated_community.id},
        )
        assert stored == 0

    async def test_the_subscription_set_is_loaded_before_the_broker_is_dialled(
        self, monkeypatch
    ) -> None:
        """Cold start: the worker cannot judge a single message without the set,
        so it must not take delivery of one before it has it. The broker's
        persistent session is the buffer meanwhile."""
        import worker.main as worker_main

        order: list[str] = []
        client = self._client([])

        async def load() -> frozenset[int]:
            order.append("load")
            return frozenset()

        def dial():
            order.append("dial")
            return client

        monkeypatch.setattr(worker_main, "_client", dial)
        await worker_main._consume(asyncio.Event(), SubscriptionCache(load, ttl_seconds=60))
        assert order == ["load", "dial"]

    async def test_a_cold_crm_keeps_the_worker_off_the_broker(self, monkeypatch, metric_delta):
        """No set has ever loaded and the CRM does not answer: raise, and never
        dial. `_connected` stays unset, so the heartbeat stops and the container
        goes unhealthy - a worker that cannot decide what to ingest ingests
        nothing."""
        import worker.main as worker_main

        async def load() -> frozenset[int]:
            raise ConnectionRefusedError("crm down")

        def dial():  # pragma: no cover - the assertion is that this never runs
            raise AssertionError("the worker dialled the broker without a subscription set")

        monkeypatch.setattr(worker_main, "_client", dial)
        worker_main._connected.clear()
        with pytest.raises(SubscriptionsUnavailable):
            await worker_main._consume(asyncio.Event(), SubscriptionCache(load, ttl_seconds=60))
        assert not worker_main._connected.is_set()
        assert _points(metric_delta(), "subscription.refreshes.total") == {
            (("outcome", "failed"),): 1
        }

    async def test_run_backs_off_on_a_cold_crm(self, monkeypatch) -> None:
        """`_run` names the cold CRM rather than reporting an `ingest:crash`, and
        backs off like any other failure to connect.

        The logger is spied on rather than read through `caplog`: importing
        `worker.main` runs `configure_logging()`, which clears the root handlers
        - caplog's included - when this happens to be the first import."""
        import worker.main as worker_main

        shutdown = asyncio.Event()

        async def load() -> frozenset[int]:
            shutdown.set()  # one attempt, then let `_run` return
            raise ConnectionRefusedError("crm down")

        def dial():  # pragma: no cover - the assertion is that this never runs
            raise AssertionError("the worker dialled the broker without a subscription set")

        logged: list[tuple[str, object]] = []

        def spy(level):
            def record(*_args, **kwargs):
                logged.append((level, kwargs.get("extra", {}).get("operation")))

            return record

        monkeypatch.setattr(worker_main, "_client", dial)
        monkeypatch.setattr(worker_main.logger, "warning", spy("warning"))
        monkeypatch.setattr(worker_main.logger, "exception", spy("exception"))
        await worker_main._run(shutdown, SubscriptionCache(load, ttl_seconds=60))

        assert logged == [("warning", "ingest:subscriptions-unavailable")], logged

    async def test_a_failing_refresh_mid_session_is_not_a_db_error(
        self, monkeypatch, db_session, community, metric_delta
    ):
        """Critique R1. The set is read OUTSIDE the per-message `try`, and a warm
        cache never raises: a CRM that fails mid-session costs freshness, not
        messages. Inside the `try`, the same failure would be counted `db_error`
        against a healthy database and feed the backpressure disconnect."""
        import worker.main as worker_main

        monkeypatch.setattr(worker_main, "AsyncSessionLocalFactory", _sessionmaker_for(db_session))
        topic = await self._device_topic(db_session, community.id)
        clock = [0.0]
        loads = [0]

        async def load() -> frozenset[int]:
            loads[0] += 1
            if loads[0] > 1:
                raise ConnectionRefusedError("crm down")
            return frozenset({community.id})

        cache = SubscriptionCache(load, ttl_seconds=60, clock=lambda: clock[0])
        await cache.get()  # warm
        clock[0] = 120.0  # the next get() refreshes, and the refresh fails
        await self._run_loop(monkeypatch, [(topic, self._telemetry())], cache)

        delta = metric_delta()
        assert loads[0] == 2, "the refresh was attempted"
        assert _points(delta, "ingest.messages.total") == {
            (("kind", "telemetry"), ("outcome", "stored")): 1
        }, "the last set that loaded still applied"
        assert _points(delta, "subscription.refreshes.total") == {
            (("outcome", "ok"),): 1,
            (("outcome", "failed"),): 1,
        }

    def test_the_set_is_read_outside_the_per_message_try(self):
        """The structural half of the test above: where the call sits."""
        from worker.main import _consume

        source = inspect.getsource(_consume)
        per_message = source.index("async for message in client.messages")
        read = source.index("active = await subscriptions.get()", per_message)
        guarded = source.index("\n            try:\n", per_message)
        assert read < guarded, "the per-message set read must precede the `try`"


class TestCardinalityIsBounded:
    """A label whose value space is unbounded is a memory leak in the SDK and a
    cost incident in the collector. The dangerous one here is attacker-chosen:
    the broker ACL is `ce/+/%u/telemetry`, so a device picks the community id in
    its own topic, and labelling by it would let a basement mint series."""

    def test_the_reject_reason_space_is_the_frozen_protocol(self):
        """13 values, and a new one is a protocol version bump rather than a
        deploy - so this label cannot grow without `v: 2`."""
        assert len(list(RejectReason)) == 13
        assert set(SCOPES) == set(RejectReason), "SCOPES must stay total over the enum"

    def test_scope_is_not_a_label_because_it_carries_nothing(self):
        """`SCOPES` is a total map reason -> scope, so scope is functionally
        determined. Adding it would double the series count and add no
        information - the classic way a metric bill doubles for nothing."""
        from worker.main import _record_outcome

        source = inspect.getsource(_record_outcome)
        assert '"scope"' not in source, (
            "a scope attribute on the rejection counter is redundant by construction - "
            "SCOPES is a total map, so it would double the series count for nothing"
        )
        assert '"reason"' in source

    # Attribute keys whose value space is not bounded by an enum or a literal.
    # The dangerous one is attacker-chosen: the broker ACL is `ce/+/%u/telemetry`,
    # so a device picks the community segment of its own topic.
    UNBOUNDED: ClassVar[frozenset[str]] = frozenset(
        {"device", "device_id", "id_device", "ean", "topic", "id_community", "community"}
    )

    @staticmethod
    def _metric_attribute_keys(module) -> list[tuple[int, str]]:
        """Every attribute key passed to an `app_metrics.*.add/record` call.

        AST, not text. The previous version read the source LINE BY LINE and
        skipped any line without both `app_metrics.` and `.add(`/`.record(` -
        so an attribute dict on a continuation line was invisible, and
        `worker/main.py` already had one. Adding `"topic": topic` to it would
        have passed.
        """
        found: list[tuple[int, str]] = []
        for node in ast.walk(ast.parse(inspect.getsource(module))):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                continue
            if node.func.attr not in ("add", "record"):
                continue
            root = node.func.value
            while isinstance(root, ast.Attribute):
                root = root.value
            if not (isinstance(root, ast.Name) and root.id == "app_metrics"):
                continue
            for arg in [*node.args, *(kw.value for kw in node.keywords)]:
                if not isinstance(arg, ast.Dict):
                    continue
                for key in arg.keys:
                    if isinstance(key, ast.Constant) and isinstance(key.value, str):
                        found.append((node.lineno, key.value))
        return found

    @pytest.mark.parametrize(
        "module_name",
        ["worker.main", "worker.scheduler", "worker.subscriptions", "api.health.routes"],
    )
    def test_no_instrument_is_labelled_by_an_unbounded_identifier(self, module_name):
        module = importlib.import_module(module_name)
        keys = self._metric_attribute_keys(module)
        offenders = [(line, key) for line, key in keys if key in self.UNBOUNDED]
        assert not offenders, (
            f"{module_name} labels a metric by an unbounded identifier: {offenders}. "
            f"One device in a basement then mints a time series per value, retained "
            f"for the process's lifetime and billed by the collector."
        )

    def test_the_scan_can_see_a_continuation_line(self):
        """The negative control, and the exact shape that defeated the old scan:
        the call on one line, the attribute dict on the next."""
        import types

        module = types.ModuleType("probe")
        module.__dict__["__source__"] = None
        source = (
            "def emit():\n"
            "    app_metrics.ingest_message_duration.record(\n"
            '        0.1, {"kind": kind, "topic": topic}\n'
            "    )\n"
        )
        found = []
        for node in ast.walk(ast.parse(source)):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                for arg in node.args:
                    if isinstance(arg, ast.Dict):
                        found += [k.value for k in arg.keys if isinstance(k, ast.Constant)]
        assert "topic" in found, "the walker must see a dict on a continuation line"
        assert "topic" in self.UNBOUNDED


class TestTheDatabaseFailurePath:
    """The worker's least visible incident: the container stays healthy through
    it, and every message it drops was PUBACKed before application code saw it,
    so it exists in neither the database nor the broker."""

    def test_counting_a_failure_cannot_itself_look_like_a_failure(self):
        """THE STRUCTURAL ONE, and the reason `_record_outcome` is not called
        inside the `try`.

        `_log_outcome` used to be the last statement of the try block. A metric
        emitted beside it there would make any instrumentation error
        indistinguishable from a database error: caught by the same handler,
        logged as `ingest:error` with a stack trace against a healthy database,
        and the outcome it was meant to record lost.

        It would not reach the backpressure disconnect as the code stands, since
        `consecutive_db_failures = 0` precedes the emission and the count would
        oscillate 0 -> 1 - but that is statement order, not a safeguard. `else:`
        is what actually keeps the two apart, and it is invisible in a diff.
        """
        from worker.main import _consume

        source = inspect.getsource(_consume)
        assert (
            "\n            else:\n" in source
        ), "the success-path emission must be in an `else:` clause, not in the `try`"
        else_body = source.split("\n            else:\n", 1)[1]
        assert "_record_outcome" in else_body
        assert "_log_outcome" in else_body

    def test_the_deliberate_disconnect_has_its_own_counter(self, metric_delta):
        """Correct behaviour AND an incident at the same time. The heartbeat only
        misses a tick across it, so the container never goes unhealthy and
        nothing else separates a worker doing its job from a worker shedding a
        database outage five already-PUBACKed messages at a time."""
        app_metrics.ingest_backpressure_disconnects.add(1)
        assert _total(metric_delta(), "ingest.backpressure.disconnects.total") == 1


class TestLateness:
    """`last_seen_at` is the SERVER's receipt clock, so a device that reconnects
    after a week and delivers the week looks exactly like a live one. This is the
    only number that separates them."""

    async def test_a_backlog_records_the_age_of_its_oldest_reading(
        self, db_session: AsyncSession, community, metric_delta
    ):
        ean = await create_owned_meter(db_session, id_community=community.id, id_member=2)
        device = await create_device(db_session, id_community=community.id, ean=ean)
        public_id = await db_session.scalar(
            text("SELECT public_id FROM device WHERE id = :d"), {"d": device}
        )
        topic = f"ce/{community.id}/{public_id}/telemetry"
        payload = json.dumps(
            {
                "v": 1,
                "measurements": [
                    _reading("2026-06-28T12:00:00Z"),
                    _reading("2026-07-01T11:45:00Z"),
                ],
            }
        ).encode()

        outcome = await handle_message(
            db_session, topic, payload, NOW, None, active_communities=frozenset({community.id})
        )
        assert outcome.stored == 2
        # Three days, from the OLDEST reading - not the newest, and not an
        # average. "How far behind is this device" is a property of the message.
        assert outcome.oldest_lateness_s == pytest.approx(3 * 86400, abs=60)

        from worker.main import _record_outcome

        _record_outcome("telemetry", outcome, 0.01)
        assert _total(metric_delta(), "ingest.measurement.lateness.seconds") == 1

    def test_nothing_stored_records_no_lateness(self, metric_delta):
        """Rather than a zero, which would read as "bang up to date".

        Asserted through the EMITTER, not on the dataclass default. Rewriting
        the guard in `_record_outcome` as `record(outcome.oldest_lateness_s or
        0.0)` - a plausible tidy-up that removes a branch - would make every
        status message and every retained clear record 0 s, collapsing the p50
        to zero while a fleet sits a week behind.
        """
        from worker.ingest import IngestOutcome
        from worker.main import _record_outcome

        _record_outcome("status", IngestOutcome(), 0.01)
        names = {name for name, _ in metric_delta()}
        assert "ingest.measurement.lateness.seconds" not in names
        assert "ingest.measurements.stored.total" not in names
        # And the message itself WAS counted, so the absence above is the guard
        # working rather than the emitter never running.
        assert "ingest.messages.total" in names


class TestHistogramBuckets:
    """The SDK's default boundaries are (0, 5, 10, ... 10000) and were designed
    for MILLISECONDS. In seconds they put a 5-second floor under everything: a
    per-message duration lands in the first bucket for ever, every percentile is
    a constant, and the chart is a flat line that looks like a very fast system."""

    @pytest.mark.parametrize(
        "name",
        [
            "ingest.message.duration.seconds",
            "ingest.measurement.lateness.seconds",
            "scheduler.job.duration.seconds",
        ],
    )
    def test_each_histogram_declares_its_own_boundaries(self, name, metric_reader):
        instrument = {
            "ingest.message.duration.seconds": app_metrics.ingest_message_duration,
            "ingest.measurement.lateness.seconds": app_metrics.ingest_measurement_lateness,
            "scheduler.job.duration.seconds": app_metrics.scheduler_job_duration,
        }[name]
        instrument.record(0.02, {"probe": "boundaries"})

        data = metric_reader.get_metrics_data()
        bounds = [
            list(point.explicit_bounds)
            for rm in data.resource_metrics
            for sm in rm.scope_metrics
            for metric in sm.metrics
            if metric.name == name
            for point in metric.data.data_points
        ]
        assert bounds, f"{name} produced no histogram point"
        default_ms = [0.0, 5.0, 10.0, 25.0, 50.0, 75.0, 100.0, 250.0, 500.0, 750.0, 1000.0]
        for declared in bounds:
            assert (
                declared[: len(default_ms)] != default_ms
            ), f"{name} fell back to the SDK's millisecond defaults"

    def test_the_sub_second_buckets_can_resolve_a_fast_message(self, metric_reader):
        """A database write is tens of milliseconds, so a healthy worker must
        not land entirely in one bucket - which is what the SDK's millisecond
        defaults would do, and what comparing against the module's own constant
        cannot detect."""
        for value in (0.004, 0.02, 0.4):
            app_metrics.ingest_message_duration.record(value, {"kind": "resolution-probe"})
        point = _histogram_point(
            metric_reader, "ingest.message.duration.seconds", ("kind", "resolution-probe")
        )
        occupied = [i for i, count in enumerate(point.bucket_counts) if count]
        assert len(occupied) == 3, (
            f"three values an order of magnitude apart landed in {len(occupied)} "
            f"bucket(s): {list(point.explicit_bounds)}"
        )

    def test_the_lateness_buckets_span_drift_to_the_acceptance_window(self, metric_reader):
        """A device drifting by five minutes and one a month behind must not
        share a bucket, and nothing may fall past the last boundary: the
        acceptance window is INGEST_MAX_AGE_DAYS and store-and-forward makes a
        week-old backlog NORMAL."""
        from core.config import settings

        month_behind = settings.INGEST_MAX_AGE_DAYS * 86400 * 0.9
        for value in (5.0, 600.0, month_behind):
            app_metrics.ingest_measurement_lateness.record(value, {"kind": "span-probe"})
        point = _histogram_point(
            metric_reader, "ingest.measurement.lateness.seconds", ("kind", "span-probe")
        )
        occupied = [i for i, count in enumerate(point.bucket_counts) if count]
        assert len(occupied) == 3, (
            f"on-time, ten-minutes-drifted and a month behind shared a bucket: "
            f"{list(point.explicit_bounds)}"
        )
        assert point.bucket_counts[-1] == 0, "the month-behind value fell into +Inf"
        # The floor is what makes DRIFT visible. With 900 as the first boundary,
        # a device on time and a device fourteen minutes late are one bucket, and
        # fleet-wide drift under a quarter of an hour never moves a percentile.
        assert point.explicit_bounds[0] < 900.0


class TestTheSchedulerJobs:
    async def test_a_successful_job_is_counted_and_timed(self, metric_delta):
        """`_observed` directly, rather than through `run_partitions`.

        `run_partitions` COMMITS - on the session-scoped engine, outside the
        per-test rollback - so running it here created partitions that outlived
        the test for the rest of the suite. Nothing needs that: the assertion
        is about the wrapper, and the wrapper is what is driven."""
        async with scheduler._observed("partitions"):
            pass
        delta = metric_delta()

        runs = _points(delta, "scheduler.job.runs.total")
        assert (("job", "partitions"), ("outcome", "ok")) in runs
        # The duration carries the outcome too. A run that skipped on a held lock
        # returns in microseconds, and folding those into the same distribution
        # as the work drags every percentile toward zero in exactly the
        # deployment that has a second replica.
        durations = _points(delta, "scheduler.job.duration.seconds")
        assert (("job", "partitions"), ("outcome", "ok")) in durations, durations

    async def test_the_drain_counter_is_actually_fed(
        self, db_session: AsyncSession, test_engine, metric_delta
    ):
        """It was declared, documented in the runbook as an alert, and wired to
        NOTHING - the drain count was computed under the lock inside
        `create_partition_draining_default` and discarded by its `-> bool`.

        A counter with no emitter reads as a flat line, and this one's flat line
        is supposed to mean "rows have never landed in a default partition" -
        the single most consequential thing the scheduler can tell an operator.
        """
        # The TEST's session, for the reason `_observed` is driven directly
        # above: on the session-scoped engine this committed NOW's partitions for
        # the rest of the suite, and that hid every later test that assumed
        # schema.sql had provisioned around a frozen date.
        sessions = _sessionmaker_for(db_session)
        await scheduler.run_partitions(sessions, now=NOW, engine=test_engine)
        delta = metric_delta()
        # Zero is the healthy value and a zero-valued `.add()` produces no point,
        # so what is asserted here is that the emitter EXISTS and is reached -
        # the drain arithmetic itself is `tests/test_partition_maintenance.py`.
        source = inspect.getsource(scheduler.run_partitions)
        assert "partition_default_rows_drained.add(drained)" in source
        assert "partitions_created.add(" in source
        assert _points(delta, "scheduler.job.runs.total")

    @pytest.mark.parametrize(
        ("job", "lock"),
        [
            ("rollups", ADVISORY_LOCK_ROLLUPS),
            ("ownership", ADVISORY_LOCK_OWNERSHIP),
            ("partitions", ADVISORY_LOCK_PARTITIONS),
            ("retention", ADVISORY_LOCK_RETENTION),
        ],
    )
    async def test_every_job_reports_lock_held(self, test_engine, metric_delta, job, lock):
        """All four, because `_observed` was only ever driven through
        `run_partitions`: a missing `run.outcome = "lock_held"` in any of the
        other three, or a typo in a `job` label, was invisible.

        The label space is the advisory-lock set from `shared/const.py`, and
        this pins it."""
        sessions = local_sessionmaker(test_engine)
        crm = local_sessionmaker(test_engine)
        async with advisory_lock(lock, engine=test_engine) as held:
            assert held, "the fixture could not take the lock it needs to hold"
            if job == "rollups":
                await scheduler.run_rollups(sessions, now=NOW, active=None, engine=test_engine)
            elif job == "ownership":
                # An EMPTY set, not None: None refuses before the lock is even
                # tried (see TestInactiveCommunities), and this is about the lock.
                await scheduler.run_ownership(
                    sessions, crm, now=NOW, active=frozenset(), engine=test_engine
                )
            elif job == "partitions":
                await scheduler.run_partitions(sessions, now=NOW, engine=test_engine)
            else:
                await scheduler.run_retention(sessions, now=NOW, engine=test_engine)

        runs = _points(metric_delta(), "scheduler.job.runs.total")
        assert (("job", job), ("outcome", "lock_held")) in runs, runs

    async def test_a_lock_held_elsewhere_is_not_a_quiet_success(self, test_engine, metric_delta):
        """THE SECOND-REPLICA CASE.

        A job that cannot take its advisory lock returns normally with an empty
        result, which from the scheduler loop is indistinguishable from a job
        that had nothing to do. A second replica therefore does no work at all,
        for ever, while both containers report healthy and both log the same
        nothing.
        """
        from shared.const import ADVISORY_LOCK_PARTITIONS
        from worker.context import advisory_lock

        sessions = local_sessionmaker(test_engine)
        async with advisory_lock(ADVISORY_LOCK_PARTITIONS, engine=test_engine) as held:
            assert held, "the fixture could not take the lock it needs to hold"
            created = await scheduler.run_partitions(sessions, now=NOW, engine=test_engine)

        assert created == []
        runs = _points(metric_delta(), "scheduler.job.runs.total")
        assert (
            ("job", "partitions"),
            ("outcome", "lock_held"),
        ) in runs, f"a skipped run must be labelled, got {runs}"

    async def test_a_failing_job_is_counted_and_still_raises(self, test_engine, metric_delta):
        """Observation must not swallow. The scheduler loop's own try/except is
        what decides whether the tick continues; this only watches."""
        sessions = local_sessionmaker(test_engine)

        async def boom(*args, **kwargs):
            raise RuntimeError("partition DDL failed")

        import worker.partitions as partitions_module

        original = partitions_module.ensure_partitions
        partitions_module.ensure_partitions = boom
        try:
            with pytest.raises(RuntimeError):
                await scheduler.run_partitions(sessions, now=NOW, engine=test_engine)
        finally:
            partitions_module.ensure_partitions = original

        runs = _points(metric_delta(), "scheduler.job.runs.total")
        assert (("job", "partitions"), ("outcome", "failed")) in runs

    def test_maintenance_is_not_a_job_value(self):
        """`run_maintenance` CALLS `run_partitions` and `run_retention`. Counting
        it as a peer would make one nightly event increment three series, and an
        exception inside retention would report as two failed jobs - the one that
        failed and the one containing it."""
        source = inspect.getsource(scheduler.run_maintenance)
        assert "_observed(" not in source


class TestRollupLag:
    """`/ops/health` answers rollup freshness per community, to a manager of that
    community. Nothing answers it for the fleet, and the fleet's own MAX(bucket)
    is the wrong number: thirty-nine healthy communities hold it at a few minutes
    while the fortieth is frozen."""

    async def test_the_gauge_reports_the_worst_community_not_the_newest_bucket(
        self, db_session: AsyncSession, test_engine
    ):
        await db_session.execute(
            text(
                "INSERT INTO rollup_community_hour "
                "(id_community, bucket, import_wh, export_wh, n_devices, "
                " n_devices_production, n_members, n_devices_unattributed, computed_at) "
                "VALUES (9101, :recent, 0, 0, 1, 1, 1, 0, :recent), "
                "       (9102, :old,    0, 0, 1, 1, 1, 0, :old)"
            ),
            {
                "recent": NOW - datetime.timedelta(minutes=30),
                "old": NOW - datetime.timedelta(days=2),
            },
        )
        await db_session.commit()

        app_metrics.rollup_newest_bucket_epoch.clear()
        try:
            await scheduler._refresh_rollup_lag(_sessionmaker_for(db_session), only=None)
            epoch = app_metrics.rollup_newest_bucket_epoch.get("data")
            assert epoch is not None
            # The FROZEN community's newest bucket, two days old - not the
            # 30 minutes a fleet-wide MAX(bucket) would have reported.
            expected = (NOW - datetime.timedelta(days=2)).timestamp()
            assert epoch == pytest.approx(expected, abs=3600)

            # And the SECOND scope, which is what actually says the scheduler is
            # alive. `data` climbs when a single meter dies - the commonest
            # incident in the service - so alerting on it alone would page for a
            # dead P1 port and stay quiet for a dead scheduler.
            tick = app_metrics.rollup_newest_bucket_epoch.get("tick")
            assert tick is not None
            assert tick == pytest.approx(NOW.timestamp(), abs=3600)
            assert tick > epoch, "the tick recomputed data older than itself"
        finally:
            app_metrics.rollup_newest_bucket_epoch.clear()
            await db_session.execute(
                text("DELETE FROM rollup_community_hour WHERE id_community IN (9101, 9102)")
            )
            await db_session.commit()

    def test_the_gauge_ages_itself_when_the_tick_stops(self):
        """THE INVERSION THIS REPLACED.

        The snapshot first held the LAG, and the tick was its only writer - so a
        tick that stopped left the callback re-exporting "four minutes behind"
        for the life of the process. The one failure the gauge exists for would
        have rendered as a flat, healthy line.

        Holding the INSTANT and subtracting at read time makes it climb in real
        time from the moment the tick stops, with no writer at all.
        """
        app_metrics.rollup_newest_bucket_epoch.clear()
        app_metrics.rollup_newest_bucket_epoch["data"] = time.time() - 600
        first = next(iter(app_metrics._rollup_lag_callback(None))).value
        assert first == pytest.approx(600, abs=5)

        # Same dict, untouched, one hour later.
        app_metrics.rollup_newest_bucket_epoch["data"] -= 3600
        second = next(iter(app_metrics._rollup_lag_callback(None))).value
        assert second == pytest.approx(4200, abs=5)
        assert second > first
        app_metrics.rollup_newest_bucket_epoch.clear()

    def test_a_bucket_in_the_future_does_not_read_as_negative(self):
        """A clock-skewed device can write a bucket ahead of the server. A
        negative age would render below every threshold and read as the freshest
        possible data."""
        app_metrics.rollup_newest_bucket_epoch.clear()
        app_metrics.rollup_newest_bucket_epoch["data"] = time.time() + 7200
        assert next(iter(app_metrics._rollup_lag_callback(None))).value == 0.0
        app_metrics.rollup_newest_bucket_epoch.clear()

    def test_an_empty_snapshot_observes_nothing_rather_than_zero(self):
        """Zero would read as "perfectly fresh" for a scheduler that has never
        run - the exact inversion of the signal. An ABSENT series is what a
        staleness alert should fire on."""
        app_metrics.rollup_newest_bucket_epoch.clear()
        assert list(app_metrics._rollup_lag_callback(None)) == []

    async def test_a_failure_to_measure_does_not_fail_the_tick(self):
        """The rollups are already committed by the time this runs. Losing one
        observation is not worth losing them - and because the gauge ages itself,
        a run of failures is not silent: the number keeps climbing."""

        def exploding_sessionmaker():
            raise RuntimeError("no session for you")

        app_metrics.rollup_newest_bucket_epoch.clear()
        await scheduler._refresh_rollup_lag(exploding_sessionmaker, only=None)  # must not raise
        assert app_metrics.rollup_newest_bucket_epoch == {}


class TestEveryInstrumentIsActuallyFed:
    """One DELTA assertion per scheduler instrument, driven through the real
    job against a real session.

    Six of the fifteen had no test naming them at all, and the paths that were
    "covered" were covered by `inspect.getsource(...)` plus a substring match.
    A string match cannot tell an emitted counter from one emitted with the
    wrong attribute, the wrong sign, or - as happened here - never emitted at
    all: `partition.default.rows_drained.total` shipped declared, documented
    in the runbook as an alert, and wired to nothing."""

    async def test_a_failing_community_is_counted_while_the_run_reports_ok(
        self, db_session, community, monkeypatch, metric_delta, test_engine
    ):
        """The swallow is deliberate - one community must not stop the tick for
        the other forty - which is exactly why the run as a whole reports
        success and this counter is the only aggregate signal that anything
        went wrong."""
        import worker.rollups as rollups_module

        ean = await create_owned_meter(db_session, id_community=community.id, id_member=1)
        await create_device(db_session, id_community=community.id, ean=ean)

        async def boom(*args, **kwargs):
            raise RuntimeError("this community is wedged")

        monkeypatch.setattr(rollups_module, "tick_community", boom)
        done = await scheduler.run_rollups(
            _sessionmaker_for(db_session),
            now=NOW,
            active=frozenset({community.id}),
            engine=test_engine,
        )

        delta = metric_delta()
        assert done == 0, "the run swallowed the failure, as designed"
        assert _points(delta, "rollup.communities.total") == {(("outcome", "failed"),): 1}
        # ...and the JOB still reports ok. That pairing is the finding.
        assert (("job", "rollups"), ("outcome", "ok")) in _points(delta, "scheduler.job.runs.total")

    async def test_a_healthy_community_is_counted_too(
        self, db_session, community, metric_delta, test_engine
    ):
        """The positive control. Without it the test above is satisfied by a
        counter that only ever reports failures."""
        ean = await create_owned_meter(db_session, id_community=community.id, id_member=2)
        await create_device(db_session, id_community=community.id, ean=ean)
        await scheduler.run_rollups(
            _sessionmaker_for(db_session),
            now=NOW,
            active=frozenset({community.id}),
            engine=test_engine,
        )
        assert _points(metric_delta(), "rollup.communities.total") == {(("outcome", "ok"),): 1}

    async def test_the_ownership_projection_reports_its_size(
        self, db_session, community, metric_delta, test_engine
    ):
        """ZERO IS THE ALARM. The refresh deletes a community's windows before
        rewriting them, so a CRM read that legitimately returns nothing wipes
        the projection and reports a successful run. The counter is the full
        size of the recomputed projection, so it stops moving when that."""
        ean = await create_owned_meter(db_session, id_community=community.id, id_member=3)
        await create_device(db_session, id_community=community.id, ean=ean)

        sessions = _sessionmaker_for(db_session)
        await scheduler.run_ownership(
            sessions, sessions, now=NOW, active=frozenset({community.id}), engine=test_engine
        )
        assert _total(metric_delta(), "ownership.windows.written.total") >= 1

    async def test_retention_reports_what_it_dropped(self, test_engine, metric_delta):
        """Structurally zero until the service is about thirteen months old, so
        what is asserted is that the job RUNS and the emitter is reached - a
        flat zero here proves nothing on its own, and the runbook says so."""
        sessions = local_sessionmaker(test_engine)
        result = await scheduler.run_retention(sessions, now=NOW, engine=test_engine)
        assert result.partitions_dropped == []
        assert (("job", "retention"), ("outcome", "ok")) in _points(
            metric_delta(), "scheduler.job.runs.total"
        )

    async def test_retention_reports_the_dead_letters_it_pruned(
        self, db_session, test_engine, metric_delta
    ):
        """NOT structurally zero for a year, unlike the partitions: it moves from
        the first night a dead letter turns `RETENTION_DEAD_LETTER_DAYS` old.
        Rows, not batches - the batch size is a tuning constant, and a counter
        of statements would change meaning every time someone tuned it."""
        await db_session.execute(
            text(
                "INSERT INTO ingest_dead_letter (topic, reason, received_at) "
                "VALUES ('ce/x', 'schema_invalid', :at), ('ce/y', 'device_unknown', :at)"
            ),
            {"at": NOW - datetime.timedelta(days=settings.RETENTION_DEAD_LETTER_DAYS + 1)},
        )

        result = await scheduler.run_retention(
            _sessionmaker_for(db_session), now=NOW, engine=test_engine
        )

        assert result.dead_letters_pruned == 2
        assert _total(metric_delta(), "dead_letters.pruned.total") == 2

    async def test_the_gauge_is_exported_not_just_stored(self, db_session, metric_reader):
        """Read through the METRIC READER, not off the dict.

        Every other gauge test reads `rollup_newest_bucket_epoch` directly, so
        deleting `create_observable_gauge` would leave them all green while the
        number was exported by nobody."""
        app_metrics.rollup_newest_bucket_epoch.clear()
        try:
            app_metrics.rollup_newest_bucket_epoch["data"] = time.time() - 300
            app_metrics.rollup_newest_bucket_epoch["tick"] = time.time() - 60
            seen = {
                tuple(point.attributes.items()): point.value
                for rm in metric_reader.get_metrics_data().resource_metrics
                for sm in rm.scope_metrics
                for metric in sm.metrics
                if metric.name == "rollup.lag.seconds"
                for point in metric.data.data_points
            }
            assert (("scope", "data"),) in seen, seen
            assert (("scope", "tick"),) in seen, seen
            assert seen[(("scope", "data"),)] == pytest.approx(300, abs=10)
        finally:
            app_metrics.rollup_newest_bucket_epoch.clear()


class TestPerCommunityFailures:
    def test_the_swallowed_failure_is_counted(self):
        """`run_rollups` logs and skips a failing community so one community
        cannot stop the tick for the other forty. That is right, and it is why
        the run as a whole then reports SUCCESS - one frozen community has no
        aggregate signal at all without this counter."""
        source = inspect.getsource(scheduler.run_rollups)
        assert 'rollup_communities.add(1, {"outcome": "failed"})' in source
        assert 'rollup_communities.add(1, {"outcome": "ok"})' in source


class TestBothEntrypointsAreWired:
    """The scheduler installed NO meter provider: `worker/scheduler_main.py`
    never called `setup_tracer_provider()`, so in staging and production it
    exported nothing - not under the wrong name, under no name. Every instrument
    added to it would have bound to a proxy that discards silently, and every
    chart would have read zero while the jobs ran perfectly."""

    @pytest.mark.parametrize(
        ("module", "component"),
        [("worker.main", "ingest-worker"), ("worker.scheduler_main", "scheduler")],
    )
    def test_the_entrypoint_installs_a_provider_and_names_itself(self, module, component):
        source = inspect.getsource(__import__(module, fromlist=["main"]).main)
        assert f'setup_tracer_provider("{component}")' in source

    @pytest.mark.parametrize("module", ["worker.main", "worker.scheduler_main"])
    def test_the_entrypoint_flushes_before_it_exits(self, module):
        """Not because the SDK would otherwise drop the data - its atexit
        handler does flush - but because that handler uses a 30-second budget
        against Docker's 10-second stop grace, so a slow collector gets the
        container SIGKILLed mid-flush. The explicit call bounds it to 5 s."""
        source = inspect.getsource(__import__(module, fromlist=["main"]).main)
        assert "shutdown_telemetry()" in source

    def test_setup_runs_before_the_first_thing_worth_counting(self):
        """Instruments rebind when the provider arrives; they are NOT replayed.
        Anything counted first is discarded, not even as a zero."""
        for module, first in (
            ("worker.main", "_install_signal_handlers"),
            ("worker.scheduler_main", "_install_signal_handlers"),
        ):
            source = inspect.getsource(__import__(module, fromlist=["main"]).main)
            assert source.index("setup_tracer_provider") < source.index(first)

    def test_the_flush_is_bounded_below_dockers_stop_grace(self):
        """THE WHOLE POINT OF CALLING IT EXPLICITLY.

        `MeterProvider(shutdown_on_exit=True)` - the default - already registers
        an atexit flush, and it works. But atexit calls `shutdown()` with no
        arguments, so it runs on the SDK's 30-second default while Docker sends
        SIGKILL 10 seconds after SIGTERM. A default raised above that grace
        period would silently restore the behaviour this replaced: killed
        mid-flush, data lost anyway, and every deploy paying the full grace
        period per container.
        """
        import inspect

        from core.tracing import EXPORTER_TIMEOUT_SECONDS, shutdown_telemetry

        default = inspect.signature(shutdown_telemetry).parameters["timeout_millis"].default
        # Only the bound carries information. Comparing it to
        # EXPORTER_TIMEOUT_MS would compare the default against the very name
        # that IS the default, in the same module.
        assert default < 10_000, "Docker's default stop grace period"
        assert default == EXPORTER_TIMEOUT_SECONDS * 1000

    def test_flushing_is_safe_when_no_provider_was_ever_installed(self):
        """ENV=local returns early, and a crash before setup leaves it unset.
        Neither may stop a container from exiting."""
        from core.tracing import shutdown_telemetry

        shutdown_telemetry()  # must not raise

    def test_a_collector_that_refuses_to_flush_does_not_block_the_exit(self, monkeypatch):
        """The branch the test above never reaches - it returns at the first
        `is None`. A container on its way out has nowhere to report to, so a
        provider that raises must be swallowed, and twice must be safe."""
        import core.tracing as tracing

        calls = []

        class Hostile:
            def shutdown(self, timeout_millis=None):
                calls.append(("metrics", timeout_millis))
                raise RuntimeError("the collector is gone")

            def force_flush(self, timeout_millis=None):
                calls.append(("logs", timeout_millis))
                raise RuntimeError("the collector is gone")

        monkeypatch.setattr(tracing, "_meter_provider", Hostile())
        monkeypatch.setattr(tracing, "_log_provider", Hostile())
        tracing.shutdown_telemetry()
        tracing.shutdown_telemetry()
        assert [name for name, _ in calls] == ["logs", "metrics"] * 2
        # ...and both got the BOUND, which is the whole point of the function.
        assert all(budget == tracing.EXPORTER_TIMEOUT_MS for _, budget in calls)


class TestTheDocstringClaimIsTrueNow:
    """`domain/reasons.py` has described this counter twice while it did not
    exist - first as a metric, then, after a "correction", as an `ingest_reject`
    TABLE that does not exist either. A guard that outlives both."""

    def test_reasons_points_at_something_real(self):
        import domain.reasons as reasons

        doc = reasons.RejectReason.__doc__ or ""
        # The BACKTICKED table reference, not the bare substring: the counter is
        # called `ingest_rejections`, which contains it.
        assert "ingest_rejections" in doc, "the docstring must name the real counter"
        # It may still MENTION the table - it explains why the earlier claim was
        # wrong - but it must say so rather than assert it.
        assert "does not exist" in doc
        assert hasattr(app_metrics, "ingest_rejections")

    def test_the_named_table_really_is_absent(self):
        """The reason the second claim was wrong, pinned so a third attempt at
        this paragraph has to check."""
        schema = (
            __import__("pathlib").Path(__file__).resolve().parents[1]
            / "scripts"
            / "sql"
            / "schema.sql"
        ).read_text(encoding="utf-8")
        assert "ingest_reject (" not in schema
        assert "CREATE TABLE IF NOT EXISTS ingest_dead_letter" in schema
