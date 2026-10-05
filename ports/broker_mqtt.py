"""The real broker adapter: Mosquitto dynamic-security over its $CONTROL topics.

Promoted from `live-data-spike/spike_lib.py`'s `DynsecAdmin`, which Phase 0 used
to establish what this API actually does. Four changes from the spike, each
because this runs inside a request rather than inside a throwaway:

  * the 5.0 s timeout becomes `MQTT_COMMAND_TIMEOUT_MS` (800 ms). KrakenD's
    `global.timeout` is 3000 ms with NO per-route override, and the enrolment
    retry path is two commands;
  * the `{"__timeout__": True}` sentinel becomes a RAISED `BrokerTimeoutError`.
    A sentinel dict a caller forgets to check is precisely the
    errors-inside-the-payload bug one level up;
  * `raw` and `orphans` become bounded deques with counters. Unbounded lists are
    a slow leak in a process that lives for weeks;
  * ONE command per publish, never a batch.

================================================================================
FIVE THINGS PHASE 0 MEASURED THAT THIS ADAPTER IS SHAPED BY

1.  ERRORS COME BACK INSIDE THE PAYLOAD. A rejected command is PUBACKed
    normally and answers `{"command":"createClient","error":"Client already
    exists",...}`. An adapter that checks the publish result reports success for
    every rejected command.

2.  THE RESPONSE TOPIC IS BROADCAST. A second admin connection received the
    first connection's responses. Correlating on `correlationData` is therefore
    MANDATORY rather than stylistic - without it two API replicas resolve each
    other's futures. (It also reinforces "one replica", commented in worker/main.)

3.  A MALFORMED PAYLOAD ANSWERS WITH NO `correlationData` AT ALL. So a caller's
    future can never resolve, and an unbounded await would deadlock inside a
    request the gateway cuts at 3000 ms. The hard timeout is the whole reason
    `BrokerTimeoutError` exists.

4.  A BATCH IS NOT ATOMIC. `{"commands":[ok, fails, ok]}` executed the third
    command after the second failed, with no rollback - leaving a half-configured
    client and no way to know which half. Hence one command per publish.

5.  `createClient` IS NOT IDEMPOTENT. A repeat answers "Client already exists",
    which is why that one error has its own exception type: it is the ONE dynsec
    error with a recovery path rather than a report.
================================================================================

RESPONSES ARE NOT RETAINED, so the subscription must be established BEFORE the
first publish. A subscribe-after-publish loses the first response every time -
and "every time" means the very first enrolment after every restart.
"""

import asyncio
import contextlib
import json
import logging
import uuid
from collections import deque
from typing import Any

import aiomqtt

from core.config import settings
from ports.broker import (
    BrokerClientExistsError,
    BrokerClientNotFoundError,
    BrokerCommandFailedError,
    BrokerTimeoutError,
    BrokerUnavailableError,
    DeviceCredentials,
)
from shared.const import DYNSEC_REQUEST_TOPIC, DYNSEC_RESPONSE_TOPIC
from shared.mqtt_tls import tls_params

logger = logging.getLogger(__name__)

_RECONNECT_BASE_DELAY_SECONDS = 1
_RECONNECT_MAX_DELAY_SECONDS = 30
# How long a caller waits for the connection to come up before giving up with
# 503. Deliberately short: the caller is inside a 3000 ms gateway budget, and a
# broker that is down should fail fast rather than consume the whole of it.
_READY_WAIT_SECONDS = 1.0
# Bounded, because this process lives for weeks. These exist for debugging, not
# for correctness.
_DEBUG_RING = 64

# dynsec's exact wording when a client id is taken. Matched case-insensitively
# on a substring rather than equality: it is a human-readable string from a C
# program, not a code, and pinning the whole sentence would make a harmless
# upstream rewording turn a recoverable retry into a burned credential.
_ALREADY_EXISTS = "already exists"
# dynsec's own wording, taken from the broker rather than guessed:
#   $ mosquitto_ctrl ... dynsec disableClient no-such-client
#   disableClient: Error: Client not found.
_CLIENT_NOT_FOUND = "client not found"


class MqttDeviceBroker:
    """A long-lived dynsec control connection.

    NOT a connection per request: a TCP plus TLS handshake inside a request
    KrakenD cuts at 3000 ms is not viable, and that timeout is not overridable
    per route here.
    """

    def __init__(self) -> None:
        self._client: aiomqtt.Client | None = None
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._ready = asyncio.Event()
        self._shutdown = asyncio.Event()
        self._supervisor: asyncio.Task[None] | None = None
        # Debug rings. `orphans` is the interesting one: a response with no
        # matching future means either correlationData was not echoed, or
        # another connection's response reached us.
        self.orphans: deque[dict[str, Any]] = deque(maxlen=_DEBUG_RING)
        self.unparseable: deque[str] = deque(maxlen=_DEBUG_RING)
        self.orphan_count = 0

    # ---- lifecycle ------------------------------------------------------

    async def start(self) -> None:
        """Launch the reconnect supervisor. NEVER RAISES.

        A broker that is down at boot must degrade to 503 on enrolment, not stop
        the API from starting: the device list, the settings and every other
        admin route need no broker at all, and compose's healthcheck reads
        /health/readiness, which deliberately does not gate on this.
        """
        if self._supervisor is not None:
            return
        self._shutdown.clear()
        self._supervisor = asyncio.create_task(self._supervise())

    async def stop(self) -> None:
        self._shutdown.set()
        self._ready.clear()
        if self._supervisor is not None:
            self._supervisor.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._supervisor
            self._supervisor = None
        # Fail any caller still waiting, rather than leaving it to time out.
        for future in self._pending.values():
            if not future.done():
                future.set_exception(BrokerUnavailableError("broker adapter stopped"))
        self._pending.clear()

    async def _supervise(self) -> None:
        delay = _RECONNECT_BASE_DELAY_SECONDS
        while not self._shutdown.is_set():
            try:
                await self._session()
                delay = _RECONNECT_BASE_DELAY_SECONDS
            except aiomqtt.MqttError as exc:
                logger.warning(
                    "dynsec control connection lost: %s",
                    exc,
                    extra={"operation": "broker:disconnected", "retry_in_seconds": delay},
                )
            except Exception:
                logger.exception("dynsec control loop crashed", extra={"operation": "broker:crash"})
            finally:
                self._ready.clear()
                self._client = None

            if self._shutdown.is_set():
                return
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._shutdown.wait(), timeout=delay)
            delay = min(delay * 2, _RECONNECT_MAX_DELAY_SECONDS)

    async def _session(self) -> None:
        """One connection: subscribe, mark ready, then read until it ends."""
        client = aiomqtt.Client(
            hostname=settings.MQTT_HOST,
            port=settings.MQTT_PORT,
            username=settings.MQTT_ADMIN_USERNAME or None,
            password=settings.MQTT_ADMIN_PASSWORD or None,
            identifier=settings.MQTT_ADMIN_CLIENT_ID,
            tls_params=tls_params(),
            protocol=aiomqtt.ProtocolVersion.V311,
            # clean_session=True: this connection holds no queue and wants none.
            # The INGEST worker is the one that needs a persistent session; an
            # admin session surviving restarts would only grow broker state.
            clean_session=True,
            keepalive=60,
        )
        async with client:
            # BEFORE the first publish. Responses are not retained, so
            # subscribing afterwards loses the first one - every time.
            await client.subscribe(DYNSEC_RESPONSE_TOPIC, qos=1)
            self._client = client
            self._ready.set()
            logger.info(
                "dynsec control connection established",
                extra={"operation": "broker:connected"},
            )
            async for message in client.messages:
                if self._shutdown.is_set():
                    return
                self._dispatch(message.payload)

    def _dispatch(self, payload: Any) -> None:
        raw = payload if isinstance(payload, bytes) else bytes(payload or b"")
        try:
            body = json.loads(raw)
        except (ValueError, UnicodeDecodeError):
            self.unparseable.append(raw[:512].decode("utf-8", errors="replace"))
            return
        # dynsec answers either `{"responses": [...]}` or a bare response
        # object; the spike observed both.
        for response in body.get("responses", [body]):
            if not isinstance(response, dict):
                continue
            correlation = response.get("correlationData")
            future = self._pending.pop(correlation, None) if correlation else None
            if future is not None and not future.done():
                future.set_result(response)
            else:
                # THE FINDING, not an anomaly: either correlationData was not
                # echoed (a malformed payload), or this is another connection's
                # response - the topic is broadcast.
                self.orphan_count += 1
                self.orphans.append(response)

    # ---- the command channel --------------------------------------------

    async def _command(self, command: str, **kwargs: Any) -> dict[str, Any]:
        """Send ONE dynsec command and await its correlated response.

        Never a batch: Phase 0 watched `{"commands":[ok, fails, ok]}` execute the
        third command after the second failed, with no rollback.
        """
        client = await self._await_ready()

        correlation = uuid.uuid4().hex
        future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._pending[correlation] = future
        payload = {"commands": [{"command": command, "correlationData": correlation, **kwargs}]}

        try:
            await client.publish(DYNSEC_REQUEST_TOPIC, json.dumps(payload), qos=1)
        except aiomqtt.MqttError as exc:
            self._pending.pop(correlation, None)
            raise BrokerUnavailableError(f"publishing {command} failed: {exc}") from exc

        try:
            async with asyncio.timeout(settings.MQTT_COMMAND_TIMEOUT_MS / 1000):
                response = await future
        except TimeoutError as exc:
            self._pending.pop(correlation, None)
            # A TIMEOUT IS NOT A FAILURE. The command may well have succeeded
            # server-side - dynsec has no transaction and no rollback - which is
            # exactly why the enrolment retry path branches on already-exists
            # instead of assuming nothing happened.
            raise BrokerTimeoutError(
                f"{command} did not answer within {settings.MQTT_COMMAND_TIMEOUT_MS} ms; "
                "it may still have taken effect"
            ) from exc

        error = response.get("error")
        if error:
            # The publish SUCCEEDED and the command was refused. This is the
            # branch an adapter that checks delivery never reaches.
            lowered = str(error).lower()
            if _ALREADY_EXISTS in lowered:
                raise BrokerClientExistsError(command, str(error))
            if _CLIENT_NOT_FOUND in lowered:
                raise BrokerClientNotFoundError(command, str(error))
            raise BrokerCommandFailedError(command, str(error))
        return response

    async def _await_ready(self) -> aiomqtt.Client:
        try:
            async with asyncio.timeout(_READY_WAIT_SECONDS):
                await self._ready.wait()
        except TimeoutError as exc:
            raise BrokerUnavailableError("no dynsec control connection") from exc
        client = self._client
        if client is None:  # pragma: no cover - torn down between the two lines
            raise BrokerUnavailableError("no dynsec control connection")
        return client

    # ---- the port -------------------------------------------------------

    async def create_device(self, credentials: DeviceCredentials, role: str) -> None:
        """Create the client WITH ITS CLIENT ID PINNED, then attach its role.

        `clientid` is what enforces protocol 4.3 at the broker rather than by
        asking connector authors nicely: Phase 0 confirmed that a client created
        with it set cannot connect with any other id - `Connection Refused: not
        authorised`. The plan never says to set it.

        Two commands, not a batch. If the second fails the client exists with no
        role, which publishes nowhere and is swept up by reconciliation - the
        harmless half-failure.
        """
        await self._command(
            "createClient",
            username=credentials.username,
            password=credentials.password,
            clientid=credentials.client_id,
        )
        await self._command("addClientRole", username=credentials.username, rolename=role)

    async def set_device_password(self, username: str, password: str) -> None:
        """Idempotent, which is what makes it the retry path for enrolment."""
        await self._command("setClientPassword", username=username, password=password)

    async def disable_device(self, username: str) -> None:
        await self._command("disableClient", username=username)

    async def delete_device(self, username: str) -> None:
        await self._command("deleteClient", username=username)

    async def clear_retained_status(self, topic: str) -> None:
        """Publish a zero-length RETAINED message to a device's status topic.

        Needs the `reaper` role on this connection. Phase 0 tried this as all
        three identities the original two-role design defined - including the
        dynsec bootstrap admin, whose default role grants publish on $CONTROL and
        nothing else - and every one was `Denied PUBLISH`. That is why there are
        three roles.

        A denied publish is still PUBACKed, so there is nothing to check here and
        no exception to catch: absence of the retained message at a subscriber is
        the only reliable observable. `scripts/verify-live-ingest.sh` is what
        actually proves it.
        """
        client = await self._await_ready()
        try:
            await client.publish(topic, payload=b"", qos=1, retain=True)
        except aiomqtt.MqttError as exc:
            raise BrokerUnavailableError(f"clearing {topic} failed: {exc}") from exc

    async def is_ready(self) -> bool:
        return self._ready.is_set()
