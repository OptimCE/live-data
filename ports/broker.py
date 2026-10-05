"""The device broker port: Protocol, value objects, typed errors, and a fake.

The real adapter is `ports/broker_mqtt.py`; which one is used is decided in
`ports/providers.py`, which imports nothing from fastapi so that both
`api/live/deps.py` and `worker/` can call it.

----------------------------------------------------------------------------
WHY THE VERBS ARE DEVICE-LIFECYCLE AND NOT `cmd(command, **kwargs)`

A generic command port would push two pieces of hard-won knowledge into every
caller: the ordering rule (create the broker client BEFORE committing) and the
on-already-exists branch. The reconciliation sweep that arrives at build step 11
would then re-implement both, differently, and the two would drift.

So the port speaks in intentions - create_device, set_device_password,
disable_device, delete_device, clear_retained_status - and the dynsec mechanics
live in one adapter.
----------------------------------------------------------------------------

WHY THE ERRORS ARE A TYPED SET

dynamic-security returns its errors INSIDE the response payload while the
publish itself is PUBACKed normally. An adapter that only checks delivery
reports success for every rejected command. Worse, a malformed payload gets a
response with NO `correlationData` at all, so the caller's future can never
resolve - which is why `BrokerTimeoutError` exists and why the Phase 0 spike's
`{"__timeout__": True}` sentinel dict was NOT promoted: a sentinel a caller
forgets to check is the same bug one level up.

`BrokerClientExistsError` is separate from `BrokerCommandFailedError` for a specific
reason: `createClient` is NOT idempotent, and a retry inside the enrolment claim
lease MUST fall through to `set_device_password` rather than fail. Without that
branch the retry burns exactly the credential the lease exists to protect.
"""

from dataclasses import dataclass
from typing import Protocol


class BrokerError(Exception):
    """Base for every broker failure the port can raise."""


class BrokerUnavailableError(BrokerError):
    """No usable control connection. -> 503.

    NOT the same as a refused command: the operation may not have been attempted
    at all.
    """


class BrokerTimeoutError(BrokerError):
    """A command was published and no correlated response arrived in time.

    A TIMEOUT IS NOT A FAILURE. The command may well have succeeded server-side
    - dynsec has no transaction and no rollback - which is precisely why the
    enrolment retry path branches on already-exists instead of assuming nothing
    happened.
    """


class BrokerCommandFailedError(BrokerError):
    """dynsec answered, and the answer was an error."""

    def __init__(self, command: str, message: str) -> None:
        super().__init__(f"{command}: {message}")
        self.command = command
        self.message = message


class BrokerClientExistsError(BrokerCommandFailedError):
    """`createClient` refused because the username already exists.

    Its own type because it is a dynsec error with a recovery path rather than
    a report: fall through to `set_device_password`.
    """


class BrokerClientNotFoundError(BrokerCommandFailedError):
    """dynsec refused because the client does not exist.

    The mirror of `BrokerClientExistsError`, and it has a recovery path for the
    same reason: on the REVOKE side, a client that is not there is the state
    revocation is trying to reach. Treating it as a failure is what made a
    device that was created and never enrolled impossible to revoke - 502, row
    stuck PENDING, and its EAN locked for ever because
    `uq_device_community_ean_live` excludes only REVOKED rows.

    It is NOT benign everywhere. `set_device_password` raising this during
    enrolment means the create/exists branch reached a client that has since
    vanished, which is a genuine fault - so the type exists to let the caller
    decide, never to be swallowed at the adapter.
    """


@dataclass(frozen=True, slots=True)
class DeviceCredentials:
    """What a device is created with.

    `client_id` is pinned at the broker, not merely recommended. A client created
    with `clientid` set cannot connect with any other id - `Connection Refused:
    not authorised` - which enforces protocol 4.3 at the broker instead of by
    asking connector authors nicely. Enrolment always sets it.
    """

    username: str
    password: str
    client_id: str


class DeviceBrokerPort(Protocol):
    """Everything the service does to the broker. Five verbs, no escape hatch."""

    async def create_device(self, credentials: DeviceCredentials, role: str) -> None:
        """Create the client and attach its role.

        Raises BrokerClientExistsError when the username is taken - the caller is
        expected to fall through to `set_device_password`, not to give up.
        """
        ...

    async def set_device_password(self, username: str, password: str) -> None:
        """Set a client's password. Idempotent, which is what makes it the
        recovery path for a retried enrolment."""
        ...

    async def disable_device(self, username: str) -> None:
        """Disable a client. Drops any live connection and refuses reconnection."""
        ...

    async def delete_device(self, username: str) -> None:
        """Delete a client.

        Phase 0 measured this: `deleteClient` DOES cut a live connection, in
        ~47 ms, on both 2.0.22 and 2.1.2 - the plan's claim that only
        `disableClient` is documented to do so was wrong. Revocation still calls
        disable first, but for the retained-status clear that sits between them,
        not because delete alone would leave a session publishing.
        """
        ...

    async def clear_retained_status(self, topic: str) -> None:
        """Publish a zero-length RETAINED message to a device's status topic.

        Needs the `reaper` role: a retained `status` that outlives its device
        replays on every worker reconnect, so the offline alert fires on every
        deploy until the team stops reading it.
        """
        ...

    async def is_ready(self) -> bool:
        """Whether a command would be attempted at all.

        Reported under /health/readiness's `detail`, never gated on: compose's
        healthcheck reads readiness, and a broker restart must not roll the API
        - and with it the whole admin surface - out of service.
        """
        ...


class FakeDeviceBroker:
    """In-memory broker for tests. Records calls; raises what it is told to.

    The suite drives this rather than a container. GitHub Actions creates
    `services:` containers BEFORE `actions/checkout`, so a repo-tracked
    mosquitto.conf cannot be their bind-mount source - and the house policy
    already says as much for NATS: unit-test the surface we expose to the broker,
    and verify the broker's real behaviour end to end against the dev stack
    (scripts/verify-live-ingest.sh). Recorded as D-7.
    """

    def __init__(self, *, ready: bool = True) -> None:
        self.ready = ready
        self.clients: dict[str, DeviceCredentials] = {}
        self.roles: dict[str, str] = {}
        self.disabled: set[str] = set()
        self.cleared_topics: list[str] = []
        self.calls: list[tuple[str, str]] = []
        # Primed failures, keyed by verb. Set one to make the next call raise.
        self.fail_with: dict[str, BrokerError] = {}

    def _maybe_fail(self, verb: str) -> None:
        error = self.fail_with.pop(verb, None)
        if error is not None:
            raise error

    async def create_device(self, credentials: DeviceCredentials, role: str) -> None:
        self.calls.append(("create_device", credentials.username))
        self._maybe_fail("create_device")
        if credentials.username in self.clients:
            # Modelled faithfully: createClient is NOT idempotent.
            raise BrokerClientExistsError("createClient", "Client already exists")
        self.clients[credentials.username] = credentials
        self.roles[credentials.username] = role

    async def set_device_password(self, username: str, password: str) -> None:
        self.calls.append(("set_device_password", username))
        self._maybe_fail("set_device_password")
        existing = self.clients.get(username)
        if existing is None:
            raise BrokerCommandFailedError("setClientPassword", "Client not found")
        self.clients[username] = DeviceCredentials(username, password, existing.client_id)

    # Both of these used to succeed on a client that was never created, which
    # is why 270 tests did not catch a device that could not be revoked. A fake
    # more forgiving than the thing it stands in for cannot fail the way
    # production does.
    async def disable_device(self, username: str) -> None:
        self.calls.append(("disable_device", username))
        self._maybe_fail("disable_device")
        if username not in self.clients:
            raise BrokerClientNotFoundError("disableClient", "Client not found")
        self.disabled.add(username)

    async def delete_device(self, username: str) -> None:
        self.calls.append(("delete_device", username))
        self._maybe_fail("delete_device")
        if username not in self.clients:
            raise BrokerClientNotFoundError("deleteClient", "Client not found")
        self.clients.pop(username, None)
        self.roles.pop(username, None)
        self.disabled.discard(username)

    async def clear_retained_status(self, topic: str) -> None:
        self.calls.append(("clear_retained_status", topic))
        self._maybe_fail("clear_retained_status")
        self.cleared_topics.append(topic)

    async def is_ready(self) -> bool:
        return self.ready
