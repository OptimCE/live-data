"""The public enrolment DTOs. FROZEN at build step 5.

`live-data-protocol.md` 5 expressed in Pydantic. These shapes are a published
interface: `optimce-connector` and both planned ESP32 firmwares implement against
them from outside this repository, and a firmware stores the response in NVS and
never asks again.

ONE CALL, because the device may be an ESP32 being configured through a captive
portal by someone holding a phone in a basement. Everything a connector needs
comes back in a single response.

NO `from __future__ import annotations` IN THE ROUTE MODULES THAT USE THESE.
`with_default_error` resolves string annotations against its OWN module globals,
so a stringified Pydantic body type becomes invisible to FastAPI and gets demoted
to a QUERY parameter - the symptom is a 422 with `loc: [query, body]` and a body
that was never parsed. It bites here in particular because POST /enroll is the
only unauthenticated route on the platform, so its 422 has no obvious owner.
"""

from pydantic import BaseModel, ConfigDict, Field


class ConnectorInfo(BaseModel):
    """Which connector is enrolling, and at what version.

    Recorded on the device row so that "which devices are running the broken
    0.3.0?" has an answer. Refreshed on every subsequent `status` message
    (protocol 3.2), not only here.
    """

    model_config = ConfigDict(extra="forbid")

    name: str = Field(max_length=64, examples=["optimce-connector"])
    version: str = Field(max_length=32, examples=["0.3.1"])


class EnrollRequest(BaseModel):
    """POST /enroll. Public, rate-limited, no user authentication.

    Note what is NOT here: no community, no EAN, no device id. The token is the
    only credential and it is what identifies the device - everything else is
    resolved from the device row the token points at. A request that carried a
    community would be a request that could claim one.
    """

    model_config = ConfigDict(extra="forbid")

    token: str = Field(
        min_length=8,
        max_length=64,
        examples=["K7M9-P2QR-4TVX"],
        description=(
            "The enrolment token, as issued. Crockford base32, grouped with "
            "hyphens; case and hyphens are normalised server-side so it can be "
            "typed into a captive portal or read aloud."
        ),
    )
    connector: ConnectorInfo


class BrokerInfo(BaseModel):
    """Where to connect.

    `host` is the PUBLIC broker address - `settings.BROKER_PUBLIC_HOST`, never
    `settings.MQTT_HOST`. The latter is a compose service name reachable only on
    the internal network, and a device that stores it in NVS is bricked
    permanently: there is no remote update and the fix is a site visit to a box
    with no keyboard. `core/config.py` asserts the two differ in
    staging/production for exactly this reason.
    """

    model_config = ConfigDict(extra="forbid")

    host: str = Field(examples=["mqtt.optimce.be"])
    port: int = Field(examples=[8883])
    tls: bool = Field(examples=[True])


class CredentialsInfo(BaseModel):
    """Username and password.

    THE PASSWORD IS SHOWN ONCE AND IS NEVER RECOVERABLE. OptimCE does not store
    it - the broker keeps only a hash. A lost secret means re-enrolment with a
    fresh token, which is a normal operation rather than an incident.

    The username IS the `device_id` (protocol 1). The broker also PINS the client
    id to it: connecting with any other id is refused, which enforces protocol
    4.3 at the broker rather than by asking connector authors nicely.

    ---- the mTLS trajectory, frozen by D-4c ----
    A later move to mTLS replaces this object with `{cert, key}` and changes
    nothing else about the flow. That shape is SERVER-GENERATED, which means it
    forecloses device-generated-key + CSR - and that is the decision, not an
    oversight: CSR would break one-call enrolment through a captive portal, which
    is the product requirement this whole endpoint exists to satisfy.
    """

    model_config = ConfigDict(extra="forbid")

    username: str
    password: str


class TopicsInfo(BaseModel):
    """The device's two topics, precomputed.

    A device may publish only to these and may not subscribe at all; the broker
    enforces it. They are returned rather than described so that a firmware never
    has to build a topic string - and so that the exact spelling the broker's ACL
    will match is the exact spelling the device was handed.
    """

    model_config = ConfigDict(extra="forbid")

    telemetry: str = Field(examples=["ce/42/3f1a.../telemetry"])
    status: str = Field(examples=["ce/42/3f1a.../status"])


class EnrollResponse(BaseModel):
    """Everything the device needs, in one response.

    Shaped so a firmware can store it in NVS and never ask again.
    """

    model_config = ConfigDict(extra="forbid")

    device_id: str
    broker: BrokerInfo
    credentials: CredentialsInfo
    topics: TopicsInfo


class RotateRequest(BaseModel):
    """POST /enroll/rotate. protocol 5.4.

    ---- SHAPE ONLY. THERE IS NO ROUTE. ----

    protocol 5.4: "The ENDPOINT arrives after the first release; the request and
    response shapes are fixed now so that implementing against them is safe."
    Connectors should implement rotation from the start, because a deployed
    device that cannot change its credentials can only ever be re-enrolled by
    hand - but for a pilot fleet, revoke-and-re-enrol is the answer.

    Defining the models without registering a route is deliberate and is what
    keeps the promise cheap: no route means no OpenAPI path, so nothing for the
    two-spec disjointness assertion to trip over, no gin route-table entry, no
    nginx location, no rate-limit zone - and, most of the point, no second
    PUBLIC USERNAME/PASSWORD ORACLE that also competes with real devices for the
    broker's connection budget (plan 8.7).

    The response is `EnrollResponse`, unchanged. That is not laziness: the shapes
    being identical is what makes D-4c's later `{cert, key}` swap work on both
    paths at once.
    """

    model_config = ConfigDict(extra="forbid")

    username: str
    password: str
