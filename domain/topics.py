"""Parsing and building the two device topics. protocol 2.

    ce/{community_id}/{device_id}/telemetry     device -> OptimCE   QoS 1
    ce/{community_id}/{device_id}/status        device -> OptimCE   QoS 0, retained

Pure: no session, no network, no settings.

----------------------------------------------------------------------------
WHY THE COMMUNITY IN THE TOPIC IS NOT TRUSTED, AND WHY THE CHECK IS LOAD-BEARING

The broker grants a device `publishClientSend` on `ce/+/%u/telemetry`. `%u`
substitutes the device's own username, so a device cannot publish as another
device - that part the broker enforces. But the `+` is a wildcard at the
community level, and it permits ANY value there.

Phase 0 demonstrated this rather than arguing it: `dev-a`, publishing to
`ce/SOMEONE-ELSES-COMMUNITY/dev-a/telemetry`, ARRIVED at the subscriber on
Mosquitto 2.1.2. The broker will not stop it.

So `community_mismatch` is the only thing that does, and it is a real access
control rather than defence in depth. Two ways to get it wrong, both silent:

  * Scoping the device SELECT by the topic's community
    (`WHERE public_id = :u AND id_community = :topic_c`) makes an attacker's
    mismatch report as `device_unknown`. The mismatch counter then reads zero
    for ever and the two alerts become indistinguishable.

  * Calling `with_tenant(topic_community)` before loading the device makes
    `with_community_scope` filter by the ATTACKER'S CLAIM. The device is not
    found, and the check never runs at all.

The order is therefore fixed: parse the topic, load the device by `public_id`
ALONE and deliberately unscoped, then compare. See `worker/ingest.py`.
----------------------------------------------------------------------------
"""

import uuid
from dataclasses import dataclass
from typing import Final

from shared.const import (
    TOPIC_PREFIX,
    TOPIC_STATUS_SUFFIX,
    TOPIC_TELEMETRY_SUFFIX,
)

_EXPECTED_SEGMENTS: Final[int] = 4


@dataclass(frozen=True, slots=True)
class ParsedTopic:
    """What a well-formed device topic carries.

    `claimed_community_id` is named for what it is: a CLAIM, made by the
    publisher, not yet checked against anything. Nothing may use it as a tenant
    until `worker/ingest.py` has compared it with the device row.
    """

    claimed_community_id: int
    device_public_id: uuid.UUID
    kind: str  # TOPIC_TELEMETRY_SUFFIX | TOPIC_STATUS_SUFFIX


def parse_device_topic(topic: str) -> ParsedTopic | None:
    """Parse `ce/{community_id}/{device_id}/{telemetry|status}`.

    Returns None for anything that is not exactly that shape - a deeper level, a
    different prefix, a non-integer community, a malformed UUID. The caller turns
    that into `schema_invalid`; there is no separate `topic_malformed` reason,
    because protocol 4.2 is frozen and does not define one.

    The community id is a DECIMAL INTEGER - the internal `community.id`, not the
    Keycloak org UUID. That is deliberate: it makes the mismatch check a
    comparison rather than a lookup, and a comparison has nowhere to fail open.
    """
    segments = topic.split("/")
    if len(segments) != _EXPECTED_SEGMENTS:
        return None
    prefix, raw_community, raw_device, kind = segments
    if prefix != TOPIC_PREFIX:
        return None
    if kind not in (TOPIC_TELEMETRY_SUFFIX, TOPIC_STATUS_SUFFIX):
        return None
    # `int()` accepts leading whitespace, a sign, and underscores ("1_0" -> 10).
    # None of those can be the internal id, and accepting them would mean two
    # distinct topic strings resolving to one community.
    #
    # THE CHECK IS "IS THIS THE CANONICAL SPELLING", not "does int() accept it".
    #
    # It was `isdigit()`, which differs from what `int()` accepts on 128 code
    # points - and on every one of them this function RAISED instead of returning
    # None. `"²".isdigit()` is True and `int("²")` is a ValueError. The
    # community level is the `+` in the broker ACL `ce/+/%u/telemetry`, so a
    # device chooses it: a superscript two in a topic was an unhandled exception
    # on the ingest path, publishable from a basement.
    #
    # `isdecimal()` alone fixes the crash and NOT the aliasing this comment
    # already forbade - ARABIC-INDIC ONE is decimal and `int()`s to 1, and so
    # does "007". Comparing against the round-trip is what actually makes the
    # mapping injective: exactly one topic string per community id, which is the
    # property the mismatch check downstream is entitled to assume.
    if not (raw_community.isascii() and raw_community.isdecimal()):
        return None
    if str(int(raw_community)) != raw_community:
        return None
    try:
        device_public_id = uuid.UUID(raw_device)
    except ValueError:
        return None
    # A UUID round-trips through several spellings (braces, urn:, no hyphens).
    # Pin the canonical one: the topic a device publishes on is the topic the
    # enrolment response handed it, verbatim, and anything else is a different
    # device as far as the broker ACL is concerned.
    if raw_device != str(device_public_id):
        return None
    return ParsedTopic(
        claimed_community_id=int(raw_community),
        device_public_id=device_public_id,
        kind=kind,
    )


def telemetry_topic(id_community: int, device_public_id: uuid.UUID) -> str:
    """The telemetry topic handed to a device at enrolment (protocol 5.2)."""
    return f"{TOPIC_PREFIX}/{id_community}/{device_public_id}/{TOPIC_TELEMETRY_SUFFIX}"


def status_topic(id_community: int, device_public_id: uuid.UUID) -> str:
    """The status topic handed to a device at enrolment (protocol 5.2).

    Also the topic whose RETAINED message revocation must clear (plan 8.6). A
    retained `status` that outlives its device replays on every worker reconnect,
    so the alert fires on every deploy and the team stops reading it.
    """
    return f"{TOPIC_PREFIX}/{id_community}/{device_public_id}/{TOPIC_STATUS_SUFFIX}"


def device_acl_pattern(suffix: str) -> str:
    """The dynsec ACL topic for the `device` role.

    `%u` substitutes the connecting client's username, which IS the device's
    public id (protocol 1). It exists only from Mosquitto 2.1.0 - on 2.0.x it is
    treated as a literal string and the device can publish NOWHERE, silently:
    the broker starts, the plugin loads, enrolment succeeds, and no telemetry
    ever arrives. That is why the image is pinned by digest in
    docker-compose.dev.yml, and why the pin must never move back to 2.0.x.
    """
    return f"{TOPIC_PREFIX}/+/%u/{suffix}"
