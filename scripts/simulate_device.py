#!/usr/bin/env python
"""Synthetic meters that publish REAL protocol traffic.

A DELIVERABLE, not a fixture. It is what makes plan deviation 6 workable - it
decouples every milestone from the DSO enabling a P1 port, which is the longest
external lead time in the project - and it is the traffic source for the
monorepo's `scripts/verify-live-ingest.sh`.

================================================================================
IT CANNOT BE RUN FROM THE HOST.

The broker publishes NO host port: 1883 is plaintext and stays on the compose
`backend` network, because publishing it would put an authenticated-but-plaintext
broker on the developer's LAN. Plan 14's `python live-data/scripts/simulate_device.py`
therefore does not work, and fails as a connect timeout that reads like a broker
fault.

    docker compose -f docker-compose.dev.yml --env-file .env.dev \
      run --rm --no-deps live-data \
      python scripts/simulate_device.py --profile pv-day --once

If a host-side run is ever genuinely wanted, publish "127.0.0.1:1883:1883" and
never "1883:1883" - loopback satisfies the stated reason while the absolute form
does not.
================================================================================

TWO CREDENTIAL PATHS, and both ship together on purpose.

    --username / --password   the step 3-4 bridge, for a device created by hand
    --enroll-token            calls POST /live-public/enroll and uses the answer

The second is what turns this from a fixture into the plan 14 driver: the
enrolment response shape is then exercised by the same run that proves ingest.
Shipping only the first is exactly how the real path never gets added.
"""

import argparse
import asyncio
import datetime
import json
import math
import secrets
import sys
import urllib.error
import urllib.request

import aiomqtt

# ---- defaults -----------------------------------------------------------

BROKER_HOST = "mosquitto"
BROKER_PORT = 1883
# Inside the compose network the API answers on 8000, unprefixed: KrakenD's
# `url_pattern` strips the service prefix, so the container serves /enroll.
ENROL_URL = "http://live-data:8000/enroll"

INTERVAL_S = 900
QUARTER_HOURS_PER_DAY = 96

PROFILES = ("pv-day", "backlog", "malformed", "oversized", "drift")


def _now() -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC)


def _align(moment: datetime.datetime) -> datetime.datetime:
    """Snap DOWN to a quarter-hour boundary.

    `ts` is the END of an interval (protocol 3.1 rule 1) and an unaligned one is
    rejected, so every generated timestamp goes through here. Getting this wrong
    is the single easiest way to produce a simulator whose traffic is entirely
    rejected for a reason that looks like a server bug.
    """
    epoch = int(moment.timestamp())
    return datetime.datetime.fromtimestamp(epoch - (epoch % INTERVAL_S), tz=datetime.UTC)


def _iso(moment: datetime.datetime) -> str:
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


# ---- the day curve ------------------------------------------------------


def _pv_wh(moment: datetime.datetime, capacity_kva: float, rng: secrets.SystemRandom) -> float:
    """Energy exported over the quarter-hour ending at `moment`.

    A clipped half-sine over daylight, jittered. Not a model of anything - the
    real forecasting work is deviation 7 and deliberately out of phase 1 - but
    shaped so the output is recognisably a PV day rather than a square wave, and
    so it never exceeds the inverter ceiling the server checks against.
    """
    local_hour = moment.hour + moment.minute / 60
    if not 6.0 <= local_hour <= 21.0:
        return 0.0
    fraction = math.sin(math.pi * (local_hour - 6.0) / 15.0)
    peak_wh = capacity_kva * 1000 * (INTERVAL_S / 3600)
    # Jitter is 0.85-1.0 of the curve: cloud, never more than the inverter can
    # physically pass.
    return round(peak_wh * fraction * (0.85 + rng.random() * 0.15), 1)


def _measurement(
    moment: datetime.datetime, *, export_wh: float, pure_injection: bool, import_wh: float = 0.0
) -> dict:
    return {
        "ts": _iso(moment),
        "interval_s": INTERVAL_S,
        "import_wh": import_wh,
        "export_wh": export_wh,
        # protocol 3.4: on a PURE INJECTION site the export IS the production.
        # Otherwise a P1 port cannot see production at all, and null is the
        # honest answer - a connector that guesses here silently understates
        # community production for ever, indistinguishably from a cloudy day.
        "production_wh": export_wh if pure_injection else None,
    }


# ---- the profiles -------------------------------------------------------


def _pv_day(args, rng) -> list[dict]:
    """One quarter-hour, or a whole day with --once omitted."""
    count = 1 if args.once else QUARTER_HOURS_PER_DAY
    end = _align(_now())
    out = []
    for i in reversed(range(count)):
        moment = end - datetime.timedelta(seconds=INTERVAL_S * i)
        out.append(
            _measurement(
                moment,
                export_wh=_pv_wh(moment, args.capacity_kva, rng),
                pure_injection=args.pure_injection,
            )
        )
    return out


def _backlog(args, rng) -> list[dict]:
    """`--hours N` of measurements in ONE array.

    This is what a device that has been offline republishes, and protocol 3.3 is
    explicit that it is NORMAL TRAFFIC rather than an exception. Capped at 200
    per message, which is the protocol's own limit.
    """
    count = min(args.hours * 4, 200)
    # `--anchor-ts` pins the window. Without it the batch ends at the current
    # quarter-hour, which makes "send the same backlog twice" a RACE: if a
    # boundary falls between the two runs the second batch covers a window one
    # slot further on, and the idempotence check sees 25 distinct timestamps
    # where it expected 24 - a failure that looks exactly like a broken upsert.
    end = _align(args.anchor_ts or _now())
    return [
        _measurement(
            end - datetime.timedelta(seconds=INTERVAL_S * i),
            export_wh=_pv_wh(
                end - datetime.timedelta(seconds=INTERVAL_S * i), args.capacity_kva, rng
            ),
            pure_injection=args.pure_injection,
        )
        for i in reversed(range(count))
    ]


def _drift(args, rng) -> list[dict]:
    """One reading just inside the acceptance window, one just outside.

    The inside one must be STORED and the outside one rejected as `ts_too_old` -
    and because that reason is measurement-scoped, both arrive in the same
    message and the good one survives. That asymmetry is the thing to see.
    """
    end = _align(_now())
    inside = end - datetime.timedelta(days=30)
    outside = end - datetime.timedelta(days=40)
    return [
        _measurement(outside, export_wh=100.0, pure_injection=args.pure_injection),
        _measurement(inside, export_wh=200.0, pure_injection=args.pure_injection),
    ]


def _oversized(args, rng) -> list[dict]:
    """A message over 64 KB.

    The broker enforces `max_packet_size` by DISCONNECTING, not by rejecting. So
    a connector that retries the identical batch never recovers, and the symptom
    reads as "the device flaps" rather than as "message too big" - which is why
    the connector has its own cap on measurements per message.

    200 measurements alone are not enough bytes, so each carries a large unknown
    field. That is legal under protocol 7 (unknown keys inside a measurement are
    ignored and counted), which keeps the test about SIZE rather than validity.
    """
    end = _align(_now())
    padding = "x" * 400
    return [
        {
            **_measurement(
                end - datetime.timedelta(seconds=INTERVAL_S * i),
                export_wh=100.0,
                pure_injection=args.pure_injection,
            ),
            "vendor_padding": padding,
        }
        for i in reversed(range(200))
    ]


# ---- publishing ---------------------------------------------------------


async def _publish(client: aiomqtt.Client, topic: str, payload: str, *, qos: int = 1) -> None:
    """Publish and report the size.

    NO ASSERTION on the result, ever. A denied publish is still PUBACKed
    (protocol 2), so a return value here proves nothing at all - absence of the
    data at the server is the only reliable observable.
    """
    await client.publish(topic, payload, qos=qos)
    print(f"  -> {topic} ({len(payload)} bytes, qos {qos})")


async def _publish_malformed(client: aiomqtt.Client, args) -> None:
    """Every rejection reason a device can actually produce, one message each.

    Deliberately one per message rather than one batch: the scope split means
    some of these discard the whole message and some discard one reading, and
    mixing them would make the counters unreadable.
    """
    end = _align(_now())
    good = _measurement(end, export_wh=100.0, pure_injection=args.pure_injection)

    cases: list[tuple[str, str, str]] = [
        (
            "unknown_field (envelope) - message-scoped",
            args.telemetry_topic,
            json.dumps({"v": 1, "surprise": True, "measurements": [good]}),
        ),
        (
            "schema_invalid - malformed JSON",
            args.telemetry_topic,
            '{"v":1,"measurements":[',
        ),
        (
            "duplicate_ts_in_batch - message-scoped",
            args.telemetry_topic,
            json.dumps({"v": 1, "measurements": [good, dict(good)]}),
        ),
        (
            "ts_in_future - measurement-scoped, the batch survives",
            args.telemetry_topic,
            json.dumps(
                {
                    "v": 1,
                    "measurements": [
                        good,
                        _measurement(
                            end + datetime.timedelta(days=2),
                            export_wh=50.0,
                            pure_injection=args.pure_injection,
                        ),
                    ],
                }
            ),
        ),
        (
            "ts_not_aligned - measurement-scoped",
            args.telemetry_topic,
            json.dumps(
                {
                    "v": 1,
                    "measurements": [
                        _measurement(
                            end + datetime.timedelta(seconds=61),
                            export_wh=50.0,
                            pure_injection=args.pure_injection,
                        )
                    ],
                }
            ),
        ),
        (
            "negative_energy - measurement-scoped",
            args.telemetry_topic,
            json.dumps(
                {
                    "v": 1,
                    "measurements": [{**good, "export_wh": -5.0}],
                }
            ),
        ),
        (
            # THE LOAD-BEARING ONE. The broker's ACL is `ce/+/%u/telemetry`, and
            # the `+` permits ANY community id - Phase 0 watched this arrive. So
            # the worker's check is a real access control, not belt-and-braces.
            "community_mismatch - the broker WILL accept this publish",
            args.foreign_topic,
            json.dumps({"v": 1, "measurements": [good]}),
        ),
    ]

    for label, topic, payload in cases:
        print(f"  {label}")
        await _publish(client, topic, payload)
        await asyncio.sleep(0.2)


# ---- enrolment ----------------------------------------------------------


def _enrol(token: str, url: str, connector: str, version: str) -> dict:
    """Exchange a token for credentials. protocol 5.1.

    Plain urllib: `httpx` is deliberately NOT a phase-1 dependency (nothing else
    makes an outbound HTTP call), and adding one for a simulator would put it in
    the worker image for no benefit.
    """
    body = json.dumps(
        {"token": token, "connector": {"name": connector, "version": version}}
    ).encode()
    request = urllib.request.Request(  # noqa: S310 - a fixed http:// URL on the compose network
        url, data=body, headers={"Content-Type": "application/json"}, method="POST"
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:  # noqa: S310
            payload: dict = json.loads(response.read())
            enrolled: dict = payload["data"]
            return enrolled
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        print(f"enrolment refused ({exc.code}): {detail}", file=sys.stderr)
        raise SystemExit(2) from exc


# ---- the run ------------------------------------------------------------


async def _run(args) -> int:
    rng = secrets.SystemRandom()

    will = aiomqtt.Will(
        topic=args.status_topic,
        # The Last Will is composed NOW and delivered when the connection dies.
        # Its `ts` is therefore older than every online status that follows it,
        # which is exactly why device_last guards liveness on the SERVER's
        # receipt clock rather than on this timestamp.
        payload=json.dumps({"v": 1, "online": False, "ts": _iso(_now())}),
        qos=0,
        retain=True,
    )

    client = aiomqtt.Client(
        hostname=args.host,
        port=args.port,
        username=args.username,
        password=args.password,
        tls_params=aiomqtt.TLSParameters() if args.tls else None,
        # The broker PINS this: a client created with `clientid` set cannot
        # connect with any other id. Using the username is what the enrolment
        # response implies and what protocol 4.3 requires.
        identifier=args.username,
        protocol=aiomqtt.ProtocolVersion.V311,
        # Devices connect with a CLEAN session (protocol 4.3): the server leaves
        # sessions to live for ever by design, so a fleet holding persistent
        # sessions would grow broker state without bound. Nothing is queued FOR
        # a device - it may not subscribe at all.
        clean_session=True,
        keepalive=60,
        will=will,
    )

    async with client:
        print(f"connected as {args.username}")
        await _publish(
            client,
            args.status_topic,
            json.dumps(
                {
                    "v": 1,
                    "online": True,
                    "connector": args.connector,
                    "version": args.connector_version,
                    "ts": _iso(_now()),
                    "diag": {"code": "ok", "since": _iso(_now())},
                }
            ),
            qos=0,
        )

        if args.profile == "malformed":
            await _publish_malformed(client, args)
        else:
            builder = {
                "pv-day": _pv_day,
                "backlog": _backlog,
                "drift": _drift,
                "oversized": _oversized,
            }[args.profile]
            measurements = builder(args, rng)
            payload = json.dumps({"v": 1, "measurements": measurements})
            if args.profile == "oversized":
                print(f"  publishing {len(payload)} bytes - the broker will DISCONNECT us")
            print(f"  {len(measurements)} measurement(s)")
            await _publish(client, args.telemetry_topic, payload)

        if args.keep_alive:
            print("holding the connection open (Ctrl-C to stop) ...")
            # An Event rather than a sleep loop: Ctrl-C unblocks immediately
            # instead of after up to five seconds, which matters because the
            # revoke test watches this process for the disconnect and then for
            # the refused reconnect.
            try:
                await asyncio.Event().wait()
            except (asyncio.CancelledError, KeyboardInterrupt):
                return 0
        # A beat, so a QoS-1 publish is on the wire before the context manager
        # tears the connection down.
        await asyncio.sleep(0.5)
    return 0


def _parse_args(argv: list[str] | None = None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default=BROKER_HOST)
    parser.add_argument("--port", type=int, default=BROKER_PORT)
    # A real device always speaks TLS (protocol 4.3); the dev broker's 1883
    # listener deliberately does not. Without this flag the simulator can only
    # ever reach dev - and the moment someone points it at staging to reproduce
    # a field problem, it fails at the handshake with an error naming nothing.
    parser.add_argument(
        "--tls", action="store_true", help="dial over TLS, as a real device does (port 8883)"
    )

    credentials = parser.add_argument_group("credentials (one of the two paths)")
    credentials.add_argument("--username", help="the device_id, for a hand-made device")
    credentials.add_argument("--password")
    credentials.add_argument(
        "--enroll-token",
        help="exchange this token for credentials through POST /enroll first",
    )
    credentials.add_argument("--enroll-url", default=ENROL_URL)

    parser.add_argument("--profile", choices=PROFILES, default="pv-day")
    parser.add_argument("--once", action="store_true", help="one measurement, not a whole day")
    parser.add_argument("--hours", type=int, default=6, help="backlog length")
    parser.add_argument(
        "--anchor-ts",
        type=datetime.datetime.fromisoformat,
        default=None,
        help=(
            "end the backlog at this instant instead of now, snapped down to a "
            "quarter-hour. Two runs with the same anchor send the IDENTICAL batch, "
            "which is what makes re-sending testable"
        ),
    )
    parser.add_argument("--capacity-kva", type=float, default=3.0)
    parser.add_argument("--pure-injection", action="store_true", default=True)
    parser.add_argument(
        "--no-pure-injection",
        dest="pure_injection",
        action="store_false",
        help="production_wh becomes null, as a P1 on a consuming site must report",
    )
    parser.add_argument("--keep-alive", action="store_true", help="hold the connection open")
    parser.add_argument("--connector", default="optimce-connector")
    parser.add_argument("--connector-version", default="0.3.1")
    parser.add_argument("--community", type=int, help="override the topic's community id")

    args = parser.parse_args(argv)

    if args.enroll_token:
        print(f"enrolling with {args.enroll_token} ...")
        enrolled = _enrol(
            args.enroll_token, args.enroll_url, args.connector, args.connector_version
        )
        args.username = enrolled["credentials"]["username"]
        args.password = enrolled["credentials"]["password"]
        args.telemetry_topic = enrolled["topics"]["telemetry"]
        args.status_topic = enrolled["topics"]["status"]
        # NOTE the host is deliberately NOT taken from the response. A real
        # device uses `broker.host` and must; the simulator runs INSIDE the
        # compose network, where that public name does not resolve. That gap is
        # the point of the two settings - see core/config.py.
        print(f"enrolled as {args.username}")
    else:
        if not (args.username and args.password):
            parser.error("pass --enroll-token, or both --username and --password")
        if args.community is None:
            parser.error("--community is required with --username/--password")
        args.telemetry_topic = f"ce/{args.community}/{args.username}/telemetry"
        args.status_topic = f"ce/{args.community}/{args.username}/status"

    community = args.community
    if community is None:
        community = int(args.telemetry_topic.split("/")[1])
    # The topic the `community_mismatch` case publishes on. The broker's `+`
    # permits it; the worker is what refuses it.
    args.foreign_topic = f"ce/{community + 9000}/{args.username}/telemetry"
    return args


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    return asyncio.run(_run(args))


if __name__ == "__main__":
    if sys.platform == "win32":
        # paho's asyncio integration uses add_reader/add_writer, which the
        # ProactorEventLoop does not implement. Without this every connection
        # dies with a bare "Operation timed out" that reads as a broker problem
        # and is not. Confined to __main__ so importing this module never
        # mutates a caller's loop policy.
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    raise SystemExit(main())
