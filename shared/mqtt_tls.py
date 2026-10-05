"""One place that turns `MQTT_TLS` into something `aiomqtt` acts on.

---------------------------------------------------------------------------
WHY THIS MODULE EXISTS AT ALL.

`MQTT_TLS` shipped as a setting with **no Python reader**. Both clients -
`ports/broker_mqtt.py` (the dynsec control connection) and `worker/main.py`
(the ingest subscriber) - constructed `aiomqtt.Client(...)` with no
`tls_params`, while `.env.staging.exemple` and `.env.production.exemple` both
carry `MQTT_TLS=true` and all four READMEs document it as a live knob.

That combination has exactly one outcome on a first staging deploy, and it is
not the reassuring one: the templates pair `MQTT_TLS=true` with
`MQTT_PORT=8883`, and `mosquitto/config/mosquitto.conf` terminates TLS on 8883
with a certificate. A plaintext CONNECT into a TLS listener is refused at the
handshake, so **neither the API nor the ingest worker can connect at all** -
enrolment fails and no telemetry is stored, with a transport-level error that
names nothing in this repository.

The other reading is worse rather than better: against a broker that happens to
expose plaintext on that port, the flag says TLS, the wire is not, and every
device credential crosses it in the clear with nothing raising.

A knob that is documented, shipped set to `true`, and connected to nothing is
the failure mode this module removes. There is deliberately no CA / client-cert
configuration here: the platform's brokers present a publicly-rooted
certificate, `ssl.create_default_context()` is what validates it, and inventing
settings for a case nobody has is how the next dead knob gets written.
---------------------------------------------------------------------------
"""

import aiomqtt

from core.config import settings


def tls_params() -> aiomqtt.TLSParameters | None:
    """`None` when `MQTT_TLS` is off - which is what dev wants and means.

    Dev's plaintext 1883 on the internal compose network is deliberate and is
    documented in `mosquitto/config/mosquitto.conf`; the listener is not
    published to the host. So `None` here is a real answer, not a fallback.

    With all fields left at `None`, paho builds the context from
    `ssl.create_default_context()`: system trust store, hostname verification
    on, `CERT_REQUIRED`. Every one of those is the behaviour we want, and every
    one of them is something an explicit setting could get wrong.
    """
    if not settings.MQTT_TLS:
        return None
    return aiomqtt.TLSParameters()
