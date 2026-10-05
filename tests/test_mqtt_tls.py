"""`MQTT_TLS` must reach a socket, not just a README.

It shipped as a setting with no Python reader: both clients built
`aiomqtt.Client(...)` with no `tls_params`, while `.env.staging.exemple` and
`.env.production.exemple` carry `MQTT_TLS=true` and all four READMEs document it.

Nothing could have caught that, which is what these tests are for. The suite runs
against a plaintext broker and always will, so the behavioural half below is
necessarily about the PARAMETER rather than the wire - and the static half exists
because the failure this file guards is not a wrong value, it is a construction
site that quietly stops passing the parameter at all.
"""

import ast
import pathlib

import aiomqtt
import pytest

from core.config import settings
from shared.mqtt_tls import tls_params

_ROOT = pathlib.Path(__file__).resolve().parents[1]

# Every place this service opens an MQTT connection. `scripts/simulate_device.py`
# is here on purpose: it stands in for a device, a device always speaks TLS, and
# it is the tool someone reaches for to reproduce a staging problem.
_CLIENT_SITES = (
    "ports/broker_mqtt.py",
    "worker/main.py",
    "scripts/simulate_device.py",
)


class TestTheFlagIsHonoured:
    def test_off_means_none_which_is_a_real_answer(self, monkeypatch):
        # Dev's plaintext 1883 on the internal compose network is deliberate and
        # documented in mosquitto.conf. `None` is the correct value here, not a
        # fallback that happens to work.
        monkeypatch.setattr(settings, "MQTT_TLS", False)
        assert tls_params() is None

    def test_on_produces_parameters_aiomqtt_acts_on(self, monkeypatch):
        monkeypatch.setattr(settings, "MQTT_TLS", True)
        params = tls_params()
        assert isinstance(params, aiomqtt.TLSParameters)

    def test_it_asks_for_the_system_trust_store_and_nothing_bespoke(self, monkeypatch):
        """All fields None, so paho builds `ssl.create_default_context()`.

        Which is the point: system trust store, hostname verification on,
        CERT_REQUIRED. Pinning a CA file or relaxing `cert_reqs` here would be a
        security decision, and this test is what makes it a visible one.
        """
        monkeypatch.setattr(settings, "MQTT_TLS", True)
        params = tls_params()
        assert params.ca_certs is None
        assert params.certfile is None
        assert params.keyfile is None
        assert (
            params.cert_reqs is None
        ), "a non-default cert_reqs is how verification gets turned off by accident"


class TestNoClientCanForgetIt:
    """The static half. A new `aiomqtt.Client(...)` with no `tls_params` is the
    exact regression that shipped once, and it is invisible in dev - where the
    flag is off and a plaintext connection is correct."""

    @pytest.mark.parametrize("rel", _CLIENT_SITES)
    def test_every_client_construction_passes_tls_params(self, rel):
        tree = ast.parse((_ROOT / rel).read_text(encoding="utf-8"))
        calls = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "Client"
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "aiomqtt"
        ]
        assert calls, f"{rel} no longer builds an aiomqtt.Client - update _CLIENT_SITES"
        for call in calls:
            assert any(kw.arg == "tls_params" for kw in call.keywords), (
                f"{rel}:{call.lineno} builds an aiomqtt.Client without tls_params, so "
                f"MQTT_TLS cannot reach it. In dev this is invisible; on the first "
                f"staging deploy it is a handshake failure that names nothing here."
            )

    def test_the_guard_can_fail(self, tmp_path):
        """The negative control. A walker that matched nothing would pass every
        assertion above and prove nothing at all."""
        bad = tmp_path / "bad.py"
        bad.write_text("import aiomqtt\nc = aiomqtt.Client(hostname='h', port=1883)\n")
        tree = ast.parse(bad.read_text())
        calls = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "Client"
        ]
        assert len(calls) == 1
        assert not any(kw.arg == "tls_params" for kw in calls[0].keywords)
