"""The forecast seam. Build step 9.

plan 14 criterion 7: "The forecast SEAM, not a forecast (deviation 7). With no
method registered, both endpoints must answer with their real shape: an empty
list, and an empty series carrying a NAMED reason. A bare [] or a 404 is a
failure - it is indistinguishable from a broken job once methods do exist."

And: "The test suite registers a dummy method in a temp package and asserts
autodiscovery finds it, that metadata loads WITHOUT the implementation, and that
a package which fails to import is skipped and logged rather than taking the
others down with it."
"""

import importlib
import pathlib
import shutil
import sys
import textwrap

import pytest

import forecasting
from forecasting.base import ForecastMethodInput, ForecastMethodMetadata
from forecasting.registry import ForecastMethodRegistry, registry
from shared.const import ProductionChain
from tests.factories.device_factory import create_device
from tests.factories.meter_factory import create_owned_meter


class _DummyInput(ForecastMethodInput):
    tilt_degrees: float = 30.0


def _meta(name: str = "dummy", chains=(ProductionChain.PHOTOVOLTAIC,)) -> ForecastMethodMetadata:
    return ForecastMethodMetadata(
        name=name,
        description=f"FORECAST.{name.upper()}.DESCRIPTION",
        version="1.0",
        input_schema=_DummyInput,
        supports=list(chains),
        required_weather_variables=["irradiance_wm2"],
    )


class TestTheRegistry:
    def test_phase_one_registers_nothing(self):
        """`methods_implemented/` is EMPTY, and that is the deliverable.

        A stub method would be worse than none: `/forecast` would return numbers
        nobody validated, on a screen that says "indicative" and is believed
        anyway.
        """
        forecasting.autodiscover()
        assert registry.list_all() == []

    def test_a_method_can_be_registered_and_found(self):
        local = ForecastMethodRegistry()
        local.register_metadata(_meta())
        assert "dummy" in local
        assert local.metadata("dummy").version == "1.0"

    def test_registering_the_same_name_twice_raises(self):
        """Loud. Two methods under one name means `forecast_production.method`
        no longer identifies which model produced a row - and the whole reason
        that column is mandatory is so a model since corrected can be found."""
        local = ForecastMethodRegistry()
        local.register_metadata(_meta())
        with pytest.raises(ValueError, match="already registered"):
            local.register_metadata(_meta())

    def test_metadata_loads_without_an_implementation(self):
        """The two-stage split, asserted.

        The API process registers metadata only. Asking for the implementation
        must RAISE rather than return something unusable, because the API can
        list a method it has no way to run - that is the point of the split.
        """
        local = ForecastMethodRegistry()
        local.register_metadata(_meta())
        assert local.list_all()
        with pytest.raises(KeyError, match="No implementation loaded"):
            local.implementation("dummy")

    def test_a_chain_with_no_method_matches_nothing(self):
        local = ForecastMethodRegistry()
        local.register_metadata(_meta(chains=(ProductionChain.PHOTOVOLTAIC,)))
        assert local.for_chain(ProductionChain.WIND) == []

    def test_an_unknown_chain_matches_nothing_rather_than_guessing(self):
        """NULL `production_chain` is a meter the CRM never classified. Falling
        back to photovoltaic because it is the common case would forecast a hydro
        installation with a solar model, and nothing would say so."""
        local = ForecastMethodRegistry()
        local.register_metadata(_meta())
        assert local.for_chain(None) == []

    def test_the_weather_variables_are_the_union_of_what_is_registered(self):
        """Derived, not configured. Adding a method that needs wind speed must
        not also mean editing the fetcher."""
        local = ForecastMethodRegistry()
        local.register_metadata(_meta("solar"))
        wind = _meta("wind", chains=(ProductionChain.WIND,))
        wind.required_weather_variables = ["wind_speed_ms"]
        local.register_metadata(wind)
        assert local.required_weather_variables() == {"irradiance_wm2", "wind_speed_ms"}

    def test_the_input_schema_serialises_as_json_schema(self):
        """What lets an admin screen render a form for a method it has never
        heard of - the reason the registry is data rather than a chain of
        imports."""
        dumped = _meta().model_dump(mode="json")
        assert dumped["input_schema"]["properties"]["tilt_degrees"]["type"] == "number"


class TestAutodiscovery:
    """Against the REAL `forecasting/methods_implemented/`, not a stand-in.

    An earlier version of this file pointed `autodiscover` at a tmp_path by
    monkeypatching `forecasting.__file__`. It failed for a reason worth keeping:
    the directory moved but the IMPORT PREFIX did not, so
    `importlib.import_module("forecasting.methods_implemented.sunny")` looked in
    the real package and found nothing.

    Which means a tmp_path version could only ever have passed by not exercising
    the import at all - so the packages are written into the real directory and
    removed afterwards. That is also the only way the "a broken package is
    skipped" case runs the real `except` branch.
    """

    METHODS_DIR = pathlib.Path(forecasting.__file__).parent / "methods_implemented"

    @pytest.fixture
    def written(self):
        created: list[pathlib.Path] = []

        def write(name: str, body: str) -> None:
            pkg = self.METHODS_DIR / name
            pkg.mkdir(exist_ok=True)
            (pkg / "__init__.py").write_text(textwrap.dedent(body), encoding="utf-8")
            created.append(pkg)

        yield write

        for pkg in created:
            shutil.rmtree(pkg, ignore_errors=True)
            sys.modules.pop(f"forecasting.methods_implemented.{pkg.name}", None)
        registry.clear()
        importlib.invalidate_caches()

    GOOD = """
        from forecasting.base import ForecastMethodInput, ForecastMethodMetadata
        from forecasting.registry import registry
        from shared.const import ProductionChain

        class Params(ForecastMethodInput):
            pass

        registry.register_metadata(
            ForecastMethodMetadata(
                name="{name}",
                description="FORECAST.{upper}.DESCRIPTION",
                input_schema=Params,
                supports=[ProductionChain.PHOTOVOLTAIC],
            )
        )
    """

    def test_a_package_that_registers_metadata_is_found(self, written):
        written("sunny", self.GOOD.format(name="sunny", upper="SUNNY"))
        registry.clear()
        importlib.invalidate_caches()
        forecasting.autodiscover()
        assert "sunny" in registry
        assert registry.metadata("sunny").supports == [ProductionChain.PHOTOVOLTAIC]

    def test_metadata_discovery_does_not_import_the_implementation(self, written):
        """The two-stage split, through the real loader.

        `method.py` raises on import. `autodiscover()` without
        `load_implementations` must never touch it - that is what lets the API
        process list a method whose dependencies it does not have.
        """
        written("lazy", self.GOOD.format(name="lazy", upper="LAZY"))
        (self.METHODS_DIR / "lazy" / "method.py").write_text(
            "raise RuntimeError('the implementation must not be imported')\n",
            encoding="utf-8",
        )
        registry.clear()
        importlib.invalidate_caches()
        forecasting.autodiscover()
        assert "lazy" in registry

    def test_a_broken_package_is_skipped_and_does_not_take_the_others_down(self, written, caplog):
        """One method that fails to import must not remove EVERY method from the
        admin screen. That is the difference between a degraded feature and an
        outage, and it is why `autodiscover` catches per package."""
        written("broken", "raise RuntimeError('boom')\n")
        written("fine", self.GOOD.format(name="fine", upper="FINE"))
        registry.clear()
        importlib.invalidate_caches()
        with caplog.at_level("ERROR"):
            forecasting.autodiscover()

        assert "fine" in registry, "a broken sibling must not hide a working method"
        assert "broken" not in registry
        assert any("broken" in record.message for record in caplog.records)


class TestTheEndpoints:
    async def test_forecast_is_empty_with_a_named_reason(
        self, client, community, manager_headers, db_session
    ):
        """plan 14 criterion 7's exact body.

        NOT a 404 and NOT a bare `[]`. A 404 says "this endpoint is not here",
        which a frontend handles by hiding the panel - so the day the first
        method ships, nothing appears and nobody knows why.
        """
        ean = await create_owned_meter(db_session, id_community=community.id, id_member=1)
        await create_device(db_session, id_community=community.id, ean=ean)

        response = await client.get("/forecast", headers=manager_headers)
        assert response.status_code == 200
        data = response.json()["data"]
        assert data["buckets"] == []
        assert data["reason"] == "no_method_for_production_chain"
        assert data["indicative"] is True

    async def test_a_community_with_no_devices_gets_the_same_answer(
        self, client, community, manager_headers
    ):
        """From the caller's side "no method for your chain" and "you have no
        devices" are the same situation with the same remediation, so they are
        the same answer rather than two states a frontend has to distinguish."""
        data = (await client.get("/forecast", headers=manager_headers)).json()["data"]
        assert data["buckets"] == []
        assert data["reason"] == "no_method_for_production_chain"

    async def test_the_methods_list_is_empty_and_is_a_list(
        self, client, community, manager_headers
    ):
        response = await client.get("/forecast/methods", headers=manager_headers)
        assert response.status_code == 200
        assert response.json()["data"] == []

    async def test_a_member_cannot_read_the_community_forecast(
        self, client, community, member_headers
    ):
        """D-14: the forecast is a COMMUNITY figure, and a member sees only their
        own sharing operation(s). Manager-only now, like `/summary`; criterion 8
        is asserted on the member-floor `/mine` reads instead."""
        response = await client.get("/forecast", headers=member_headers)
        assert response.status_code == 403
        assert response.json()["error_code"] == 2

    async def test_the_methods_list_is_manager_only(self, client, community, member_headers):
        """It describes how the PLATFORM is configured, not the community's
        energy."""
        assert (await client.get("/forecast/methods", headers=member_headers)).status_code == 403

    async def test_the_forecast_still_runs_the_visibility_gate(
        self, client, community, manager_headers, db_session
    ):
        """It reuses the summary's gate rather than growing its own - plan 9.1's
        retrofit warning. A manager always passes it (the switches govern what
        MEMBERS see), so with both switches off the manager still gets 200."""
        from sqlalchemy import text

        await db_session.execute(
            text(
                "INSERT INTO community_live_settings (id_community, members_see_production, "
                "members_see_aggregate, k) VALUES (:c, FALSE, FALSE, 5)"
            ),
            {"c": community.id},
        )
        await db_session.flush()
        response = await client.get("/forecast", headers=manager_headers)
        assert response.status_code == 200


class TestTheWeatherPort:
    def test_it_is_a_protocol_with_no_implementation(self):
        """Deviation 7 ships the seam, not an adapter. An adapter written now
        would be written against no method, so nothing would constrain its shape
        - and it would drag `httpx` into the worker image for nothing."""
        import ports.weather as weather

        assert hasattr(weather, "WeatherPort")
        # Types DEFINED here, not imported ones - `Sequence` and `Protocol`
        # are in `vars()` too and are not implementations of anything.
        concrete = [
            name
            for name, value in vars(weather).items()
            if isinstance(value, type)
            and getattr(value, "__module__", None) == weather.__name__
            and name != "WeatherPort"
        ]
        assert concrete == [], f"ports/weather.py must stay a Protocol only, found {concrete}"

    def test_httpx_is_not_a_runtime_dependency(self):
        """It IS in testing.txt - fastapi's ASGITransport needs it - and must
        stay out of base/api/worker until the first forecast method needs it."""
        from pathlib import Path

        for name in ("base.txt", "api.txt", "worker.txt"):
            body = Path("requirements") / name
            lines = [
                line.strip()
                for line in body.read_text(encoding="utf-8").splitlines()
                if line.strip() and not line.strip().startswith("#")
            ]
            assert not any(
                line.lower().startswith("httpx") for line in lines
            ), f"httpx must not be in requirements/{name} until a forecast method needs it"
