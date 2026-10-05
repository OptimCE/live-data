"""In-memory registry of forecast methods. Mirrors `algorithms/registry.py`.

Metadata is registered at import time by each method package's `__init__`
(lightweight, API-safe). Implementation classes are registered by the package's
`method` module, imported only by the worker.
"""

from forecasting.base import ForecastMethod, ForecastMethodMetadata
from shared.const import ProductionChain


class ForecastMethodRegistry:
    def __init__(self) -> None:
        self._metadata: dict[str, ForecastMethodMetadata] = {}
        self._implementations: dict[str, type[ForecastMethod]] = {}

    # ---- registration ----------------------------------------------------

    def register_metadata(self, meta: ForecastMethodMetadata) -> None:
        if meta.name in self._metadata:
            raise ValueError(f"Forecast method '{meta.name}' already registered")
        self._metadata[meta.name] = meta

    def register_implementation(self, cls: type[ForecastMethod]) -> None:
        name = cls.metadata.name
        if name not in self._metadata:
            self._metadata[name] = cls.metadata
        self._implementations[name] = cls

    # ---- lookup ----------------------------------------------------------

    def metadata(self, name: str) -> ForecastMethodMetadata:
        if name not in self._metadata:
            raise KeyError(f"Unknown forecast method: {name}")
        return self._metadata[name]

    def implementation(self, name: str) -> type[ForecastMethod]:
        if name not in self._implementations:
            raise KeyError(f"No implementation loaded for: {name}")
        return self._implementations[name]

    def list_all(self) -> list[ForecastMethodMetadata]:
        return list(self._metadata.values())

    def for_chain(self, chain: ProductionChain | None) -> list[ForecastMethodMetadata]:
        """Every registered method that declares support for `chain`.

        An unknown chain - `None`, which is what `meter_data.production_chain`
        carries for a meter nobody classified - matches NOTHING. Guessing
        photovoltaic because it is the common case would silently forecast a
        hydro installation with a solar model.
        """
        if chain is None:
            return []
        return [meta for meta in self._metadata.values() if chain in meta.supports]

    def required_weather_variables(self) -> set[str]:
        """The union across registered methods.

        What `ports/weather.py`'s future adapter should fetch. Derived rather
        than configured, so adding a method that needs wind speed does not also
        mean editing the fetcher.
        """
        return {
            variable
            for meta in self._metadata.values()
            for variable in meta.required_weather_variables
        }

    def clear(self) -> None:
        """Empty the registry. FOR TESTS ONLY.

        Module-level singletons and autodiscovery do not mix well under pytest:
        without this, a test that registers a dummy method leaks it into every
        later test's view of `list_all()`.
        """
        self._metadata.clear()
        self._implementations.clear()

    def __contains__(self, name: str) -> bool:
        return name in self._metadata


registry = ForecastMethodRegistry()
