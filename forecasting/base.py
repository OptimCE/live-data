"""The forecast-method contract. Build step 9 - the SEAM, not a forecast.

Deviation 7: forecasting leaves phase 1 and only its extension point ships. This
file, `registry.py`, `__init__.py` and `ports/weather.py` are the whole of it.
There is no method, and `methods_implemented/` is empty on purpose.

----------------------------------------------------------------------------
THIS MIRRORS `allocation-key-generation/algorithms/`, ALMOST EXACTLY.

plan 10.1: "Copy the algorithm registry, because it already solves this... Mirror
it rather than inventing a variant; a reader who knows one then knows both."

The same three pieces, the same two-stage discovery, the same
`@field_serializer` turning `input_schema` into a JSON Schema so an admin screen
can render a form for a method it has never heard of.

TWO DELIBERATE DIVERGENCES, each because this service is not that one:

1.  NO `queue` FIELD. `AlgorithmMetadata.queue` is the NATS subject a worker
    subscribes to. This service has no NATS - `requirements/base.txt` documents
    its absence at length - and a field naming a transport that does not exist is
    a field someone will eventually try to use.

2.  `supports` AND `required_weather_variables` ARE NEW. The algorithm registry
    has no equivalent, and their absence is the reason a second algorithm there
    means editing the data loader. Here, a method declares which production
    chains it can forecast and which weather variables it needs, so adding a wind
    model is a new package rather than an edit to the photovoltaic one and to
    whatever fetches the weather.
----------------------------------------------------------------------------

`method` AND `method_version` ARE NOT OPTIONAL.

plan 10.3, and `forecast_production`'s primary key already enforces it: without
them two methods cannot be compared, a retro-adjustment cannot be attributed, and
rows produced by a model since corrected cannot be found again.
"""

import datetime
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import ClassVar

from pydantic import BaseModel, ConfigDict, Field, field_serializer

from shared.const import ProductionChain


class ForecastMethodInput(BaseModel):
    """Base for a method's parameter schema. `extra="forbid"`, like the sibling."""

    model_config = ConfigDict(extra="forbid")


@dataclass(frozen=True, slots=True)
class ForecastContext:
    """Everything a method is given. A pure function of this and its inputs.

    Loaded by the caller - the method performs NO I/O, has no session and makes
    no HTTP call. That is what lets a method be unit-tested against a table of
    numbers, and what keeps the weather adapter in one place rather than in every
    method that wants a temperature.
    """

    ean: str
    id_community: int
    production_chain: ProductionChain | None
    # The AC injection ceiling from `device.capacity_kva`. kVA, NEVER kWc - the
    # CRM column is the inverter and grid-connection limit, and a PV array is
    # routinely oversized against it (plan deviation 4). A method that treats this
    # as panel peak will over-forecast every clear noon.
    capacity_kva: float | None
    # The buckets to forecast, UTC hour starts, ascending.
    buckets: tuple[datetime.datetime, ...]
    # Whatever `ports/weather.py` supplied, keyed by variable name. Empty in phase
    # 1, because no adapter exists.
    weather: dict[str, tuple[float, ...]]


class ForecastPoint(BaseModel):
    bucket: datetime.datetime
    wh: float


class ForecastResult(BaseModel):
    """Pure result. The caller persists it into `forecast_production`."""

    points: list[ForecastPoint] = Field(default_factory=list)


class ForecastMethodMetadata(BaseModel):
    """Lightweight, serialisable description. No executable logic, no heavy deps.

    Registered at import time by each method package's `__init__`, so the API can
    list methods and render their forms WITHOUT importing numpy.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str = Field(
        ...,
        description="Unique identifier, used in URLs and stored in forecast_production.method.",
        pattern=r"^[a-z][a-z0-9_]*$",
    )
    description: str = Field(
        ...,
        description=(
            "i18n key (dot notation, e.g. 'FORECAST.CLEARSKY.DESCRIPTION') resolved "
            "to a localised string at the API response boundary."
        ),
    )
    version: str = Field(
        default="1.0",
        description=(
            "Version of the method's input/output contract. Stored as "
            "forecast_production.method_version, which is why it is not optional: "
            "rows produced by a model since corrected have to be findable."
        ),
    )
    input_schema: type[ForecastMethodInput] = Field(
        ..., description="Pydantic model describing the method's parameters."
    )
    supports: list[ProductionChain] = Field(
        ...,
        description=(
            "Which production chains this method can forecast. The registry "
            "resolves a device's chain to a method through this list; a chain with "
            "no method is answered with a NAMED reason, never an error."
        ),
    )
    required_weather_variables: list[str] = Field(
        default_factory=list,
        description=(
            "Variable names this method needs from ports/weather.py. Declared here "
            "so the fetcher can ask for the union of what is registered, rather "
            "than being edited every time a method is added."
        ),
    )
    tags: list[str] = Field(default_factory=list)

    @field_serializer("input_schema")
    def _serialize_input_schema(self, input_schema: type[ForecastMethodInput]) -> dict:
        """Expose the parameter model as a JSON Schema.

        The raw value is a Pydantic model CLASS, which Pydantic cannot serialise.
        The contract wants plain JSON Schema anyway - it is what lets an admin
        screen render a form for a method it does not know about, which is the
        whole point of the registry being data rather than code.
        """
        return input_schema.model_json_schema()


class ForecastMethod[InputT: ForecastMethodInput](ABC):
    """Abstract base for an implementation. Imported by the WORKER only.

    Subclass modules may pull in heavy dependencies; `autodiscover()` without
    `load_implementations` never imports them, which is what keeps the API
    process able to list methods it cannot run.
    """

    metadata: ClassVar[ForecastMethodMetadata]

    @abstractmethod
    async def run(self, inputs: InputT, context: ForecastContext) -> ForecastResult:
        """Execute against validated inputs and pre-loaded context.

        MUST be pure: no database, no network. The caller owns loading and
        persistence, so a method can be tested against a table of numbers.
        """
        ...
