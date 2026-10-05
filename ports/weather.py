"""The weather port. A PROTOCOL AND NOTHING ELSE. Build step 9.

No adapter, no `httpx`, no base URL, no API key, no retry policy. That is the
deliverable, not an unfinished one.

----------------------------------------------------------------------------
WHY THERE IS NO IMPLEMENTATION HERE.

plan 10.4 and deviation 7: forecasting leaves phase 1 and only its extension
point ships. An adapter written now would be written against no method, so
nothing would constrain its shape, and the first real method would find it wrong
in ways only discoverable by rewriting it.

Worse, it would drag `httpx` into `requirements/worker.txt`. That file's absence
of httpx is documented at length in `base.txt` and is currently TRUE of the whole
service: nothing in live-data makes an outbound HTTP call. Adding the dependency
before anything uses it puts an unused network client in the worker image and
invites the api-direction import mistake for no benefit.

So: the Protocol is here, so that a method's `required_weather_variables` has
something to mean and so the first adapter has a shape to satisfy. When it
arrives, it arrives WITH the method that needs it, and `httpx` arrives in
`worker.txt` at the same moment.
----------------------------------------------------------------------------

THE SHAPE IS VARIABLE-KEYED, NOT FIELD-KEYED, AND THAT IS THE POINT.

`fetch` takes the variable names the CALLER wants - in practice
`registry.required_weather_variables()`, the union across registered methods -
and returns them keyed by name. A Protocol with `temperature_c` and
`irradiance_wm2` as named fields would have to be edited for the first method
that wants wind speed, and so would every fake, every test and the adapter.
"""

import datetime
from collections.abc import Sequence
from typing import Protocol


class WeatherPort(Protocol):
    """Hourly weather for one location over a bucket range."""

    async def fetch(
        self,
        *,
        latitude: float,
        longitude: float,
        variables: Sequence[str],
        buckets: Sequence[datetime.datetime],
    ) -> dict[str, tuple[float, ...]]:
        """Values per variable, aligned to `buckets`, same length and order.

        ALIGNED TO `buckets`, not merely covering them: a forecast method indexes
        the two together, so a provider returning its own grid - or silently
        dropping an hour it has no data for - would shift every value after the
        gap by one hour. The adapter is responsible for resampling and for
        filling, and for saying so when it cannot.

        A variable the provider cannot supply is ABSENT FROM THE RESULT rather
        than present and full of zeros. Zero irradiance is midnight; a missing
        key is a method that must decline to forecast.
        """
        ...
