"""Forecast method packages. EMPTY IN PHASE 1, and deliberately so.

Deviation 7: "forecasting leaves phase 1; only its extension point ships". A stub
method here would be worse than none - `GET /forecast` would return numbers
nobody had validated, on a screen that says "indicative" and is believed anyway.

A method is a PACKAGE with two modules:

    methods_implemented/clearsky/
        __init__.py   registers ForecastMethodMetadata (no heavy imports)
        method.py     registers the ForecastMethod subclass (numpy, pvlib, ...)

The split is what lets the API list a method and render its parameter form
without being able to run it.
"""
