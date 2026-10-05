import logging

from fastapi import Request
from fastapi.responses import JSONResponse

from core.context_vars import current_locale, current_request_id
from core.errors.errors import ErrorException
from core.i18n import translate
from core.tracing import add_exception

logger = logging.getLogger(__name__)


async def error_exception_handler(request: Request, exc: ErrorException) -> JSONResponse:
    """
    Handles all ErrorException instances raised anywhere in the request lifecycle.
    Resolves the translation key using the current request's locales ContextVar.
    Returns the translated message in data, the domain code in error_code.
    """
    add_exception(exc)  # marks span as ERROR
    locale = current_locale.get()
    message = translate(exc.error.key, locale=locale)
    # request_id / user_id / community_id / user_role are injected onto every
    # record by RequestIdFilter (core/logging.py) so we don't repeat them here.
    logger.warning(
        "Domain error",
        extra={
            "error_code": exc.error.code,
            "error_key": exc.error.key,
            "path": request.url.path,
        },
    )

    response = JSONResponse(
        status_code=exc.status_code,
        content={
            "data": message,
            "error_code": exc.error.code,
        },
    )
    request_id = current_request_id.get()
    if request_id:
        response.headers["X-Request-ID"] = request_id
    return response


async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """
    Catch-all for any unhandled exception.
    Returns a generic 500 without leaking internals.
    """
    logger.error(
        "Unhandled exception",
        exc_info=exc,
        extra={"path": request.url.path},
    )

    locale = current_locale.get()
    # This read `translate(...) if False else "Erreur interne du serveur"`, copied
    # from the sibling template: the translate() call was unreachable, `locale`
    # was read and thrown away, and ERRORS.INTERNAL existed in no locale file -
    # so every unhandled 500 answered an English, Dutch or German client in
    # French. Neither ruff nor mypy flags a dead branch that still references its
    # variable, and no test covers the catch-all's TEXT.
    message = translate("ERRORS.INTERNAL", locale=locale)

    response = JSONResponse(
        status_code=500,
        content={
            "data": message,
            "error_code": 0,
        },
    )
    request_id = current_request_id.get()
    if request_id:
        response.headers["X-Request-ID"] = request_id
    return response
