"""
Request limits middleware - body size cap + request timeout.

Protects against oversized payloads and slow-loris / hung-request attacks.

Body size check:
    Reads Content-Length before the request reaches FastAPI's body parser.
    If the declared length exceeds MAX_BODY_BYTES, the request is rejected
    immediately with 413 Payload Too Large, before any body is read into memory.

    This is a cheap up-front gate, not a complete one. A client using chunked
    transfer-encoding sends no Content-Length and so slips past this check; the
    middleware cannot bound such a body without first buffering it, which would
    allocate the very memory the cap exists to prevent. nginx's
    `client_max_body_size` is the coarse gate in front of it.

    NOTE the divergence from the sibling annexes: they carry a second, larger cap
    for multipart upload routes (`_UPLOAD_ROUTE_SUFFIXES`). live-data has no
    upload endpoint and phase 1 adds none - every route takes a small JSON body,
    and the public enrol leg is capped far below this at the nginx location. The
    branch is removed rather than left empty so that nobody reads a dormant
    upload path as an invitation to wire one: the public leg is the platform's
    first unauthenticated endpoint and its body size is part of its attack
    surface.

Request timeout:
    Wraps the entire downstream handler in asyncio.wait_for().
    If the handler does not complete within TIMEOUT_SECONDS,
    the client receives a 504 Gateway Timeout.

    KrakenD cuts every request at 3000 ms (`global.timeout`, with no per-route
    override), so this ceiling is only ever reached on a direct call to :8008.
    It is kept as the backstop for exactly that path.

Usage:
    app.add_middleware(RequestLimitsMiddleware)
"""

import asyncio
import logging

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

# Defined in shared/const.py, not here: this module imports starlette, which the
# worker image does not install, and worker code must still be able to read the
# cap. Re-exported so callers and tests can read it off the middleware that
# applies it.
from shared.const import MAX_BODY_BYTES

logger = logging.getLogger(__name__)

__all__ = ["MAX_BODY_BYTES", "RequestLimitsMiddleware"]

# 30 seconds - covers complex DB queries. Unreachable through the gateway.
TIMEOUT_SECONDS = 30


class RequestLimitsMiddleware(BaseHTTPMiddleware):
    """
    Enforces request body size limits and per-request timeout.
    """

    async def dispatch(self, request: Request, call_next):
        # --- Body size gate ---
        content_length = request.headers.get("content-length")
        if content_length is not None:
            try:
                if int(content_length) > MAX_BODY_BYTES:
                    logger.warning(
                        "Request rejected: body too large",
                        extra={
                            "content_length": content_length,
                            "max_allowed": MAX_BODY_BYTES,
                            "path": request.url.path,
                        },
                    )
                    return JSONResponse(
                        status_code=413,
                        content={
                            "data": "Payload too large",
                            "error_code": 0,
                        },
                    )
            except ValueError:
                pass

        # --- Request timeout ---
        try:
            response = await asyncio.wait_for(
                call_next(request),
                timeout=TIMEOUT_SECONDS,
            )
        except TimeoutError:
            logger.error(
                "Request timed out",
                extra={
                    "timeout_seconds": TIMEOUT_SECONDS,
                    "path": request.url.path,
                    "method": request.method,
                },
            )
            return JSONResponse(
                status_code=504,
                content={
                    "data": "Request timeout",
                    "error_code": 0,
                },
            )

        return response
