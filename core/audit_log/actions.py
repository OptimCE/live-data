"""Audit log action codes.

Action codes follow the ``domain.entity.verb`` convention used by
``crm-backend`` (e.g. ``crm.allocation_key.created``). They are stored as
``VARCHAR(128)`` and the ``AuditAction`` type stays open-ended so call sites
can introduce new codes without round-tripping this module.

----------------------------------------------------------------------------
THIS FILE ARRIVED AS ``administrative-document``'S REGISTRY, VERBATIM.

Fifteen foreign codes - dossiers, document versions, deadline rules - for a
service that has none of those things, under a docstring announcing itself as
"action codes emitted by ``administrative-document``" and claiming to mirror a
journaled state machine live-data does not have. Nothing referenced it, and it
was imported at every boot because the package ``__init__`` re-exports it.

Meanwhile live-data's five real codes were string literals spread across three
service modules. The audit table is SHARED across every annexe, so a typo in one
of those literals does not fail - it files a row under an action nobody queries,
and the row is found years later or not at all.
----------------------------------------------------------------------------
"""

from typing import Final

AuditAction = str


class AuditActions:
    """Every action code ``live-data`` emits.

    ``tests/test_audit_actions.py`` asserts this class and the call sites agree
    in BOTH directions: a code used and not listed, or listed and not used, is
    the drift that makes this file worth having rather than a second place to
    keep the same strings.
    """

    # ---- devices (build steps 4 and 5) ----
    DEVICE_CREATED: Final[AuditAction] = "live_data.device.created"
    # S105 below is suppressed deliberately: this is an audit action code, not a
    # credential. The NAME carries "token" because the action is issuing one; the
    # value is a string that gets written to a log column.
    DEVICE_TOKEN_ISSUED: Final[AuditAction] = "live_data.device.token_issued"  # noqa: S105
    DEVICE_REVOKED: Final[AuditAction] = "live_data.device.revoked"
    # The only one written on an UNAUTHENTICATED request. `api/live_public`
    # resolves the community from the token's device row, never from a header.
    DEVICE_ENROLLED: Final[AuditAction] = "live_data.device.enrolled"

    # ---- visibility (build step 6) ----
    # Audited with the before AND after values: "the manager set k to 3" is not
    # actionable a month later without what it had been.
    SETTINGS_UPDATED: Final[AuditAction] = "live_data.settings.updated"
