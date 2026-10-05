from core.errors.errors import Error


# ---------------------------------------------------------------------------
# Auth (no domain code - use xxx)
# ---------------------------------------------------------------------------
class _AuthErrors:
    UNAUTHORIZED = Error(code=1, key="ERRORS.AUTH.UNAUTHORIZED")
    FORBIDDEN = Error(code=2, key="ERRORS.AUTH.FORBIDDEN")
    RATE_LIMITED = Error(code=3, key="ERRORS.AUTH.RATE_LIMITED")
    AUTHORIZATION_MISSING = Error(code=4, key="ERRORS.AUTH.AUTHORIZATION_MISSING")


class _SubscriptionErrors:
    NOT_SUBSCRIBED = Error(code=1003, key="ERRORS.SUBSCRIPTION.NOT_SUBSCRIBED")


class _LiveErrors:
    """Live-data domain errors (2400-2449 block).

    The block was checked, not remembered. 2300-2312 is a REAL collision that
    already shipped - `news-board` and `administrative-document` both occupy it -
    and it is latent only because the frontend maps codes per feature rather than
    through a global registry. The highest 2xxx anywhere in the monorepo is 2365.

    Every key here needs an entry under ERRORS.LIVE in all four of
    locales/{en,fr,nl,de}.json, with the code appended to the message text.
    tests/test_locales.py re-derives the key set from this registry, so a new
    code surfaces there as a missing translation rather than as a silent gap -
    but only because `errors.live` is named in its domain tuple. If you add a
    third domain class, add it there too.

    `ErrorException`'s status_code DEFAULTS TO 400. Always pass it explicitly.
    """

    # ---- devices (2400-2409) ----
    GET_DEVICES = Error(code=2400, key="ERRORS.LIVE.GET_DEVICES")
    GET_DEVICE = Error(code=2401, key="ERRORS.LIVE.GET_DEVICE")
    DEVICE_NOT_FOUND = Error(code=2402, key="ERRORS.LIVE.DEVICE_NOT_FOUND")
    CREATE_DEVICE = Error(code=2403, key="ERRORS.LIVE.CREATE_DEVICE")
    # The EAN does not resolve to an active meter in the CRM for this community.
    # Validated at creation because `device.ean` is a plain column, never an FK -
    # it lives in another database. A typo'd EAN otherwise produces a device that
    # ingests happily and is attributed to nobody, discovered months later with
    # real data already stored against it. -> 422.
    EAN_NOT_FOUND = Error(code=2404, key="ERRORS.LIVE.EAN_NOT_FOUND")
    # A device already exists for this EAN in this community.
    DUPLICATE_EAN = Error(code=2405, key="ERRORS.LIVE.DUPLICATE_EAN")
    REVOKE_DEVICE = Error(code=2406, key="ERRORS.LIVE.REVOKE_DEVICE")
    # The device is already revoked; its broker client no longer exists.
    DEVICE_ALREADY_REVOKED = Error(code=2407, key="ERRORS.LIVE.DEVICE_ALREADY_REVOKED")

    # ---- enrolment tokens (2410-2419) ----
    ISSUE_TOKEN = Error(code=2410, key="ERRORS.LIVE.ISSUE_TOKEN")
    # ONE opaque answer for expired, consumed and unknown. Distinguishing them
    # turns the public leg into a token oracle, and all three have the same
    # remediation: ask for a new code. -> 400.
    TOKEN_NOT_FOUND = Error(code=2411, key="ERRORS.LIVE.TOKEN_NOT_FOUND")
    # A claim lease is open on this token. A retry inside the lease is refused
    # rather than served, because the public leg has no claimant identity to
    # match against - that is the whole point of blanking the headers. Recovery
    # happens on a re-claim after the lease, which takes the set-password branch.
    # -> 409 with Retry-After.
    ENROLMENT_IN_PROGRESS = Error(code=2412, key="ERRORS.LIVE.ENROLMENT_IN_PROGRESS")
    ENROLMENT_FAILED = Error(code=2413, key="ERRORS.LIVE.ENROLMENT_FAILED")

    # ---- the broker (2420-2429) ----
    # The dynsec control connection is not ready, or a command timed out. A
    # TIMEOUT IS NOT A FAILURE: the command may well have succeeded server-side,
    # which is exactly why the retry path must branch on already-exists rather
    # than assume nothing happened. -> 503.
    BROKER_UNAVAILABLE = Error(code=2420, key="ERRORS.LIVE.BROKER_UNAVAILABLE")
    # dynsec answered, and the answer was an error. Errors come back INSIDE the
    # response payload, not as a transport failure - an adapter that only checks
    # delivery reports success for every rejected command. -> 502.
    BROKER_COMMAND_FAILED = Error(code=2421, key="ERRORS.LIVE.BROKER_COMMAND_FAILED")

    # ---- the community (2430-2439) ----
    # The device's community has no active `live-data` subscription. Distinct
    # from 2411 on purpose: the caller already holds a valid token for a real
    # device, so there is nothing left to leak, and the remediation is different
    # and actionable. `require_feature` CANNOT produce this - it reads
    # `community_subscription` scoped to X-Community-ID, and the public leg has
    # neither a community header nor a user. -> 403.
    COMMUNITY_NOT_SUBSCRIBED = Error(code=2430, key="ERRORS.LIVE.COMMUNITY_NOT_SUBSCRIBED")
    GET_SETTINGS = Error(code=2431, key="ERRORS.LIVE.GET_SETTINGS")

    # ---- the read surface (2432-2439) ----
    GET_SUMMARY = Error(code=2432, key="ERRORS.LIVE.GET_SUMMARY")
    GET_SERIES = Error(code=2433, key="ERRORS.LIVE.GET_SERIES")
    UPDATE_SETTINGS = Error(code=2434, key="ERRORS.LIVE.UPDATE_SETTINGS")
    GET_FORECAST = Error(code=2435, key="ERRORS.LIVE.GET_FORECAST")
    GET_OPS_HEALTH = Error(code=2436, key="ERRORS.LIVE.GET_OPS_HEALTH")

    # ---- visibility and window validation (2440-2449) ----
    # ONE code for BOTH `members_see_aggregate` and `members_see_production`, and
    # the same 403. Distinguishing them would tell a member which switch their
    # administrator turned off, which is itself information about the community's
    # configuration - and the remediation is identical either way: ask the
    # administrator. plan 14 criterion 8 requires the 403 rather than a nulled
    # field, because a balance minus a consumption reconstructs production
    # exactly.
    AGGREGATE_NOT_VISIBLE = Error(code=2440, key="ERRORS.LIVE.AGGREGATE_NOT_VISIBLE")
    # 422, NEVER a silent snap. plan 9.3: free-form bounds are the differencing
    # attack - vary the window until the contributing set is one member. Snapping
    # silently would answer every one of those requests.
    WINDOW_NOT_SNAPPED = Error(code=2441, key="ERRORS.LIVE.WINDOW_NOT_SNAPPED")
    WINDOW_TOO_LARGE = Error(code=2442, key="ERRORS.LIVE.WINDOW_TOO_LARGE")
    UNKNOWN_RESOLUTION = Error(code=2443, key="ERRORS.LIVE.UNKNOWN_RESOLUTION")
    GET_DIAGNOSTICS = Error(code=2444, key="ERRORS.LIVE.GET_DIAGNOSTICS")

    # ---- sharing operations (D-14) ----
    GET_OPERATIONS = Error(code=2445, key="ERRORS.LIVE.GET_OPERATIONS")
    # 404 for an operation that does not exist, belongs to another community, or
    # - on /mine - is not one the caller holds a meter in. ONE answer for all
    # three, never a 403: telling a member that an operation exists but is not
    # theirs is itself a disclosure, the same choice `DEVICE_NOT_FOUND` made.
    OPERATION_NOT_FOUND = Error(code=2446, key="ERRORS.LIVE.OPERATION_NOT_FOUND")


class _Errors:
    auth = _AuthErrors()
    subscription = _SubscriptionErrors()
    live = _LiveErrors()


errors = _Errors()
