"""Export the OpenAPI spec(s) that KrakenD's generator consumes.

Two modes:

    python scripts/export_openapi.py docs/openapi.json
        One file, every non-/health path. Used by .github/workflows/
        update-documentation.yml, which publishes human-readable docs.

    python scripts/export_openapi.py --split /output/live.json /output/live-public.json
        TWO DISJOINT files. Used by the `live-data-doc-gen` compose one-shot,
        which feeds krakend-builder.yaml's two service entries.

================================================================================
WHY TWO FILES, AND WHY THIS SCRIPT ASSERTS RATHER THAN JUST SPLITS

KrakenD gates its JWT validator PER SERVICE ENTRY (`auth: false` in
krakend-builder.yaml), never per endpoint. So a service with one public route
among authenticated ones cannot be expressed as one entry - it needs two, and two
entries need two swagger files. Pointing both entries at the SAME file would
emit every path twice, once under each prefix.

The failure this script exists to prevent is NOT a route collision. That is worth
stating plainly, because it is the intuitive answer and it is wrong:

    The generator builds `endpoint = f"{service_prefix}{path}"` (parser.py:211)
    and derives the prefix from the service KEY (cli.py:94-96). So a path present
    in both specs produces `/live/X` AND `/live-public/X` - two DISTINCT gin
    routes. There is no duplicate, `krakend check -tnc` exits 0, KrakenD boots
    clean, and `docker compose ps` shows nothing wrong.

    What actually happens is that a route meant to be authenticated is PUBLISHED
    UNAUTHENTICATED at `/live-public/X`, carrying no `extra_config` at all - and
    the platform has no existing signal for that, because all 191 pre-existing
    endpoints are authenticated.

And the likelier human error is subtler still: a new route added to
`live_public_routes`, or an authenticated route tagged "Live public". That is
present in exactly ONE spec, so every disjointness check passes cleanly.

Only EQUALITY AGAINST A HARD-CODED LITERAL catches both. `PUBLIC_OPERATIONS`
below is that literal. Widening it is a deliberate, reviewable act; forgetting to
is a failed build rather than a public endpoint.

The second half of the guard lives outside this script, against the generated
`krakend.json`: exactly one endpoint in the whole file may lack an
`extra_config` key. See scripts/verify-krakend-public-surface.py in the monorepo.
================================================================================

WRITE ORDERING. Both targets are unlinked first and every assertion runs before
any write. `krakend/config/` is a gitignored bind mount that is never cleaned, so
a run that wrote live.json and then crashed would leave a STALE live-public.json
behind - and `krakend-config` would merge it while printing "Success!". That is
the same failure shape docker-stack.sh already documents for swagger-doc-gen.
"""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# PUBLIC_OPERATIONS is declared beside the router it describes, not here: the
# tests import the same literal, and a second copy in scripts/ is a second
# thing to forget.
from api.live_public.routes import PUBLIC_OPERATIONS, PUBLIC_TAG
from main import app

_HTTP_METHODS = {"get", "post", "put", "patch", "delete", "options", "head", "trace"}


def _build_spec() -> dict:
    spec = app.openapi()
    # /health never reaches the gateway. Dropping it here is also why plan 13's
    # "step 1 has only /health" could not have worked: it would have produced a
    # zero-path spec and a byte-identical krakend.json.
    spec["paths"] = {
        path: methods
        for path, methods in spec.get("paths", {}).items()
        if not path.startswith("/health")
    }
    return spec


def _operations(paths: dict) -> set[tuple[str, str]]:
    return {
        (path, method)
        for path, item in paths.items()
        for method in item
        if method.lower() in _HTTP_METHODS
    }


def _public_operations(paths: dict) -> set[tuple[str, str]]:
    """Operations carrying the public tag.

    Partitioned on the TAG rather than on a path prefix, because there is no
    prefix to partition on: `url_pattern` strips the service prefix, so this app
    serves `/version` and `/enroll` and the two routers' paths are
    indistinguishable by path alone. The tag is set in exactly one place -
    main.py's include_router - and imported from the router module here, so the
    string cannot drift between the two.
    """
    return {
        (path, method)
        for path, item in paths.items()
        for method, operation in item.items()
        if method.lower() in _HTTP_METHODS and PUBLIC_TAG in (operation.get("tags") or [])
    }


def _subset(spec: dict, wanted: set[tuple[str, str]]) -> dict:
    out: dict = json.loads(json.dumps(spec))
    paths: dict = {}
    for path, item in spec["paths"].items():
        kept = {
            method: operation
            for method, operation in item.items()
            if method.lower() not in _HTTP_METHODS or (path, method.lower()) in wanted
        }
        # Drop a path whose only remaining keys are non-operations (parameters,
        # summary): an empty path object would become a gin route with no method.
        if any(m.lower() in _HTTP_METHODS for m in kept):
            paths[path] = kept
    out["paths"] = paths
    return out


def _fail(message: str) -> None:
    print(f"export_openapi: {message}", file=sys.stderr)
    sys.exit(1)


def export_single(output_path: Path) -> None:
    spec = _build_spec()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(spec, indent=2))
    print(f"Wrote OpenAPI spec ({len(spec['paths'])} paths) to {output_path}")


def export_split(live_path: Path, public_path: Path) -> None:
    spec = _build_spec()
    all_ops = _operations(spec["paths"])
    public_ops = _public_operations(spec["paths"])
    private_ops = all_ops - public_ops

    # ---- assert BEFORE writing anything ----

    if public_ops != set(PUBLIC_OPERATIONS):
        unexpected = sorted(public_ops - set(PUBLIC_OPERATIONS))
        missing = sorted(set(PUBLIC_OPERATIONS) - public_ops)
        _fail(
            "the PUBLIC operation set is not what this service intends.\n"
            f"  unexpectedly public : {unexpected or 'none'}\n"
            f"  expected but absent : {missing or 'none'}\n"
            "An operation tagged "
            f"'{PUBLIC_TAG}' is served with NO auth/validator at all. If that is "
            "genuinely intended, widen PUBLIC_OPERATIONS in api/live_public/routes.py - "
            "deliberately, and in the same change as the nginx header-blanking "
            "location and the rate-limit zone for the new path."
        )

    if not private_ops:
        # Not pedantry: a `live` entry whose swagger has no paths generates no
        # endpoints, so the authenticated half of the gateway config would vanish
        # silently and krakend.json would look fine.
        _fail("the AUTHENTICATED spec has no operations - the `live` service would be empty.")

    overlap = public_ops & private_ops
    if overlap:
        _fail(f"operations claimed by both specs: {sorted(overlap)}")

    live_spec = _subset(spec, private_ops)
    public_spec = _subset(spec, public_ops)

    # ---- only now touch the filesystem ----
    for target in (live_path, public_path):
        target.parent.mkdir(parents=True, exist_ok=True)
        target.unlink(missing_ok=True)

    live_path.write_text(json.dumps(live_spec, indent=2))
    public_path.write_text(json.dumps(public_spec, indent=2))
    print(
        f"Wrote {len(live_spec['paths'])} authenticated path(s) to {live_path} "
        f"and {len(public_spec['paths'])} public path(s) to {public_path}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--split",
        nargs=2,
        metavar=("LIVE_JSON", "LIVE_PUBLIC_JSON"),
        help="Write two disjoint specs instead of one merged file.",
    )
    parser.add_argument(
        "output",
        nargs="?",
        help="Single-file output path (omit when using --split).",
    )
    args = parser.parse_args()

    if args.split:
        if args.output:
            parser.error("pass either --split or a single output path, not both")
        export_split(Path(args.split[0]), Path(args.split[1]))
        return

    if not args.output:
        parser.error("an output path is required unless --split is used")
    export_single(Path(args.output))


if __name__ == "__main__":
    main()
