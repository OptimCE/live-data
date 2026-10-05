"""Unit tests for the ``x-user-orgs`` parser.

The gateway injects ``x-user-orgs`` as Keycloak/Go-formatted blocks. ``orgPath``
is derived from group names and may contain spaces, which the previous
whitespace-split parser could not handle (it raised IndexError and logged
"Failed to parse org token"). These cases lock in the regex-based parser, which
mirrors crm-backend's ``shared/middlewares/context.ts``.
"""

from core.security.gateway_auth import parse_user_orgs
from core.security.user_context import Role


def _multi_org_blob(orgs: list[tuple[str, str]]) -> str:
    """Build the ``[orgId:.. orgPath:.. roles:[..]],map[..]`` header KrakenD sends."""
    return ",map".join(f"[orgId:{oid} orgPath:/{oid} roles:[{role}]]" for oid, role in orgs)


def test_parse_single_org():
    tokens = parse_user_orgs("[orgId:abc orgPath:/x roles:[ADMIN]]")
    assert len(tokens) == 1
    assert tokens[0].org_id == "abc"
    assert tokens[0].role == Role.ADMIN


def test_parse_multi_org_picks_highest_role_per_org():
    tokens = parse_user_orgs(_multi_org_blob([("abc", "MEMBER,MANAGER"), ("def", "MEMBER")]))
    by_id = {t.org_id: t.role for t in tokens}
    assert by_id["abc"] == Role.MANAGER  # highest of MEMBER, MANAGER
    assert by_id["def"] == Role.MEMBER


def test_parse_org_path_with_spaces():
    # Keycloak group names (and thus orgPath) routinely contain spaces.
    tokens = parse_user_orgs("[orgId:abc orgPath:/Mon Comité roles:[ADMIN]]")
    assert len(tokens) == 1
    assert tokens[0].org_id == "abc"
    assert tokens[0].org_path == "/Mon Comité"
    assert tokens[0].role == Role.ADMIN


def test_parse_multi_org_with_spaces_in_paths():
    blob = (
        "[orgId:2c8a orgPath:/Mon Comité roles:[ADMIN]],"
        "map[orgId:100 orgPath:/Autre CE roles:[MEMBER]]"
    )
    by_id = {t.org_id: t.role for t in parse_user_orgs(blob)}
    assert by_id["2c8a"] == Role.ADMIN
    assert by_id["100"] == Role.MEMBER


def test_parse_space_separated_roles():
    tokens = parse_user_orgs("[orgId:abc orgPath:/x roles:[MEMBER MANAGER]]")
    assert len(tokens) == 1
    assert tokens[0].role == Role.MANAGER
