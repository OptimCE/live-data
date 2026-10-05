import logging
import re

from core.security.user_context import ROLE_HIERARCHY, OrgToken, Role

logger = logging.getLogger(__name__)

# Mirrors crm-backend's shared/middlewares/context.ts parser. The gateway injects
# x-user-orgs as Keycloak/Go-formatted blocks, e.g.
#   [orgId:<uuid> orgPath:/<group name> roles:[ADMIN,MANAGER]],map[orgId:… …]
# ``orgPath`` is a Keycloak *group path* derived from group names and may contain
# spaces, so fields are extracted with a non-greedy regex rather than by splitting
# on whitespace (the previous approach mis-aligned columns and raised IndexError).
_ORG_RE = re.compile(r"orgId:(\S+)\s+orgPath:(.*?)\s+roles:\[([^\]]*)\]")


def parse_user_orgs(user_orgs: str) -> list[OrgToken]:
    """Parse the ``x-user-orgs`` header injected by KrakenD/Keycloak."""
    tokens: list[OrgToken] = []
    for match in _ORG_RE.finditer(user_orgs):
        org_id, org_path, roles_raw = match.group(1), match.group(2), match.group(3)
        # Roles may be comma- or space-separated depending on serialization.
        role_list = [r for r in re.split(r"[,\s]+", roles_raw.strip()) if r]
        highest = _resolve_highest_role(role_list)
        if highest is not None:
            tokens.append(OrgToken(org_id=org_id, org_path=org_path, role=highest))
    return tokens


def _resolve_highest_role(roles: list[str]) -> Role | None:
    """Returns the highest-privilege role from a list of role strings."""
    best: Role | None = None
    best_rank = -1

    for r in roles:
        try:
            role = Role(r)
        except ValueError:
            continue
        rank = ROLE_HIERARCHY.get(role, 0)
        if rank > best_rank:
            best_rank = rank
            best = role

    return best
