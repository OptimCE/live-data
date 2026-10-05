from dataclasses import dataclass
from enum import StrEnum


class Role(StrEnum):
    MEMBER = "MEMBER"
    MANAGER = "MANAGER"
    ADMIN = "ADMIN"


# Higher number = higher privilege
ROLE_HIERARCHY: dict[Role, int] = {
    Role.MEMBER: 1,
    Role.MANAGER: 2,
    Role.ADMIN: 3,
}


@dataclass
class OrgToken:
    org_id: str
    org_path: str
    role: Role


@dataclass
class UserContext:
    user_id: str
    community_id: str | None = None
    role: Role | None = None
    source_ip: str | None = None
