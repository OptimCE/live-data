from typing import Any

from pydantic import BaseModel, Field

from core.audit_log.actions import AuditAction


class AuditLogInput(BaseModel):
    action: AuditAction = Field(..., description="Action code, e.g. domain.entity.verb.")
    entity_type: str = Field(..., description="Domain entity being acted on.")
    entity_id: str | None = Field(
        default=None,
        description="Primary key of the entity, stringified.",
    )
    payload: dict[str, Any] = Field(
        default_factory=dict,
        description="Contextual data for the action.",
    )
    source: str | None = Field(
        default=None,
        description="Overrides the default source value when set.",
    )
