import datetime
from typing import Any

from sqlalchemy import TIMESTAMP, BigInteger, Boolean, Integer, String, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from core.database.database import CrmBase
from shared.const import FeatureName


class Community(CrmBase):
    __tablename__ = "community"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, nullable=False)
    name: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        unique=True,
    )
    auth_community_id: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        unique=True,
    )
    created_at: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP,
        nullable=False,
        default=lambda: datetime.datetime.now(datetime.UTC).replace(tzinfo=None),
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP,
        nullable=False,
        default=lambda: datetime.datetime.now(datetime.UTC).replace(tzinfo=None),
        onupdate=lambda: datetime.datetime.now(datetime.UTC).replace(tzinfo=None),
    )


class CommunitySubscription(CrmBase):
    __tablename__ = "community_subscription"
    __table_args__ = (
        UniqueConstraint(
            "id_community",
            "feature",
            name="uq_community_subscription_community_feature",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    id_community: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
    )
    feature: Mapped[FeatureName] = mapped_column(
        String(64),
        nullable=False,
    )
    is_active: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
    )
    created_at: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        default=lambda: datetime.datetime.now(datetime.UTC),
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        default=lambda: datetime.datetime.now(datetime.UTC),
        onupdate=lambda: datetime.datetime.now(datetime.UTC),
    )


class AuditLogModel(CrmBase):
    __tablename__ = "audit_log"
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    id_community: Mapped[int | None] = mapped_column(Integer, nullable=True)
    timestamp: Mapped[datetime.datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    action: Mapped[str] = mapped_column(String(128), nullable=False)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    entity_type: Mapped[str] = mapped_column(String(64), nullable=False)
    entity_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    user_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    user_email: Mapped[str | None] = mapped_column(String(256), nullable=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
