"""Mappings of the CRM core tables this service touches.

This service owns none of these tables - they live in the CRM database. It reads
``app_user`` and *inserts* into ``audit_log`` (mapped in ``core/database/models.py``);
it never updates or deletes anything here.

Keep this module minimal: richer CRM reads go through raw SELECTs behind a
Protocol rather than mapping whole tables. Whole-table mappings earn their place
only where rows are constructed.

Deliberately ABSENT, and not to be pasted back without a grant to match:
``community_user`` and ``notification`` are mapped in the sibling annexes because
those services fan notifications out. live-data produces no notifications in
phase 1, so ``live_data_svc`` has no INSERT grant on ``notification`` and no
reason to read the roster. A mapped class with no grant behind it is how a write
ends up swallowed by ``AuditLogService``-style SAVEPOINT handling and returns 200
with the row gone (see postgres/provision/30-crm-grants.sql).
"""

from sqlalchemy import Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from core.database.database import CrmBase


class AppUserModel(CrmBase):
    # Partial mapping of the CRM `app_user` table: the columns the audit log
    # service needs to denormalise the writer's identity onto each row.
    __tablename__ = "app_user"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    auth_user_id: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    email: Mapped[str] = mapped_column(String(256), nullable=False)
    # Preferred language. NULL for every account created before the column
    # existed.
    locale: Mapped[str | None] = mapped_column(String(8), nullable=True)
    first_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_name: Mapped[str | None] = mapped_column(Text, nullable=True)
