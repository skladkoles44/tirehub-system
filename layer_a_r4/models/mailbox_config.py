from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Index, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from layer_a_r4.db.base import Base, now_utc_sql


class MailboxConfig(Base):
    __tablename__ = "mailbox_config"
    __table_args__ = (
        Index("ix_mailbox_config_is_active", "is_active"),
    )

    mailbox_name: Mapped[str] = mapped_column(Text, primary_key=True)
    display_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="1")
    match_policy_version: Mapped[str | None] = mapped_column(Text, nullable=True)
    storage_policy_version: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=now_utc_sql())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=now_utc_sql())

    email_events: Mapped[list["EmailEvent"]] = relationship(back_populates="mailbox")
