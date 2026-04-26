from __future__ import annotations

from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from layer_a_r4.db.base import Base, now_utc_sql


class HandoffOutbox(Base):
    __tablename__ = "handoff_outbox"
    __table_args__ = (
        UniqueConstraint("handoff_id", name="uq_handoff_outbox_handoff_id"),
        CheckConstraint("publish_attempt_count >= 0", name="ck_handoff_outbox_attempt_nonnegative"),
        Index("ix_handoff_outbox_pending", "publish_confirmed_at", "publish_attempted_at"),
    )

    handoff_outbox_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    handoff_id: Mapped[str] = mapped_column(String(64), ForeignKey("handoff.handoff_id", ondelete="CASCADE"), nullable=False)
    payload_ref: Mapped[str] = mapped_column(Text, nullable=False)
    publish_attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    publish_attempted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    publish_confirmed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_publish_error_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=now_utc_sql())

    handoff: Mapped["Handoff"] = relationship(back_populates="outbox")
