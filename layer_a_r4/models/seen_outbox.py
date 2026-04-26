from __future__ import annotations

from datetime import datetime

from sqlalchemy import BigInteger, CheckConstraint, DateTime, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from layer_a_r4.db.base import Base, now_utc_sql


class SeenOutbox(Base):
    __tablename__ = "seen_outbox"
    __table_args__ = (
        UniqueConstraint("event_id", name="uq_seen_outbox_event_id"),
        CheckConstraint("attempt_count >= 0", name="ck_seen_outbox_attempt_nonnegative"),
        Index("ix_seen_outbox_pending", "uidvalidity", "confirmed_at", "attempted_at"),
    )

    seen_task_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    event_id: Mapped[str] = mapped_column(String(64), ForeignKey("email_event.event_id", ondelete="CASCADE"), nullable=False)
    mailbox_name: Mapped[str] = mapped_column(Text, nullable=False)
    uidvalidity: Mapped[int] = mapped_column(BigInteger, nullable=False)
    uid: Mapped[int] = mapped_column(BigInteger, nullable=False)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    attempted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    confirmed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=now_utc_sql())

    email_event: Mapped["EmailEvent"] = relationship(back_populates="seen_outbox")
