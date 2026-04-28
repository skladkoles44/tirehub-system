from __future__ import annotations

from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from layer_a_r4.db.base import Base, enum_check
from layer_a_r4.models.enums import PROCESSING_STATES


class EventTransitionLog(Base):
    __tablename__ = "event_transition_log"
    __table_args__ = (
        UniqueConstraint("event_id", "transition_seq", name="uq_event_transition_log_seq"),
        enum_check("ck_transition_from_state", "from_state", PROCESSING_STATES, nullable=True),
        enum_check("ck_transition_to_state", "to_state", PROCESSING_STATES),
        CheckConstraint("duration_ms IS NULL OR duration_ms >= 0", name="ck_event_transition_log_duration_nonnegative"),
        Index("ix_event_transition_log_event_id", "event_id", "transition_seq"),
    )

    transition_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    event_id: Mapped[str] = mapped_column(String(64), ForeignKey("email_event.event_id", ondelete="CASCADE"), nullable=False)
    transition_seq: Mapped[int] = mapped_column(Integer, nullable=False)
    ts_utc: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    from_state: Mapped[str | None] = mapped_column(Text, nullable=True)
    to_state: Mapped[str] = mapped_column(Text, nullable=False)
    actor: Mapped[str] = mapped_column(Text, nullable=False)
    reason_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    detail_ref: Mapped[str | None] = mapped_column(Text, nullable=True)
    duration_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)

    email_event: Mapped["EmailEvent"] = relationship(back_populates="transition_logs")
