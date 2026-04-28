from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from layer_a_r4.db.base import Base, enum_check, now_utc_sql
from layer_a_r4.models.enums import PROCESSING_ERROR_STAGES


class ProcessingErrorLog(Base):
    __tablename__ = "processing_error_log"
    __table_args__ = (
        enum_check("ck_processing_error_log_stage", "stage", PROCESSING_ERROR_STAGES),
        Index("ix_processing_error_log_event_id", "event_id", "created_at"),
        Index("ix_processing_error_log_stage", "stage"),
    )

    error_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    event_id: Mapped[str] = mapped_column(String(64), ForeignKey("email_event.event_id", ondelete="CASCADE"), nullable=False)
    stage: Mapped[str] = mapped_column(Text, nullable=False)
    error_class: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    message: Mapped[str | None] = mapped_column(Text, nullable=True)
    traceback: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_retryable: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="0")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=now_utc_sql())

    email_event: Mapped["EmailEvent"] = relationship(back_populates="processing_errors")
