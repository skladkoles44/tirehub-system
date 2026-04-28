from __future__ import annotations

from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from layer_a_r4.db.base import Base, enum_check, now_utc_sql
from layer_a_r4.models.enums import CARRIER_STRATEGIES, HANDOFF_ROW_STATUSES


class Handoff(Base):
    __tablename__ = "handoff"
    __table_args__ = (
        UniqueConstraint("email_event_id", name="uq_handoff_email_event_id"),
        enum_check("ck_handoff_carrier_strategy", "carrier_strategy", CARRIER_STRATEGIES),
        enum_check("ck_handoff_status", "handoff_status", HANDOFF_ROW_STATUSES),
        CheckConstraint("carrier_kind IN ('attachment', 'url', 'none')", name="ck_handoff_carrier_kind"),
        CheckConstraint(
            "(carrier_strategy <> 'ATTACHMENT') OR "
            "("
            "carrier_kind = 'attachment' AND "
            "carrier_attachment_index IS NOT NULL AND "
            "carrier_url_index IS NULL AND "
            "carrier_url_ref IS NULL"
            ")",
            name="ck_handoff_attachment_shape",
        ),
        CheckConstraint(
            "(carrier_strategy <> 'BODY_LINK') OR "
            "("
            "carrier_kind = 'url' AND "
            "carrier_url_index IS NOT NULL AND "
            "carrier_attachment_index IS NULL AND "
            "carrier_path IS NULL"
            ")",
            name="ck_handoff_url_shape",
        ),
        CheckConstraint(
            "(carrier_strategy <> 'NONE') OR "
            "("
            "carrier_kind = 'none' AND "
            "carrier_attachment_index IS NULL AND "
            "carrier_url_index IS NULL AND "
            "carrier_path IS NULL AND "
            "carrier_url_ref IS NULL"
            ")",
            name="ck_handoff_none_shape",
        ),
        CheckConstraint(
            "(handoff_status NOT IN ('READY', 'PUBLISHED')) OR supplier_id IS NOT NULL",
            name="ck_handoff_ready_requires_supplier",
        ),
        CheckConstraint(
            "(handoff_status NOT IN ('READY', 'PUBLISHED')) OR handoff_payload_ref IS NOT NULL",
            name="ck_handoff_ready_requires_payload_ref",
        ),
        CheckConstraint(
            "(handoff_status NOT IN ('READY', 'PUBLISHED')) OR "
            "("
            "(carrier_strategy = 'ATTACHMENT' AND carrier_path IS NOT NULL) OR "
            "(carrier_strategy = 'BODY_LINK' AND carrier_url_ref IS NOT NULL)"
            ")",
            name="ck_handoff_ready_requires_materialized_carrier_ref",
        ),
        Index("ix_handoff_status", "handoff_status"),
    )

    handoff_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    email_event_id: Mapped[str] = mapped_column(String(64), ForeignKey("email_event.event_id", ondelete="CASCADE"), nullable=False)
    supplier_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    carrier_strategy: Mapped[str] = mapped_column(Text, nullable=False)
    carrier_kind: Mapped[str] = mapped_column(Text, nullable=False)
    carrier_attachment_index: Mapped[int | None] = mapped_column(Integer, nullable=True)
    carrier_url_index: Mapped[int | None] = mapped_column(Integer, nullable=True)
    carrier_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    carrier_url_ref: Mapped[str | None] = mapped_column(Text, nullable=True)
    evidence_ref: Mapped[str | None] = mapped_column(Text, nullable=True)
    matched_rule_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    handoff_status: Mapped[str] = mapped_column(Text, nullable=False)
    handoff_payload_ref: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=now_utc_sql())

    email_event: Mapped["EmailEvent"] = relationship(back_populates="handoff")
    outbox: Mapped["HandoffOutbox | None"] = relationship(back_populates="handoff", uselist=False, cascade="all, delete-orphan")
