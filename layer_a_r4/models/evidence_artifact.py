from __future__ import annotations

from datetime import datetime

from sqlalchemy import BigInteger, CheckConstraint, DateTime, ForeignKey, Index, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from layer_a_r4.db.base import Base, enum_check, now_utc_sql
from layer_a_r4.models.enums import ARTIFACT_KINDS, ARTIFACT_ROLES, ARTIFACT_STATUSES


class EvidenceArtifact(Base):
    __tablename__ = "evidence_artifact"
    __table_args__ = (
        enum_check("ck_evidence_artifact_kind", "artifact_kind", ARTIFACT_KINDS),
        enum_check("ck_evidence_artifact_role", "artifact_role", ARTIFACT_ROLES),
        enum_check("ck_evidence_artifact_status", "artifact_status", ARTIFACT_STATUSES),
        CheckConstraint(
            "(artifact_status <> 'PRESENT') OR artifact_path IS NOT NULL",
            name="ck_evidence_artifact_present_requires_path",
        ),
        CheckConstraint(
            "(artifact_status <> 'EXPIRED') OR expired_at IS NOT NULL",
            name="ck_evidence_artifact_expired_requires_time",
        ),
        CheckConstraint(
            "(artifact_status <> 'NOT_SAVED') OR artifact_path IS NULL",
            name="ck_evidence_artifact_not_saved_no_path",
        ),
        Index("ix_evidence_artifact_event_id", "event_id"),
        Index("ix_evidence_artifact_kind", "artifact_kind"),
        Index("ix_evidence_artifact_status", "artifact_status"),
    )

    artifact_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    event_id: Mapped[str] = mapped_column(String(64), ForeignKey("email_event.event_id", ondelete="CASCADE"), nullable=False)
    artifact_kind: Mapped[str] = mapped_column(Text, nullable=False)
    artifact_role: Mapped[str] = mapped_column(Text, nullable=False)
    artifact_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    artifact_status: Mapped[str] = mapped_column(Text, nullable=False)
    sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    size_bytes: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    storage_policy_version: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=now_utc_sql())
    expired_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    email_event: Mapped["EmailEvent"] = relationship(back_populates="evidence_artifacts")
