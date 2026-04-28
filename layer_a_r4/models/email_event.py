from __future__ import annotations

from datetime import datetime

from sqlalchemy import JSON, BigInteger, Boolean, CheckConstraint, DateTime, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from layer_a_r4.db.base import Base, enum_check, now_utc_sql
from layer_a_r4.models.enums import (
    CARRIER_STRATEGIES,
    EMAIL_EVENT_HANDOFF_STATUSES,
    MATCH_STATUSES,
    OUTCOME_STATUSES,
    PROCESSING_STATES,
    SEEN_STATUSES,
)


class EmailEvent(Base):
    __tablename__ = "email_event"
    __table_args__ = (
        UniqueConstraint("mailbox_name", "uidvalidity", "uid", name="uq_email_event_imap_triplet"),
        CheckConstraint("uidvalidity > 0", name="ck_email_event_uidvalidity_positive"),
        CheckConstraint("uid > 0", name="ck_email_event_uid_positive"),
        enum_check("ck_email_event_processing_state", "processing_state", PROCESSING_STATES),
        enum_check("ck_email_event_outcome_status", "outcome_status", OUTCOME_STATUSES, nullable=True),
        enum_check("ck_email_event_match_status", "match_status", MATCH_STATUSES, nullable=True),
        enum_check("ck_email_event_carrier_strategy", "carrier_strategy", CARRIER_STRATEGIES, nullable=True),
        enum_check("ck_email_event_handoff_status", "handoff_status", EMAIL_EVENT_HANDOFF_STATUSES),
        enum_check("ck_email_event_seen_status", "seen_status", SEEN_STATUSES),
        CheckConstraint(
            "((processing_state <> 'FINALIZED') AND outcome_status IS NULL) "
            "OR ((processing_state = 'FINALIZED') AND outcome_status IS NOT NULL)",
            name="ck_email_event_finalized_requires_outcome",
        ),
        CheckConstraint(
            "(match_status <> 'MATCHED') OR (supplier_id IS NOT NULL AND matched_rule_id IS NOT NULL)",
            name="ck_email_event_matched_requires_supplier_rule",
        ),
        CheckConstraint(
            "(match_status <> 'UNMATCHED') OR supplier_id IS NULL",
            name="ck_email_event_unmatched_supplier_null",
        ),
        CheckConstraint(
            "(match_status <> 'AMBIGUOUS') OR (supplier_id IS NULL AND candidate_supplier_ids IS NOT NULL)",
            name="ck_email_event_ambiguous_has_candidates",
        ),
        CheckConstraint(
            "(selected_attachment_index IS NULL) OR carrier_strategy = 'ATTACHMENT'",
            name="ck_email_event_selected_attachment_strategy",
        ),
        CheckConstraint(
            "(selected_url_index IS NULL) OR carrier_strategy = 'BODY_LINK'",
            name="ck_email_event_selected_url_strategy",
        ),
        CheckConstraint(
            "(outcome_status <> 'HANDOFF_READY') OR (carrier_strategy IN ('ATTACHMENT', 'BODY_LINK'))",
            name="ck_email_event_ready_disallows_none_strategy",
        ),
        CheckConstraint(
            "(outcome_status <> 'HANDOFF_READY') OR "
            "(carrier_strategy <> 'ATTACHMENT') OR "
            "(selected_attachment_index IS NOT NULL)",
            name="ck_email_event_ready_attachment_selected",
        ),
        CheckConstraint(
            "(outcome_status <> 'HANDOFF_READY') OR "
            "(carrier_strategy <> 'BODY_LINK') OR "
            "(selected_url_index IS NOT NULL)",
            name="ck_email_event_ready_url_selected",
        ),
        CheckConstraint(
            "(outcome_status <> 'HANDOFF_READY') OR (handoff_status IN ('READY', 'PUBLISHED'))",
            name="ck_email_event_ready_requires_handoff_status",
        ),
        CheckConstraint(
            "(processing_state <> 'FINALIZED') OR "
            "(outcome_status = 'HANDOFF_READY') OR "
            "(handoff_status = 'BLOCKED')",
            name="ck_email_event_reject_requires_blocked_handoff",
        ),
        CheckConstraint(
            "(outcome_status <> 'HANDOFF_READY') OR (reject_reason IS NULL AND reject_detail IS NULL)",
            name="ck_email_event_ready_clears_reject_fields",
        ),
        CheckConstraint(
            "(seen_status <> 'ACKED') OR processed_at IS NOT NULL",
            name="ck_email_event_seen_acked_requires_processed_at",
        ),
        Index("ix_email_event_processing_state", "processing_state"),
        Index("ix_email_event_outcome_status", "outcome_status"),
        Index("ix_email_event_handoff_status", "handoff_status"),
        Index("ix_email_event_seen_status", "seen_status"),
        Index("ix_email_event_message_id_normalized", "message_id_normalized"),
        Index("ix_email_event_received_at", "received_at"),
    )

    event_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    mailbox_name: Mapped[str] = mapped_column(Text, ForeignKey("mailbox_config.mailbox_name", name="fk_email_event_mailbox"), nullable=False)
    uidvalidity: Mapped[int] = mapped_column(BigInteger, nullable=False)
    uid: Mapped[int] = mapped_column(BigInteger, nullable=False)
    natural_key: Mapped[str] = mapped_column(Text, nullable=False)
    processing_state: Mapped[str] = mapped_column(Text, nullable=False, server_default="'CAPTURED'")
    outcome_status: Mapped[str | None] = mapped_column(Text, nullable=True)
    match_status: Mapped[str | None] = mapped_column(Text, nullable=True)
    carrier_strategy: Mapped[str | None] = mapped_column(Text, nullable=True)
    handoff_status: Mapped[str] = mapped_column(Text, nullable=False, server_default="'NOT_CREATED'")
    seen_status: Mapped[str] = mapped_column(Text, nullable=False, server_default="'NOT_REQUESTED'")
    message_id_raw: Mapped[str | None] = mapped_column(Text, nullable=True)
    message_id_normalized: Mapped[str | None] = mapped_column(Text, nullable=True)
    subject_raw: Mapped[str | None] = mapped_column(Text, nullable=True)
    subject_decoded: Mapped[str | None] = mapped_column(Text, nullable=True)
    from_raw: Mapped[str | None] = mapped_column(Text, nullable=True)
    sender_raw: Mapped[str | None] = mapped_column(Text, nullable=True)
    reply_to_raw: Mapped[str | None] = mapped_column(Text, nullable=True)
    to_raw: Mapped[str | None] = mapped_column(Text, nullable=True)
    cc_raw: Mapped[str | None] = mapped_column(Text, nullable=True)
    bcc_raw: Mapped[str | None] = mapped_column(Text, nullable=True)
    return_path_raw: Mapped[str | None] = mapped_column(Text, nullable=True)
    authentication_results_raw: Mapped[str | None] = mapped_column(Text, nullable=True)
    delivered_to_raw: Mapped[str | None] = mapped_column(Text, nullable=True)
    received_headers: Mapped[list | dict | None] = mapped_column(JSON, nullable=True)
    references_raw: Mapped[str | None] = mapped_column(Text, nullable=True)
    in_reply_to_raw: Mapped[str | None] = mapped_column(Text, nullable=True)
    date_header_raw: Mapped[str | None] = mapped_column(Text, nullable=True)
    internal_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    date_header_parsed_utc: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    delivery_delay_minutes: Mapped[int | None] = mapped_column(Integer, nullable=True)
    header_to_intake_delay_minutes: Mapped[int | None] = mapped_column(Integer, nullable=True)
    from_addresses: Mapped[list | dict | None] = mapped_column(JSON, nullable=True)
    sender_addresses: Mapped[list | dict | None] = mapped_column(JSON, nullable=True)
    reply_to_addresses: Mapped[list | dict | None] = mapped_column(JSON, nullable=True)
    to_addresses: Mapped[list | dict | None] = mapped_column(JSON, nullable=True)
    cc_addresses: Mapped[list | dict | None] = mapped_column(JSON, nullable=True)
    from_domain: Mapped[str | None] = mapped_column(Text, nullable=True)
    sender_domain: Mapped[str | None] = mapped_column(Text, nullable=True)
    reply_to_domain: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_headers: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_eml_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_eml_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    raw_size_bytes: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    headers_only_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    preview_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    mime_part_count_total: Mapped[int | None] = mapped_column(Integer, nullable=True)
    mime_part_count_relevant: Mapped[int | None] = mapped_column(Integer, nullable=True)
    mime_tree_depth: Mapped[int | None] = mapped_column(Integer, nullable=True)
    mime_parts: Mapped[list | dict | None] = mapped_column(JSON, nullable=True)
    mime_parse_warnings: Mapped[list | dict | None] = mapped_column(JSON, nullable=True)
    body_text_plain: Mapped[str | None] = mapped_column(Text, nullable=True)
    body_text_from_html: Mapped[str | None] = mapped_column(Text, nullable=True)
    body_html_raw_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    body_extract_truncated: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="0")
    body_plain_truncated_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    body_html_truncated_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    body_parse_warnings: Mapped[list | dict | None] = mapped_column(JSON, nullable=True)
    attachments: Mapped[list | dict | None] = mapped_column(JSON, nullable=True)
    selected_attachment_index: Mapped[int | None] = mapped_column(Integer, nullable=True)
    extracted_urls: Mapped[list | dict | None] = mapped_column(JSON, nullable=True)
    selected_url_index: Mapped[int | None] = mapped_column(Integer, nullable=True)
    supplier_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    matched_rule_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    matched_rule_snapshot: Mapped[list | dict | None] = mapped_column(JSON, nullable=True)
    candidate_supplier_ids: Mapped[list | dict | None] = mapped_column(JSON, nullable=True)
    candidate_rule_ids: Mapped[list | dict | None] = mapped_column(JSON, nullable=True)
    ambiguity_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    match_observations: Mapped[list | dict | None] = mapped_column(JSON, nullable=True)
    reject_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    reject_detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    parse_warnings: Mapped[list | dict | None] = mapped_column(JSON, nullable=True)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    processing_duration_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=now_utc_sql())

    mailbox: Mapped["MailboxConfig"] = relationship(back_populates="email_events")
    transition_logs: Mapped[list["EventTransitionLog"]] = relationship(back_populates="email_event", cascade="all, delete-orphan")
    processing_errors: Mapped[list["ProcessingErrorLog"]] = relationship(back_populates="email_event", cascade="all, delete-orphan")
    evidence_artifacts: Mapped[list["EvidenceArtifact"]] = relationship(back_populates="email_event", cascade="all, delete-orphan")
    handoff: Mapped["Handoff | None"] = relationship(back_populates="email_event", uselist=False, cascade="all, delete-orphan")
    seen_outbox: Mapped["SeenOutbox | None"] = relationship(back_populates="email_event", uselist=False, cascade="all, delete-orphan")
