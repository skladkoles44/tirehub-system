"""layer_a initial schema r4 canonical

Revision ID: 0001_layer_a_init
Revises:
Create Date: 2026-04-22
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0001_layer_a_init"
down_revision = None
branch_labels = None
depends_on = None


PROCESSING_STATES = (
    "CAPTURED",
    "EVIDENCE_STAGED",
    "EVIDENCE_COMMITTED",
    "PARSED",
    "MATCH_RESOLVED",
    "FINALIZED",
)

OUTCOME_STATUSES = (
    "HANDOFF_READY",
    "REJECTED_UNMATCHED",
    "REJECTED_AMBIGUOUS",
    "REJECTED_NO_CARRIER",
    "REJECTED_OVERSIZED_EMAIL",
    "REJECTED_TOO_COMPLEX",
    "REJECTED_RELEVANT_PARTS_LIMIT",
    "REJECTED_MALFORMED_EMAIL",
    "REJECTED_ATTACHMENT_POLICY",
    "REJECTED_URL_POLICY",
    "REJECTED_NO_SPACE",
    "TERMINAL_ERROR",
)

MATCH_STATUSES = (
    "MATCHED",
    "UNMATCHED",
    "AMBIGUOUS",
)

CARRIER_STRATEGIES = (
    "ATTACHMENT",
    "BODY_LINK",
    "NONE",
)

EMAIL_EVENT_HANDOFF_STATUSES = (
    "NOT_CREATED",
    "READY",
    "PUBLISHED",
    "BLOCKED",
)

HANDOFF_ROW_STATUSES = (
    "READY",
    "PUBLISHED",
    "BLOCKED",
)

SEEN_STATUSES = (
    "NOT_REQUESTED",
    "PENDING",
    "ACKED",
    "FAILED_RETRYABLE",
    "OBSOLETE_EPOCH",
)

PROCESSING_ERROR_STAGES = (
    "CAPTURE",
    "EVIDENCE",
    "PARSE",
    "MATCH",
    "HANDOFF",
    "SEEN_ACK",
    "FINALIZE",
)

ARTIFACT_KINDS = (
    "RAW_EML",
    "HEADERS_ONLY",
    "PREVIEW",
    "HTML_BODY",
    "ATTACHMENT",
    "MIME_PART",
)

ARTIFACT_ROLES = (
    "PRIMARY",
    "DERIVED",
)

ARTIFACT_STATUSES = (
    "PRESENT",
    "EXPIRED",
    "NOT_SAVED",
)


def _now() -> sa.TextClause:
    return sa.text("CURRENT_TIMESTAMP")


def _enum_check(name: str, column_name: str, values: tuple[str, ...], nullable: bool = False) -> sa.CheckConstraint:
    quoted = ", ".join(f"'{value}'" for value in values)
    if nullable:
        sql = f"{column_name} IS NULL OR {column_name} IN ({quoted})"
    else:
        sql = f"{column_name} IN ({quoted})"
    return sa.CheckConstraint(sql, name=name)


def upgrade() -> None:
    op.create_table(
        "mailbox_config",
        sa.Column("mailbox_name", sa.Text(), primary_key=True),
        sa.Column("display_name", sa.Text(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("1")),
        sa.Column("match_policy_version", sa.Text(), nullable=True),
        sa.Column("storage_policy_version", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=_now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=_now()),
    )
    op.create_index("ix_mailbox_config_is_active", "mailbox_config", ["is_active"])

    op.create_table(
        "email_event",
        sa.Column("event_id", sa.String(length=64), primary_key=True),
        sa.Column("mailbox_name", sa.Text(), nullable=False),
        sa.Column("uidvalidity", sa.BigInteger(), nullable=False),
        sa.Column("uid", sa.BigInteger(), nullable=False),
        sa.Column("natural_key", sa.Text(), nullable=False),
        sa.Column("processing_state", sa.Text(), nullable=False, server_default=sa.text("'CAPTURED'")),
        sa.Column("outcome_status", sa.Text(), nullable=True),
        sa.Column("match_status", sa.Text(), nullable=True),
        sa.Column("carrier_strategy", sa.Text(), nullable=True),
        sa.Column("handoff_status", sa.Text(), nullable=False, server_default=sa.text("'NOT_CREATED'")),
        sa.Column("seen_status", sa.Text(), nullable=False, server_default=sa.text("'NOT_REQUESTED'")),
        sa.Column("message_id_raw", sa.Text(), nullable=True),
        sa.Column("message_id_normalized", sa.Text(), nullable=True),
        sa.Column("subject_raw", sa.Text(), nullable=True),
        sa.Column("subject_decoded", sa.Text(), nullable=True),
        sa.Column("from_raw", sa.Text(), nullable=True),
        sa.Column("sender_raw", sa.Text(), nullable=True),
        sa.Column("reply_to_raw", sa.Text(), nullable=True),
        sa.Column("to_raw", sa.Text(), nullable=True),
        sa.Column("cc_raw", sa.Text(), nullable=True),
        sa.Column("bcc_raw", sa.Text(), nullable=True),
        sa.Column("return_path_raw", sa.Text(), nullable=True),
        sa.Column("authentication_results_raw", sa.Text(), nullable=True),
        sa.Column("delivered_to_raw", sa.Text(), nullable=True),
        sa.Column("received_headers", sa.JSON(), nullable=True),
        sa.Column("references_raw", sa.Text(), nullable=True),
        sa.Column("in_reply_to_raw", sa.Text(), nullable=True),
        sa.Column("date_header_raw", sa.Text(), nullable=True),
        sa.Column("internal_date", sa.DateTime(timezone=True), nullable=True),
        sa.Column("date_header_parsed_utc", sa.DateTime(timezone=True), nullable=True),
        sa.Column("delivery_delay_minutes", sa.Integer(), nullable=True),
        sa.Column("header_to_intake_delay_minutes", sa.Integer(), nullable=True),
        sa.Column("from_addresses", sa.JSON(), nullable=True),
        sa.Column("sender_addresses", sa.JSON(), nullable=True),
        sa.Column("reply_to_addresses", sa.JSON(), nullable=True),
        sa.Column("to_addresses", sa.JSON(), nullable=True),
        sa.Column("cc_addresses", sa.JSON(), nullable=True),
        sa.Column("from_domain", sa.Text(), nullable=True),
        sa.Column("sender_domain", sa.Text(), nullable=True),
        sa.Column("reply_to_domain", sa.Text(), nullable=True),
        sa.Column("raw_headers", sa.Text(), nullable=True),
        sa.Column("raw_eml_path", sa.Text(), nullable=True),
        sa.Column("raw_eml_sha256", sa.String(length=64), nullable=True),
        sa.Column("raw_size_bytes", sa.BigInteger(), nullable=True),
        sa.Column("headers_only_path", sa.Text(), nullable=True),
        sa.Column("preview_path", sa.Text(), nullable=True),
        sa.Column("mime_part_count_total", sa.Integer(), nullable=True),
        sa.Column("mime_part_count_relevant", sa.Integer(), nullable=True),
        sa.Column("mime_tree_depth", sa.Integer(), nullable=True),
        sa.Column("mime_parts", sa.JSON(), nullable=True),
        sa.Column("mime_parse_warnings", sa.JSON(), nullable=True),
        sa.Column("body_text_plain", sa.Text(), nullable=True),
        sa.Column("body_text_from_html", sa.Text(), nullable=True),
        sa.Column("body_html_raw_path", sa.Text(), nullable=True),
        sa.Column("body_extract_truncated", sa.Boolean(), nullable=False, server_default=sa.text("0")),
        sa.Column("body_plain_truncated_reason", sa.Text(), nullable=True),
        sa.Column("body_html_truncated_reason", sa.Text(), nullable=True),
        sa.Column("body_parse_warnings", sa.JSON(), nullable=True),
        sa.Column("attachments", sa.JSON(), nullable=True),
        sa.Column("selected_attachment_index", sa.Integer(), nullable=True),
        sa.Column("extracted_urls", sa.JSON(), nullable=True),
        sa.Column("selected_url_index", sa.Integer(), nullable=True),
        sa.Column("supplier_id", sa.Text(), nullable=True),
        sa.Column("matched_rule_id", sa.Text(), nullable=True),
        sa.Column("matched_rule_snapshot", sa.JSON(), nullable=True),
        sa.Column("candidate_supplier_ids", sa.JSON(), nullable=True),
        sa.Column("candidate_rule_ids", sa.JSON(), nullable=True),
        sa.Column("ambiguity_reason", sa.Text(), nullable=True),
        sa.Column("match_observations", sa.JSON(), nullable=True),
        sa.Column("reject_reason", sa.Text(), nullable=True),
        sa.Column("reject_detail", sa.Text(), nullable=True),
        sa.Column("parse_warnings", sa.JSON(), nullable=True),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("processing_duration_ms", sa.Integer(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=_now()),
        sa.ForeignKeyConstraint(["mailbox_name"], ["mailbox_config.mailbox_name"], name="fk_email_event_mailbox"),
        sa.UniqueConstraint("mailbox_name", "uidvalidity", "uid", name="uq_email_event_imap_triplet"),
        sa.CheckConstraint("uidvalidity > 0", name="ck_email_event_uidvalidity_positive"),
        sa.CheckConstraint("uid > 0", name="ck_email_event_uid_positive"),
        _enum_check("ck_email_event_processing_state", "processing_state", PROCESSING_STATES),
        _enum_check("ck_email_event_outcome_status", "outcome_status", OUTCOME_STATUSES, nullable=True),
        _enum_check("ck_email_event_match_status", "match_status", MATCH_STATUSES, nullable=True),
        _enum_check("ck_email_event_carrier_strategy", "carrier_strategy", CARRIER_STRATEGIES, nullable=True),
        _enum_check("ck_email_event_handoff_status", "handoff_status", EMAIL_EVENT_HANDOFF_STATUSES),
        _enum_check("ck_email_event_seen_status", "seen_status", SEEN_STATUSES),
        sa.CheckConstraint(
            "((processing_state <> 'FINALIZED') AND outcome_status IS NULL) "
            "OR ((processing_state = 'FINALIZED') AND outcome_status IS NOT NULL)",
            name="ck_email_event_finalized_requires_outcome",
        ),
        sa.CheckConstraint(
            "(match_status <> 'MATCHED') OR (supplier_id IS NOT NULL AND matched_rule_id IS NOT NULL)",
            name="ck_email_event_matched_requires_supplier_rule",
        ),
        sa.CheckConstraint(
            "(match_status <> 'UNMATCHED') OR supplier_id IS NULL",
            name="ck_email_event_unmatched_supplier_null",
        ),
        sa.CheckConstraint(
            "(match_status <> 'AMBIGUOUS') OR (supplier_id IS NULL AND candidate_supplier_ids IS NOT NULL)",
            name="ck_email_event_ambiguous_has_candidates",
        ),
        sa.CheckConstraint(
            "(selected_attachment_index IS NULL) OR carrier_strategy = 'ATTACHMENT'",
            name="ck_email_event_selected_attachment_strategy",
        ),
        sa.CheckConstraint(
            "(selected_url_index IS NULL) OR carrier_strategy = 'BODY_LINK'",
            name="ck_email_event_selected_url_strategy",
        ),
        sa.CheckConstraint(
            "(outcome_status <> 'HANDOFF_READY') OR (carrier_strategy IN ('ATTACHMENT', 'BODY_LINK'))",
            name="ck_email_event_ready_disallows_none_strategy",
        ),
        sa.CheckConstraint(
            "(outcome_status <> 'HANDOFF_READY') OR "
            "(carrier_strategy <> 'ATTACHMENT') OR "
            "(selected_attachment_index IS NOT NULL)",
            name="ck_email_event_ready_attachment_selected",
        ),
        sa.CheckConstraint(
            "(outcome_status <> 'HANDOFF_READY') OR "
            "(carrier_strategy <> 'BODY_LINK') OR "
            "(selected_url_index IS NOT NULL)",
            name="ck_email_event_ready_url_selected",
        ),
        sa.CheckConstraint(
            "(outcome_status <> 'HANDOFF_READY') OR (handoff_status IN ('READY', 'PUBLISHED'))",
            name="ck_email_event_ready_requires_handoff_status",
        ),
        sa.CheckConstraint(
            "(processing_state <> 'FINALIZED') OR "
            "(outcome_status = 'HANDOFF_READY') OR "
            "(handoff_status = 'BLOCKED')",
            name="ck_email_event_reject_requires_blocked_handoff",
        ),
        sa.CheckConstraint(
            "(outcome_status <> 'HANDOFF_READY') OR (reject_reason IS NULL AND reject_detail IS NULL)",
            name="ck_email_event_ready_clears_reject_fields",
        ),
        sa.CheckConstraint(
            "(seen_status <> 'ACKED') OR processed_at IS NOT NULL",
            name="ck_email_event_seen_acked_requires_processed_at",
        ),
    )
    op.create_index("ix_email_event_processing_state", "email_event", ["processing_state"])
    op.create_index("ix_email_event_outcome_status", "email_event", ["outcome_status"])
    op.create_index("ix_email_event_handoff_status", "email_event", ["handoff_status"])
    op.create_index("ix_email_event_seen_status", "email_event", ["seen_status"])
    op.create_index("ix_email_event_message_id_normalized", "email_event", ["message_id_normalized"])
    op.create_index("ix_email_event_received_at", "email_event", ["received_at"])

    op.create_table(
        "event_transition_log",
        sa.Column("transition_id", sa.String(length=64), primary_key=True),
        sa.Column("event_id", sa.String(length=64), sa.ForeignKey("email_event.event_id", ondelete="CASCADE"), nullable=False),
        sa.Column("transition_seq", sa.Integer(), nullable=False),
        sa.Column("ts_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("from_state", sa.Text(), nullable=True),
        sa.Column("to_state", sa.Text(), nullable=False),
        sa.Column("actor", sa.Text(), nullable=False),
        sa.Column("reason_code", sa.Text(), nullable=True),
        sa.Column("detail_ref", sa.Text(), nullable=True),
        sa.Column("duration_ms", sa.Integer(), nullable=True),
        sa.UniqueConstraint("event_id", "transition_seq", name="uq_event_transition_log_seq"),
        _enum_check("ck_transition_from_state", "from_state", PROCESSING_STATES, nullable=True),
        _enum_check("ck_transition_to_state", "to_state", PROCESSING_STATES),
        sa.CheckConstraint("duration_ms IS NULL OR duration_ms >= 0", name="ck_event_transition_log_duration_nonnegative"),
    )
    op.create_index("ix_event_transition_log_event_id", "event_transition_log", ["event_id", "transition_seq"])

    op.create_table(
        "processing_error_log",
        sa.Column("error_id", sa.String(length=64), primary_key=True),
        sa.Column("event_id", sa.String(length=64), sa.ForeignKey("email_event.event_id", ondelete="CASCADE"), nullable=False),
        sa.Column("stage", sa.Text(), nullable=False),
        sa.Column("error_class", sa.Text(), nullable=True),
        sa.Column("error_code", sa.Text(), nullable=True),
        sa.Column("message", sa.Text(), nullable=True),
        sa.Column("traceback", sa.Text(), nullable=True),
        sa.Column("is_retryable", sa.Boolean(), nullable=False, server_default=sa.text("0")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=_now()),
        _enum_check("ck_processing_error_log_stage", "stage", PROCESSING_ERROR_STAGES),
    )
    op.create_index("ix_processing_error_log_event_id", "processing_error_log", ["event_id", "created_at"])
    op.create_index("ix_processing_error_log_stage", "processing_error_log", ["stage"])

    op.create_table(
        "evidence_artifact",
        sa.Column("artifact_id", sa.String(length=64), primary_key=True),
        sa.Column("event_id", sa.String(length=64), sa.ForeignKey("email_event.event_id", ondelete="CASCADE"), nullable=False),
        sa.Column("artifact_kind", sa.Text(), nullable=False),
        sa.Column("artifact_role", sa.Text(), nullable=False),
        sa.Column("artifact_path", sa.Text(), nullable=True),
        sa.Column("artifact_status", sa.Text(), nullable=False),
        sa.Column("sha256", sa.String(length=64), nullable=True),
        sa.Column("size_bytes", sa.BigInteger(), nullable=True),
        sa.Column("storage_policy_version", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=_now()),
        sa.Column("expired_at", sa.DateTime(timezone=True), nullable=True),
        _enum_check("ck_evidence_artifact_kind", "artifact_kind", ARTIFACT_KINDS),
        _enum_check("ck_evidence_artifact_role", "artifact_role", ARTIFACT_ROLES),
        _enum_check("ck_evidence_artifact_status", "artifact_status", ARTIFACT_STATUSES),
        sa.CheckConstraint(
            "(artifact_status <> 'PRESENT') OR artifact_path IS NOT NULL",
            name="ck_evidence_artifact_present_requires_path",
        ),
        sa.CheckConstraint(
            "(artifact_status <> 'EXPIRED') OR expired_at IS NOT NULL",
            name="ck_evidence_artifact_expired_requires_time",
        ),
        sa.CheckConstraint(
            "(artifact_status <> 'NOT_SAVED') OR artifact_path IS NULL",
            name="ck_evidence_artifact_not_saved_no_path",
        ),
    )
    op.create_index("ix_evidence_artifact_event_id", "evidence_artifact", ["event_id"])
    op.create_index("ix_evidence_artifact_kind", "evidence_artifact", ["artifact_kind"])
    op.create_index("ix_evidence_artifact_status", "evidence_artifact", ["artifact_status"])

    op.create_table(
        "handoff",
        sa.Column("handoff_id", sa.String(length=64), primary_key=True),
        sa.Column("email_event_id", sa.String(length=64), sa.ForeignKey("email_event.event_id", ondelete="CASCADE"), nullable=False),
        sa.Column("supplier_id", sa.Text(), nullable=True),
        sa.Column("carrier_strategy", sa.Text(), nullable=False),
        sa.Column("carrier_kind", sa.Text(), nullable=False),
        sa.Column("carrier_attachment_index", sa.Integer(), nullable=True),
        sa.Column("carrier_url_index", sa.Integer(), nullable=True),
        sa.Column("carrier_path", sa.Text(), nullable=True),
        sa.Column("carrier_url_ref", sa.Text(), nullable=True),
        sa.Column("evidence_ref", sa.Text(), nullable=True),
        sa.Column("matched_rule_id", sa.Text(), nullable=True),
        sa.Column("handoff_status", sa.Text(), nullable=False),
        sa.Column("handoff_payload_ref", sa.Text(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=_now()),
        sa.UniqueConstraint("email_event_id", name="uq_handoff_email_event_id"),
        _enum_check("ck_handoff_carrier_strategy", "carrier_strategy", CARRIER_STRATEGIES),
        _enum_check("ck_handoff_status", "handoff_status", HANDOFF_ROW_STATUSES),
        sa.CheckConstraint("carrier_kind IN ('attachment', 'url', 'none')", name="ck_handoff_carrier_kind"),
        sa.CheckConstraint(
            "(carrier_strategy <> 'ATTACHMENT') OR "
            "("
            "carrier_kind = 'attachment' AND "
            "carrier_attachment_index IS NOT NULL AND "
            "carrier_url_index IS NULL AND "
            "carrier_url_ref IS NULL"
            ")",
            name="ck_handoff_attachment_shape",
        ),
        sa.CheckConstraint(
            "(carrier_strategy <> 'BODY_LINK') OR "
            "("
            "carrier_kind = 'url' AND "
            "carrier_url_index IS NOT NULL AND "
            "carrier_attachment_index IS NULL AND "
            "carrier_path IS NULL"
            ")",
            name="ck_handoff_url_shape",
        ),
        sa.CheckConstraint(
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
        sa.CheckConstraint(
            "(handoff_status NOT IN ('READY', 'PUBLISHED')) OR supplier_id IS NOT NULL",
            name="ck_handoff_ready_requires_supplier",
        ),
        sa.CheckConstraint(
            "(handoff_status NOT IN ('READY', 'PUBLISHED')) OR handoff_payload_ref IS NOT NULL",
            name="ck_handoff_ready_requires_payload_ref",
        ),
        sa.CheckConstraint(
            "(handoff_status NOT IN ('READY', 'PUBLISHED')) OR "
            "("
            "(carrier_strategy = 'ATTACHMENT' AND carrier_path IS NOT NULL) OR "
            "(carrier_strategy = 'BODY_LINK' AND carrier_url_ref IS NOT NULL)"
            ")",
            name="ck_handoff_ready_requires_materialized_carrier_ref",
        ),
    )
    op.create_index("ix_handoff_status", "handoff", ["handoff_status"])

    op.create_table(
        "handoff_outbox",
        sa.Column("handoff_outbox_id", sa.String(length=64), primary_key=True),
        sa.Column("handoff_id", sa.String(length=64), sa.ForeignKey("handoff.handoff_id", ondelete="CASCADE"), nullable=False),
        sa.Column("payload_ref", sa.Text(), nullable=False),
        sa.Column("publish_attempt_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("publish_attempted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("publish_confirmed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_publish_error_code", sa.Text(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=_now()),
        sa.UniqueConstraint("handoff_id", name="uq_handoff_outbox_handoff_id"),
        sa.CheckConstraint("publish_attempt_count >= 0", name="ck_handoff_outbox_attempt_nonnegative"),
    )
    op.create_index("ix_handoff_outbox_pending", "handoff_outbox", ["publish_confirmed_at", "publish_attempted_at"])

    op.create_table(
        "seen_outbox",
        sa.Column("seen_task_id", sa.String(length=128), primary_key=True),
        sa.Column("event_id", sa.String(length=64), sa.ForeignKey("email_event.event_id", ondelete="CASCADE"), nullable=False),
        sa.Column("mailbox_name", sa.Text(), nullable=False),
        sa.Column("uidvalidity", sa.BigInteger(), nullable=False),
        sa.Column("uid", sa.BigInteger(), nullable=False),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("attempted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("confirmed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error_code", sa.Text(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=_now()),
        sa.UniqueConstraint("event_id", name="uq_seen_outbox_event_id"),
        sa.CheckConstraint("attempt_count >= 0", name="ck_seen_outbox_attempt_nonnegative"),
    )
    op.create_index("ix_seen_outbox_pending", "seen_outbox", ["uidvalidity", "confirmed_at", "attempted_at"])


def downgrade() -> None:
    op.drop_index("ix_seen_outbox_pending", table_name="seen_outbox")
    op.drop_table("seen_outbox")

    op.drop_index("ix_handoff_outbox_pending", table_name="handoff_outbox")
    op.drop_table("handoff_outbox")

    op.drop_index("ix_handoff_status", table_name="handoff")
    op.drop_table("handoff")

    op.drop_index("ix_evidence_artifact_status", table_name="evidence_artifact")
    op.drop_index("ix_evidence_artifact_kind", table_name="evidence_artifact")
    op.drop_index("ix_evidence_artifact_event_id", table_name="evidence_artifact")
    op.drop_table("evidence_artifact")

    op.drop_index("ix_processing_error_log_stage", table_name="processing_error_log")
    op.drop_index("ix_processing_error_log_event_id", table_name="processing_error_log")
    op.drop_table("processing_error_log")

    op.drop_index("ix_event_transition_log_event_id", table_name="event_transition_log")
    op.drop_table("event_transition_log")

    op.drop_index("ix_email_event_received_at", table_name="email_event")
    op.drop_index("ix_email_event_message_id_normalized", table_name="email_event")
    op.drop_index("ix_email_event_seen_status", table_name="email_event")
    op.drop_index("ix_email_event_handoff_status", table_name="email_event")
    op.drop_index("ix_email_event_outcome_status", table_name="email_event")
    op.drop_index("ix_email_event_processing_state", table_name="email_event")
    op.drop_table("email_event")

    op.drop_index("ix_mailbox_config_is_active", table_name="mailbox_config")
    op.drop_table("mailbox_config")
