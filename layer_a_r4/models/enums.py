from __future__ import annotations

from enum import StrEnum


class ProcessingState(StrEnum):
    CAPTURED = "CAPTURED"
    EVIDENCE_STAGED = "EVIDENCE_STAGED"
    EVIDENCE_COMMITTED = "EVIDENCE_COMMITTED"
    PARSED = "PARSED"
    MATCH_RESOLVED = "MATCH_RESOLVED"
    FINALIZED = "FINALIZED"


class OutcomeStatus(StrEnum):
    HANDOFF_READY = "HANDOFF_READY"
    REJECTED_UNMATCHED = "REJECTED_UNMATCHED"
    REJECTED_AMBIGUOUS = "REJECTED_AMBIGUOUS"
    REJECTED_NO_CARRIER = "REJECTED_NO_CARRIER"
    REJECTED_OVERSIZED_EMAIL = "REJECTED_OVERSIZED_EMAIL"
    REJECTED_TOO_COMPLEX = "REJECTED_TOO_COMPLEX"
    REJECTED_RELEVANT_PARTS_LIMIT = "REJECTED_RELEVANT_PARTS_LIMIT"
    REJECTED_MALFORMED_EMAIL = "REJECTED_MALFORMED_EMAIL"
    REJECTED_ATTACHMENT_POLICY = "REJECTED_ATTACHMENT_POLICY"
    REJECTED_URL_POLICY = "REJECTED_URL_POLICY"
    REJECTED_NO_SPACE = "REJECTED_NO_SPACE"
    TERMINAL_ERROR = "TERMINAL_ERROR"


class MatchStatus(StrEnum):
    MATCHED = "MATCHED"
    UNMATCHED = "UNMATCHED"
    AMBIGUOUS = "AMBIGUOUS"


class CarrierStrategy(StrEnum):
    ATTACHMENT = "ATTACHMENT"
    BODY_LINK = "BODY_LINK"
    NONE = "NONE"


class EmailEventHandoffStatus(StrEnum):
    NOT_CREATED = "NOT_CREATED"
    READY = "READY"
    PUBLISHED = "PUBLISHED"
    BLOCKED = "BLOCKED"


class HandoffRowStatus(StrEnum):
    READY = "READY"
    PUBLISHED = "PUBLISHED"
    BLOCKED = "BLOCKED"


class SeenStatus(StrEnum):
    NOT_REQUESTED = "NOT_REQUESTED"
    PENDING = "PENDING"
    ACKED = "ACKED"
    FAILED_RETRYABLE = "FAILED_RETRYABLE"
    OBSOLETE_EPOCH = "OBSOLETE_EPOCH"


class ProcessingErrorStage(StrEnum):
    CAPTURE = "CAPTURE"
    EVIDENCE = "EVIDENCE"
    PARSE = "PARSE"
    MATCH = "MATCH"
    HANDOFF = "HANDOFF"
    SEEN_ACK = "SEEN_ACK"
    FINALIZE = "FINALIZE"


class ArtifactKind(StrEnum):
    RAW_EML = "RAW_EML"
    HEADERS_ONLY = "HEADERS_ONLY"
    PREVIEW = "PREVIEW"
    HTML_BODY = "HTML_BODY"
    ATTACHMENT = "ATTACHMENT"
    MIME_PART = "MIME_PART"


class ArtifactRole(StrEnum):
    PRIMARY = "PRIMARY"
    DERIVED = "DERIVED"


class ArtifactStatus(StrEnum):
    PRESENT = "PRESENT"
    EXPIRED = "EXPIRED"
    NOT_SAVED = "NOT_SAVED"


PROCESSING_STATES = tuple(item.value for item in ProcessingState)
OUTCOME_STATUSES = tuple(item.value for item in OutcomeStatus)
MATCH_STATUSES = tuple(item.value for item in MatchStatus)
CARRIER_STRATEGIES = tuple(item.value for item in CarrierStrategy)
EMAIL_EVENT_HANDOFF_STATUSES = tuple(item.value for item in EmailEventHandoffStatus)
HANDOFF_ROW_STATUSES = tuple(item.value for item in HandoffRowStatus)
SEEN_STATUSES = tuple(item.value for item in SeenStatus)
PROCESSING_ERROR_STAGES = tuple(item.value for item in ProcessingErrorStage)
ARTIFACT_KINDS = tuple(item.value for item in ArtifactKind)
ARTIFACT_ROLES = tuple(item.value for item in ArtifactRole)
ARTIFACT_STATUSES = tuple(item.value for item in ArtifactStatus)
