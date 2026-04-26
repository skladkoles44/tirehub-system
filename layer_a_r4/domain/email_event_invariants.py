"""Pure domain validators for Layer A email_event invariants.

No database access. No side effects. Operates on ORM model instances only.

Domain value sets are derived from Block 2 enums.
Reject outcome set is **narrower** than the full OutcomeStatus enum —
only canonical Block 3 reject outcomes are allowed through the guard layer.
Additional OutcomeStatus values are valid at the storage layer but are
rejected by the domain guard as non-canonical for transition purposes.

All enum-backed fields are normalized via _enum_value() so that both
raw strings and Enum/StrEnum members are handled identically.

Cross-field coherence rules enforce:
- outcome → match_status consistency
- outcome → carrier_strategy consistency
- handoff row carrier_kind membership
- carrier_strategy=NONE implies no selected indices
- rejected terminal outcomes require non-empty reject_reason
- pre-finalized events must not carry terminal outcome or reject metadata
- carrier/attachment/url indices must be non-negative when set
"""

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

from layer_a_r4.models.enums import (
    CarrierStrategy,
    EmailEventHandoffStatus,
    HandoffRowStatus,
    MatchStatus,
    OutcomeStatus,
    ProcessingState,
    SeenStatus,
)

from .exceptions import (
    InvalidCarrierSelection,
    InvalidHandoffState,
    InvalidMatchResolution,
    InvalidSeenState,
)

if TYPE_CHECKING:
    from layer_a_r4.models.email_event import EmailEvent
    from layer_a_r4.models.handoff import Handoff
    from layer_a_r4.models.seen_outbox import SeenOutbox


def _enum_value(value: str | Enum | None) -> str | None:
    """Normalize a value that may be an Enum/StrEnum member or a plain string."""
    if value is None:
        return None
    return value.value if isinstance(value, Enum) else value


def _is_blank_text(value: object) -> bool:
    """Return True if *value* is not a non-empty string after stripping."""
    return not isinstance(value, str) or not value.strip()


def _require_none(value, field_name: str, context: str) -> None:
    if value is not None:
        raise InvalidMatchResolution(f"{field_name} must be NULL when {context}")


def _require_none_or_empty(value, field_name: str, context: str) -> None:
    if value:
        raise InvalidMatchResolution(f"{field_name} must be NULL or empty when {context}")


def _require_non_negative(
    value: int | None,
    field_name: str,
    exc_type: type[Exception] = InvalidCarrierSelection,
) -> None:
    """Require *value* to be NULL or >= 0, raising *exc_type* on violation."""
    if value is not None and value < 0:
        raise exc_type(f"{field_name} must be >= 0, got {value}")


_VALID_PROCESSING_STATES = frozenset(e.value for e in ProcessingState)
_VALID_MATCH_STATUSES = frozenset(e.value for e in MatchStatus)
_VALID_CARRIER_STRATEGIES = frozenset(e.value for e in CarrierStrategy)
_VALID_CARRIER_KINDS = frozenset({"attachment", "url", "none"})
_VALID_EMAIL_EVENT_HANDOFF_STATUSES = frozenset(e.value for e in EmailEventHandoffStatus)
_VALID_HANDOFF_ROW_STATUSES = frozenset(e.value for e in HandoffRowStatus)
_VALID_SEEN_STATUSES = frozenset(e.value for e in SeenStatus)
_VALID_OUTCOME_STATUSES = frozenset(e.value for e in OutcomeStatus)

_CANONICAL_REJECTED_OUTCOMES = frozenset({
    OutcomeStatus.REJECTED_UNMATCHED.value,
    OutcomeStatus.REJECTED_AMBIGUOUS.value,
    OutcomeStatus.REJECTED_NO_CARRIER.value,
    OutcomeStatus.REJECTED_OVERSIZED_EMAIL.value,
    OutcomeStatus.REJECTED_TOO_COMPLEX.value,
    OutcomeStatus.REJECTED_RELEVANT_PARTS_LIMIT.value,
})


def validate_matching_invariants(event: EmailEvent) -> None:
    """Проверка match_status coherence."""
    ms = _enum_value(event.match_status)
    supplier_id = event.supplier_id
    matched_rule_id = event.matched_rule_id
    candidate_supplier_ids = event.candidate_supplier_ids
    candidate_rule_ids = event.candidate_rule_ids

    if ms is None:
        _require_none(supplier_id, "supplier_id", "match_status is NULL")
        _require_none(matched_rule_id, "matched_rule_id", "match_status is NULL")
        _require_none_or_empty(candidate_supplier_ids, "candidate_supplier_ids", "match_status is NULL")
        _require_none_or_empty(candidate_rule_ids, "candidate_rule_ids", "match_status is NULL")
        return

    if ms not in _VALID_MATCH_STATUSES:
        raise InvalidMatchResolution(f"Unknown match_status: {ms}")

    if ms == "MATCHED":
        if supplier_id is None:
            raise InvalidMatchResolution("MATCHED requires supplier_id")
        if matched_rule_id is None:
            raise InvalidMatchResolution("MATCHED requires matched_rule_id")
        if candidate_supplier_ids:
            raise InvalidMatchResolution("MATCHED requires candidate_supplier_ids to be NULL or empty")
        if candidate_rule_ids:
            raise InvalidMatchResolution("MATCHED requires candidate_rule_ids to be NULL or empty")

    elif ms == "UNMATCHED":
        if supplier_id is not None:
            raise InvalidMatchResolution("UNMATCHED requires supplier_id IS NULL")
        if matched_rule_id is not None:
            raise InvalidMatchResolution("UNMATCHED requires matched_rule_id IS NULL")
        if candidate_supplier_ids:
            raise InvalidMatchResolution("UNMATCHED requires candidate_supplier_ids to be NULL or empty")
        if candidate_rule_ids:
            raise InvalidMatchResolution("UNMATCHED requires candidate_rule_ids to be NULL or empty")

    elif ms == "AMBIGUOUS":
        if supplier_id is not None:
            raise InvalidMatchResolution("AMBIGUOUS requires supplier_id IS NULL")
        if matched_rule_id is not None:
            raise InvalidMatchResolution("AMBIGUOUS requires matched_rule_id IS NULL")
        if not candidate_supplier_ids:
            raise InvalidMatchResolution("AMBIGUOUS requires non-empty candidate_supplier_ids")
        if candidate_rule_ids is not None and len(candidate_rule_ids) == 0:
            raise InvalidMatchResolution(
                "AMBIGUOUS candidate_rule_ids must be NULL or non-empty"
            )


def validate_carrier_selection_invariants(event: EmailEvent) -> None:
    """Проверка carrier_selection coherence."""
    strategy = _enum_value(event.carrier_strategy)
    att_idx = event.selected_attachment_index
    url_idx = event.selected_url_index

    if strategy is not None and strategy not in _VALID_CARRIER_STRATEGIES:
        raise InvalidCarrierSelection(f"Unknown carrier_strategy: {strategy}")

    _require_non_negative(att_idx, "selected_attachment_index")
    _require_non_negative(url_idx, "selected_url_index")

    if strategy == "NONE":
        if att_idx is not None:
            raise InvalidCarrierSelection(
                f"carrier_strategy=NONE requires selected_attachment_index IS NULL, got {att_idx}"
            )
        if url_idx is not None:
            raise InvalidCarrierSelection(
                f"carrier_strategy=NONE requires selected_url_index IS NULL, got {url_idx}"
            )
        return

    if att_idx is not None and url_idx is not None:
        raise InvalidCarrierSelection(
            "Cannot have both selected_attachment_index and selected_url_index"
        )

    if att_idx is not None and strategy != "ATTACHMENT":
        raise InvalidCarrierSelection(
            f"selected_attachment_index requires carrier_strategy=ATTACHMENT, got {strategy}"
        )

    if url_idx is not None and strategy != "BODY_LINK":
        raise InvalidCarrierSelection(
            f"selected_url_index requires carrier_strategy=BODY_LINK, got {strategy}"
        )

    if strategy == "ATTACHMENT" and att_idx is None:
        raise InvalidCarrierSelection(
            "carrier_strategy=ATTACHMENT requires selected_attachment_index"
        )

    if strategy == "BODY_LINK" and url_idx is None:
        raise InvalidCarrierSelection(
            "carrier_strategy=BODY_LINK requires selected_url_index"
        )


def validate_email_event_handoff_invariants(event: EmailEvent) -> None:
    """Проверка EmailEvent.handoff_status ↔ outcome_status coherence
    и outcome-driven cross-field rules."""
    outcome = _enum_value(event.outcome_status)
    handoff = _enum_value(event.handoff_status)
    ps = _enum_value(event.processing_state)
    carrier_strategy = _enum_value(event.carrier_strategy)
    match_status = _enum_value(event.match_status)

    if ps not in _VALID_PROCESSING_STATES:
        raise InvalidHandoffState(f"Unknown processing_state: {ps}")

    if handoff not in _VALID_EMAIL_EVENT_HANDOFF_STATUSES:
        raise InvalidHandoffState(f"Unknown handoff_status: {handoff}")

    if outcome is not None and outcome not in _VALID_OUTCOME_STATUSES:
        raise InvalidHandoffState(f"Unknown outcome_status: {outcome}")

    if ps != "FINALIZED":
        if outcome is not None:
            raise InvalidHandoffState(
                f"Non-FINALIZED event must have outcome_status=NULL, got {outcome}"
            )
        if handoff != "NOT_CREATED":
            raise InvalidHandoffState(
                f"Non-FINALIZED event must have handoff_status=NOT_CREATED, got {handoff}"
            )
        if event.reject_reason is not None or event.reject_detail is not None:
            raise InvalidHandoffState(
                "Non-FINALIZED event must have reject_reason IS NULL and reject_detail IS NULL"
            )
        return

    if outcome is None:
        raise InvalidHandoffState("FINALIZED event must have outcome_status set")

    if outcome == OutcomeStatus.HANDOFF_READY.value:
        if match_status != MatchStatus.MATCHED.value:
            raise InvalidHandoffState(
                f"HANDOFF_READY requires match_status=MATCHED, got {match_status}"
            )
        if event.supplier_id is None:
            raise InvalidHandoffState("HANDOFF_READY requires supplier_id")
        if event.matched_rule_id is None:
            raise InvalidHandoffState("HANDOFF_READY requires matched_rule_id")
        if carrier_strategy not in ("ATTACHMENT", "BODY_LINK"):
            raise InvalidHandoffState(
                f"HANDOFF_READY requires carrier_strategy ATTACHMENT or BODY_LINK, got {carrier_strategy}"
            )
        if handoff not in ("READY", "PUBLISHED"):
            raise InvalidHandoffState(
                f"HANDOFF_READY requires handoff_status READY/PUBLISHED, got {handoff}"
            )
        if event.reject_reason is not None or event.reject_detail is not None:
            raise InvalidHandoffState(
                "HANDOFF_READY requires reject_reason IS NULL and reject_detail IS NULL"
            )

    elif outcome in _CANONICAL_REJECTED_OUTCOMES:
        if handoff != "BLOCKED":
            raise InvalidHandoffState(
                f"Rejected outcome requires handoff_status=BLOCKED, got {handoff}"
            )
        if _is_blank_text(event.reject_reason):
            raise InvalidHandoffState(f"{outcome} requires non-empty reject_reason")

        if outcome == OutcomeStatus.REJECTED_UNMATCHED.value:
            if match_status != MatchStatus.UNMATCHED.value:
                raise InvalidHandoffState(
                    f"REJECTED_UNMATCHED requires match_status=UNMATCHED, got {match_status}"
                )

        if outcome == OutcomeStatus.REJECTED_AMBIGUOUS.value:
            if match_status != MatchStatus.AMBIGUOUS.value:
                raise InvalidHandoffState(
                    f"REJECTED_AMBIGUOUS requires match_status=AMBIGUOUS, got {match_status}"
                )

        if outcome == OutcomeStatus.REJECTED_NO_CARRIER.value:
            if carrier_strategy != CarrierStrategy.NONE.value:
                raise InvalidHandoffState(
                    f"REJECTED_NO_CARRIER requires carrier_strategy=NONE, got {carrier_strategy}"
                )

    else:
        raise InvalidHandoffState(
            f"OutcomeStatus not allowed by Block 3 canon for transition: {outcome}"
        )


def validate_handoff_row_invariants(handoff: Handoff) -> None:
    """Проверка инвариантов строки Handoff."""
    hs = _enum_value(handoff.handoff_status)
    cs = _enum_value(handoff.carrier_strategy)
    ck = _enum_value(handoff.carrier_kind)

    if hs not in _VALID_HANDOFF_ROW_STATUSES:
        raise InvalidHandoffState(f"Unknown handoff row status: {hs}")

    if cs not in _VALID_CARRIER_STRATEGIES:
        raise InvalidHandoffState(f"Unknown carrier_strategy in handoff: {cs}")

    if ck not in _VALID_CARRIER_KINDS:
        raise InvalidHandoffState(f"Unknown carrier_kind in handoff: {ck}")

    _require_non_negative(
        handoff.carrier_attachment_index,
        "handoff.carrier_attachment_index",
        InvalidHandoffState,
    )
    _require_non_negative(
        handoff.carrier_url_index,
        "handoff.carrier_url_index",
        InvalidHandoffState,
    )

    if hs in ("READY", "PUBLISHED") and cs == "NONE":
        raise InvalidHandoffState(
            "READY/PUBLISHED handoff cannot have carrier_strategy=NONE"
        )

    if hs in ("READY", "PUBLISHED"):
        if handoff.supplier_id is None:
            raise InvalidHandoffState("READY/PUBLISHED handoff requires supplier_id")
        if handoff.handoff_payload_ref is None:
            raise InvalidHandoffState("READY/PUBLISHED handoff requires handoff_payload_ref")

    if cs == "ATTACHMENT":
        if ck != "attachment":
            raise InvalidHandoffState(
                f"ATTACHMENT requires carrier_kind='attachment', got {ck}"
            )
        if handoff.carrier_attachment_index is None:
            raise InvalidHandoffState("ATTACHMENT requires carrier_attachment_index")
        if handoff.carrier_url_index is not None:
            raise InvalidHandoffState("ATTACHMENT requires carrier_url_index IS NULL")
        if handoff.carrier_url_ref is not None:
            raise InvalidHandoffState("ATTACHMENT requires carrier_url_ref IS NULL")
        if hs in ("READY", "PUBLISHED") and handoff.carrier_path is None:
            raise InvalidHandoffState("READY/PUBLISHED ATTACHMENT requires carrier_path")

    elif cs == "BODY_LINK":
        if ck != "url":
            raise InvalidHandoffState(
                f"BODY_LINK requires carrier_kind='url', got {ck}"
            )
        if handoff.carrier_url_index is None:
            raise InvalidHandoffState("BODY_LINK requires carrier_url_index")
        if handoff.carrier_attachment_index is not None:
            raise InvalidHandoffState("BODY_LINK requires carrier_attachment_index IS NULL")
        if handoff.carrier_path is not None:
            raise InvalidHandoffState("BODY_LINK requires carrier_path IS NULL")
        if hs in ("READY", "PUBLISHED") and handoff.carrier_url_ref is None:
            raise InvalidHandoffState("READY/PUBLISHED BODY_LINK requires carrier_url_ref")

    elif cs == "NONE":
        if ck != "none":
            raise InvalidHandoffState(
                f"NONE requires carrier_kind='none', got {ck}"
            )
        if handoff.carrier_attachment_index is not None:
            raise InvalidHandoffState("NONE requires carrier_attachment_index IS NULL")
        if handoff.carrier_url_index is not None:
            raise InvalidHandoffState("NONE requires carrier_url_index IS NULL")
        if handoff.carrier_path is not None:
            raise InvalidHandoffState("NONE requires carrier_path IS NULL")
        if handoff.carrier_url_ref is not None:
            raise InvalidHandoffState("NONE requires carrier_url_ref IS NULL")


def validate_email_event_seen_invariants(event: EmailEvent) -> None:
    """Проверка EmailEvent.seen_status coherence."""
    if event.uidvalidity <= 0:
        raise InvalidSeenState(f"uidvalidity must be > 0, got {event.uidvalidity}")
    if event.uid <= 0:
        raise InvalidSeenState(f"uid must be > 0, got {event.uid}")

    ss = _enum_value(event.seen_status)

    if ss not in _VALID_SEEN_STATUSES:
        raise InvalidSeenState(f"Unknown seen_status: {ss}")

    if ss == "ACKED" and event.processed_at is None:
        raise InvalidSeenState("ACKED requires processed_at IS NOT NULL")


def validate_seen_outbox_invariants(
    seen: SeenOutbox, event: EmailEvent | None = None
) -> None:
    """Проверка инвариантов строки SeenOutbox."""
    if seen.uidvalidity <= 0:
        raise InvalidSeenState(f"SeenOutbox uidvalidity must be > 0, got {seen.uidvalidity}")
    if seen.uid <= 0:
        raise InvalidSeenState(f"SeenOutbox uid must be > 0, got {seen.uid}")
    if seen.attempt_count < 0:
        raise InvalidSeenState(f"SeenOutbox attempt_count must be >= 0, got {seen.attempt_count}")

    if seen.confirmed_at is not None and seen.attempted_at is not None:
        if seen.confirmed_at < seen.attempted_at:
            raise InvalidSeenState(
                f"SeenOutbox confirmed_at {seen.confirmed_at} < attempted_at {seen.attempted_at}"
            )

    if event is not None:
        if seen.event_id != event.event_id:
            raise InvalidSeenState(
                f"SeenOutbox.event_id {seen.event_id} != EmailEvent.event_id {event.event_id}"
            )
        if seen.mailbox_name != event.mailbox_name:
            raise InvalidSeenState(
                f"SeenOutbox.mailbox_name {seen.mailbox_name} != EmailEvent.mailbox_name {event.mailbox_name}"
            )
        if seen.uidvalidity != event.uidvalidity:
            raise InvalidSeenState(
                f"SeenOutbox.uidvalidity {seen.uidvalidity} != EmailEvent.uidvalidity {event.uidvalidity}"
            )
        if seen.uid != event.uid:
            raise InvalidSeenState(
                f"SeenOutbox.uid {seen.uid} != EmailEvent.uid {event.uid}"
            )
        if seen.confirmed_at is not None and _enum_value(event.seen_status) != SeenStatus.ACKED.value:
            raise InvalidSeenState(
                f"SeenOutbox confirmed_at is set but EmailEvent.seen_status is "
                f"{_enum_value(event.seen_status)}, expected ACKED"
            )


def validate_email_event_invariants(event: EmailEvent) -> None:
    """Полная проверка всех инвариантов email_event."""
    validate_matching_invariants(event)
    validate_carrier_selection_invariants(event)
    validate_email_event_handoff_invariants(event)
    validate_email_event_seen_invariants(event)
