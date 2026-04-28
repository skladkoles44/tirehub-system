"""Tests for email_event snapshot invariants.

Covers every violation class from email_event_invariants.py:
- InvalidMatchResolution
- InvalidCarrierSelection
- InvalidHandoffState
- InvalidSeenState
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from layer_a_r4.domain.email_event_invariants import (
    validate_carrier_selection_invariants,
    validate_email_event_handoff_invariants,
    validate_email_event_invariants,
    validate_email_event_seen_invariants,
    validate_handoff_row_invariants,
    validate_matching_invariants,
    validate_seen_outbox_invariants,
)
from layer_a_r4.domain.exceptions import (
    InvalidCarrierSelection,
    InvalidHandoffState,
    InvalidMatchResolution,
    InvalidSeenState,
)
from layer_a_r4.models.email_event import EmailEvent
from layer_a_r4.models.handoff import Handoff
from layer_a_r4.models.seen_outbox import SeenOutbox


_TS = datetime(2026, 4, 23, 12, 0, 0, tzinfo=timezone.utc)
_TS2 = datetime(2026, 4, 23, 12, 0, 1, tzinfo=timezone.utc)


def _email_event(**overrides) -> EmailEvent:
    base = {
        "event_id": "evt-001",
        "mailbox_name": "INBOX",
        "uidvalidity": 1,
        "uid": 1001,
        "natural_key": "INBOX:1:1001",
        "processing_state": "MATCH_RESOLVED",
        "outcome_status": None,
        "match_status": "MATCHED",
        "carrier_strategy": "ATTACHMENT",
        "handoff_status": "NOT_CREATED",
        "seen_status": "NOT_REQUESTED",
        "supplier_id": "supplier-42",
        "matched_rule_id": "rule-7",
        "selected_attachment_index": 0,
        "selected_url_index": None,
        "reject_reason": None,
        "reject_detail": None,
        "candidate_supplier_ids": None,
        "candidate_rule_ids": None,
        "received_at": _TS,
        "first_seen_at": _TS,
        "processed_at": None,
        "processing_duration_ms": None,
        "updated_at": _TS,
    }
    base.update(overrides)
    return EmailEvent(**base)


def _handoff(**overrides) -> Handoff:
    base = {
        "handoff_id": "h-001",
        "email_event_id": "evt-001",
        "supplier_id": "supplier-42",
        "carrier_strategy": "ATTACHMENT",
        "carrier_kind": "attachment",
        "carrier_attachment_index": 0,
        "carrier_url_index": None,
        "carrier_path": "/tmp/evt-001/attachment.blob",
        "carrier_url_ref": None,
        "evidence_ref": None,
        "matched_rule_id": "rule-7",
        "handoff_status": "READY",
        "handoff_payload_ref": "/tmp/evt-001/handoff.json",
        "updated_at": _TS,
    }
    base.update(overrides)
    return Handoff(**base)


def _seen_outbox(event: EmailEvent, **overrides) -> SeenOutbox:
    base = {
        "seen_task_id": "seen-001",
        "event_id": event.event_id,
        "mailbox_name": event.mailbox_name,
        "uidvalidity": event.uidvalidity,
        "uid": event.uid,
        "attempt_count": 1,
        "attempted_at": _TS,
        "confirmed_at": _TS2,
        "last_error_code": None,
        "updated_at": _TS,
    }
    base.update(overrides)
    return SeenOutbox(**base)


def test_matched_requires_supplier_id():
    event = _email_event(match_status="MATCHED", supplier_id=None)
    with pytest.raises(InvalidMatchResolution, match="supplier_id"):
        validate_matching_invariants(event)


def test_matched_requires_matched_rule_id():
    event = _email_event(match_status="MATCHED", matched_rule_id=None)
    with pytest.raises(InvalidMatchResolution, match="matched_rule_id"):
        validate_matching_invariants(event)


def test_matched_forbids_candidate_supplier_ids():
    event = _email_event(match_status="MATCHED", candidate_supplier_ids=["s1"])
    with pytest.raises(InvalidMatchResolution, match="candidate_supplier_ids"):
        validate_matching_invariants(event)


def test_matched_forbids_candidate_rule_ids():
    event = _email_event(match_status="MATCHED", candidate_rule_ids=["r1"])
    with pytest.raises(InvalidMatchResolution, match="candidate_rule_ids"):
        validate_matching_invariants(event)


def test_unmatched_forbids_supplier_id():
    event = _email_event(
        match_status="UNMATCHED",
        supplier_id="s",
        matched_rule_id=None,
    )
    with pytest.raises(InvalidMatchResolution, match="supplier_id"):
        validate_matching_invariants(event)


def test_unmatched_forbids_matched_rule_id():
    event = _email_event(
        match_status="UNMATCHED",
        supplier_id=None,
        matched_rule_id="r",
    )
    with pytest.raises(InvalidMatchResolution, match="matched_rule_id"):
        validate_matching_invariants(event)


def test_unmatched_forbids_candidate_supplier_ids():
    event = _email_event(
        match_status="UNMATCHED",
        supplier_id=None,
        matched_rule_id=None,
        candidate_supplier_ids=["s1"],
    )
    with pytest.raises(InvalidMatchResolution, match="candidate_supplier_ids"):
        validate_matching_invariants(event)


def test_unmatched_forbids_candidate_rule_ids():
    event = _email_event(
        match_status="UNMATCHED",
        supplier_id=None,
        matched_rule_id=None,
        candidate_rule_ids=["r1"],
    )
    with pytest.raises(InvalidMatchResolution, match="candidate_rule_ids"):
        validate_matching_invariants(event)


def test_ambiguous_requires_candidate_supplier_ids():
    event = _email_event(
        match_status="AMBIGUOUS",
        supplier_id=None,
        matched_rule_id=None,
        candidate_supplier_ids=None,
    )
    with pytest.raises(InvalidMatchResolution, match="candidate_supplier_ids"):
        validate_matching_invariants(event)


def test_ambiguous_forbids_empty_candidate_rule_ids():
    event = _email_event(
        match_status="AMBIGUOUS",
        supplier_id=None,
        matched_rule_id=None,
        candidate_supplier_ids=["s1"],
        candidate_rule_ids=[],
    )
    with pytest.raises(InvalidMatchResolution, match="candidate_rule_ids"):
        validate_matching_invariants(event)


def test_unknown_match_status():
    event = _email_event(match_status="GARBAGE")
    with pytest.raises(InvalidMatchResolution, match="Unknown match_status"):
        validate_matching_invariants(event)


def test_both_indices_set():
    event = _email_event(selected_attachment_index=0, selected_url_index=0)
    with pytest.raises(InvalidCarrierSelection, match="both"):
        validate_carrier_selection_invariants(event)


def test_attachment_without_index():
    event = _email_event(
        carrier_strategy="ATTACHMENT",
        selected_attachment_index=None,
    )
    with pytest.raises(InvalidCarrierSelection, match="selected_attachment_index"):
        validate_carrier_selection_invariants(event)


def test_body_link_without_index():
    event = _email_event(
        carrier_strategy="BODY_LINK",
        selected_url_index=None,
        selected_attachment_index=None,
    )
    with pytest.raises(InvalidCarrierSelection, match="selected_url_index"):
        validate_carrier_selection_invariants(event)


def test_none_with_attachment_index():
    event = _email_event(
        carrier_strategy="NONE",
        selected_attachment_index=0,
        selected_url_index=None,
    )
    with pytest.raises(InvalidCarrierSelection, match="NONE requires"):
        validate_carrier_selection_invariants(event)


def test_none_with_url_index():
    event = _email_event(
        carrier_strategy="NONE",
        selected_attachment_index=None,
        selected_url_index=0,
    )
    with pytest.raises(InvalidCarrierSelection, match="NONE requires"):
        validate_carrier_selection_invariants(event)


def test_negative_attachment_index():
    event = _email_event(selected_attachment_index=-1)
    with pytest.raises(InvalidCarrierSelection, match=">= 0"):
        validate_carrier_selection_invariants(event)


def test_negative_url_index():
    event = _email_event(
        carrier_strategy="BODY_LINK",
        selected_attachment_index=None,
        selected_url_index=-1,
    )
    with pytest.raises(InvalidCarrierSelection, match=">= 0"):
        validate_carrier_selection_invariants(event)


def test_unknown_carrier_strategy():
    event = _email_event(carrier_strategy="GARBAGE")
    with pytest.raises(InvalidCarrierSelection, match="Unknown carrier_strategy"):
        validate_carrier_selection_invariants(event)


def test_non_finalized_with_outcome():
    event = _email_event(processing_state="PARSED", outcome_status="HANDOFF_READY")
    with pytest.raises(InvalidHandoffState, match="Non-FINALIZED"):
        validate_email_event_handoff_invariants(event)


def test_non_finalized_with_handoff_not_created():
    event = _email_event(processing_state="PARSED", handoff_status="READY")
    with pytest.raises(InvalidHandoffState, match="Non-FINALIZED"):
        validate_email_event_handoff_invariants(event)


def test_non_finalized_with_reject_reason():
    event = _email_event(processing_state="PARSED", reject_reason="early")
    with pytest.raises(InvalidHandoffState, match="Non-FINALIZED"):
        validate_email_event_handoff_invariants(event)


def test_finalized_without_outcome():
    event = _email_event(processing_state="FINALIZED", outcome_status=None)
    with pytest.raises(InvalidHandoffState, match="outcome_status"):
        validate_email_event_handoff_invariants(event)


def test_handoff_ready_requires_matched():
    event = _email_event(
        processing_state="FINALIZED",
        outcome_status="HANDOFF_READY",
        match_status="UNMATCHED",
        supplier_id="s",
        matched_rule_id="r",
        handoff_status="READY",
    )
    with pytest.raises(InvalidHandoffState, match="MATCHED"):
        validate_email_event_handoff_invariants(event)


def test_handoff_ready_requires_supplier_id():
    event = _email_event(
        processing_state="FINALIZED",
        outcome_status="HANDOFF_READY",
        match_status="MATCHED",
        supplier_id=None,
        matched_rule_id="r",
        handoff_status="READY",
    )
    with pytest.raises(InvalidHandoffState, match="supplier_id"):
        validate_email_event_handoff_invariants(event)


def test_handoff_ready_requires_matched_rule_id():
    event = _email_event(
        processing_state="FINALIZED",
        outcome_status="HANDOFF_READY",
        match_status="MATCHED",
        supplier_id="s",
        matched_rule_id=None,
        handoff_status="READY",
    )
    with pytest.raises(InvalidHandoffState, match="matched_rule_id"):
        validate_email_event_handoff_invariants(event)


def test_handoff_ready_requires_carrier_strategy():
    event = _email_event(
        processing_state="FINALIZED",
        outcome_status="HANDOFF_READY",
        match_status="MATCHED",
        supplier_id="s",
        matched_rule_id="r",
        carrier_strategy="NONE",
        handoff_status="READY",
    )
    with pytest.raises(InvalidHandoffState, match="carrier_strategy"):
        validate_email_event_handoff_invariants(event)


def test_handoff_ready_requires_handoff_status():
    event = _email_event(
        processing_state="FINALIZED",
        outcome_status="HANDOFF_READY",
        match_status="MATCHED",
        supplier_id="s",
        matched_rule_id="r",
        carrier_strategy="ATTACHMENT",
        selected_attachment_index=0,
        handoff_status="BLOCKED",
    )
    with pytest.raises(InvalidHandoffState, match="handoff_status"):
        validate_email_event_handoff_invariants(event)


def test_handoff_ready_forbids_reject_fields():
    event = _email_event(
        processing_state="FINALIZED",
        outcome_status="HANDOFF_READY",
        match_status="MATCHED",
        supplier_id="s",
        matched_rule_id="r",
        handoff_status="READY",
        reject_reason="bad",
    )
    with pytest.raises(InvalidHandoffState, match="reject_reason"):
        validate_email_event_handoff_invariants(event)


def test_rejected_requires_blocked():
    event = _email_event(
        processing_state="FINALIZED",
        outcome_status="REJECTED_TOO_COMPLEX",
        reject_reason="too complex",
        handoff_status="READY",
    )
    with pytest.raises(InvalidHandoffState, match="BLOCKED"):
        validate_email_event_handoff_invariants(event)


def test_rejected_requires_reject_reason():
    event = _email_event(
        processing_state="FINALIZED",
        outcome_status="REJECTED_TOO_COMPLEX",
        reject_reason=None,
        handoff_status="BLOCKED",
    )
    with pytest.raises(InvalidHandoffState, match="reject_reason"):
        validate_email_event_handoff_invariants(event)


def test_rejected_requires_nonempty_reject_reason():
    event = _email_event(
        processing_state="FINALIZED",
        outcome_status="REJECTED_TOO_COMPLEX",
        reject_reason="   ",
        handoff_status="BLOCKED",
    )
    with pytest.raises(InvalidHandoffState, match="reject_reason"):
        validate_email_event_handoff_invariants(event)


def test_rejected_unmatched_requires_unmatched():
    event = _email_event(
        processing_state="FINALIZED",
        outcome_status="REJECTED_UNMATCHED",
        match_status="MATCHED",
        supplier_id="s",
        matched_rule_id="r",
        reject_reason="no match",
        handoff_status="BLOCKED",
    )
    with pytest.raises(InvalidHandoffState, match="UNMATCHED"):
        validate_email_event_handoff_invariants(event)


def test_rejected_ambiguous_requires_ambiguous():
    event = _email_event(
        processing_state="FINALIZED",
        outcome_status="REJECTED_AMBIGUOUS",
        match_status="MATCHED",
        supplier_id="s",
        matched_rule_id="r",
        reject_reason="ambiguous",
        handoff_status="BLOCKED",
    )
    with pytest.raises(InvalidHandoffState, match="AMBIGUOUS"):
        validate_email_event_handoff_invariants(event)


def test_rejected_no_carrier_requires_none():
    event = _email_event(
        processing_state="FINALIZED",
        outcome_status="REJECTED_NO_CARRIER",
        match_status="MATCHED",
        supplier_id="s",
        matched_rule_id="r",
        carrier_strategy="ATTACHMENT",
        selected_attachment_index=0,
        reject_reason="no carrier",
        handoff_status="BLOCKED",
    )
    with pytest.raises(InvalidHandoffState, match="NONE"):
        validate_email_event_handoff_invariants(event)


def test_unknown_processing_state():
    event = _email_event(processing_state="GARBAGE")
    with pytest.raises(InvalidHandoffState, match="Unknown processing_state"):
        validate_email_event_handoff_invariants(event)


def test_unknown_outcome_status():
    event = _email_event(
        processing_state="FINALIZED",
        outcome_status="GARBAGE",
        handoff_status="BLOCKED",
    )
    with pytest.raises(InvalidHandoffState, match="Unknown outcome_status"):
        validate_email_event_handoff_invariants(event)


def test_handoff_row_ready_without_supplier():
    h = _handoff(handoff_status="READY", supplier_id=None)
    with pytest.raises(InvalidHandoffState, match="supplier_id"):
        validate_handoff_row_invariants(h)


def test_handoff_row_ready_without_payload():
    h = _handoff(handoff_status="READY", handoff_payload_ref=None)
    with pytest.raises(InvalidHandoffState, match="handoff_payload_ref"):
        validate_handoff_row_invariants(h)


def test_handoff_row_ready_with_none_strategy():
    h = _handoff(
        handoff_status="READY",
        carrier_strategy="NONE",
        carrier_kind="none",
        carrier_attachment_index=None,
        carrier_url_index=None,
        carrier_path=None,
        carrier_url_ref=None,
    )
    with pytest.raises(InvalidHandoffState, match="NONE"):
        validate_handoff_row_invariants(h)


def test_handoff_row_attachment_without_kind():
    h = _handoff(carrier_strategy="ATTACHMENT", carrier_kind="url")
    with pytest.raises(InvalidHandoffState, match="carrier_kind"):
        validate_handoff_row_invariants(h)


def test_handoff_row_negative_attachment_index():
    h = _handoff(carrier_attachment_index=-1)
    with pytest.raises(InvalidHandoffState, match=">= 0"):
        validate_handoff_row_invariants(h)


def test_handoff_row_unknown_carrier_kind():
    h = _handoff(carrier_strategy="ATTACHMENT", carrier_kind="GARBAGE")
    with pytest.raises(InvalidHandoffState, match="Unknown carrier_kind"):
        validate_handoff_row_invariants(h)


def test_handoff_row_unknown_handoff_status():
    h = _handoff(handoff_status="GARBAGE")
    with pytest.raises(InvalidHandoffState, match="Unknown handoff row status"):
        validate_handoff_row_invariants(h)


def test_acked_requires_processed_at():
    event = _email_event(seen_status="ACKED", processed_at=None)
    with pytest.raises(InvalidSeenState, match="processed_at"):
        validate_email_event_seen_invariants(event)


def test_uidvalidity_zero():
    event = _email_event(uidvalidity=0)
    with pytest.raises(InvalidSeenState, match="uidvalidity"):
        validate_email_event_seen_invariants(event)


def test_uid_zero():
    event = _email_event(uid=0)
    with pytest.raises(InvalidSeenState, match="uid"):
        validate_email_event_seen_invariants(event)


def test_unknown_seen_status():
    event = _email_event(seen_status="GARBAGE")
    with pytest.raises(InvalidSeenState, match="Unknown seen_status"):
        validate_email_event_seen_invariants(event)


def test_seen_outbox_uidvalidity_zero():
    event = _email_event()
    seen = _seen_outbox(event, uidvalidity=0)
    with pytest.raises(InvalidSeenState, match="uidvalidity"):
        validate_seen_outbox_invariants(seen, event)


def test_seen_outbox_uid_zero():
    event = _email_event()
    seen = _seen_outbox(event, uid=0)
    with pytest.raises(InvalidSeenState, match="uid"):
        validate_seen_outbox_invariants(seen, event)


def test_seen_outbox_negative_attempt():
    event = _email_event()
    seen = _seen_outbox(event, attempt_count=-1)
    with pytest.raises(InvalidSeenState, match="attempt_count"):
        validate_seen_outbox_invariants(seen, event)


def test_seen_outbox_confirmed_before_attempted():
    event = _email_event()
    seen = _seen_outbox(event, attempted_at=_TS2, confirmed_at=_TS)
    with pytest.raises(InvalidSeenState, match="confirmed_at"):
        validate_seen_outbox_invariants(seen, event)


def test_seen_outbox_confirmed_without_acked():
    event = _email_event(seen_status="PENDING")
    seen = _seen_outbox(event, confirmed_at=_TS2)
    with pytest.raises(InvalidSeenState, match="ACKED"):
        validate_seen_outbox_invariants(seen, event)


def test_seen_outbox_event_id_mismatch():
    event = _email_event()
    seen = _seen_outbox(event, event_id="other-id")
    with pytest.raises(InvalidSeenState, match="event_id"):
        validate_seen_outbox_invariants(seen, event)


def test_seen_outbox_mailbox_mismatch():
    event = _email_event()
    seen = _seen_outbox(event, mailbox_name="OTHER")
    with pytest.raises(InvalidSeenState, match="mailbox_name"):
        validate_seen_outbox_invariants(seen, event)


def test_seen_outbox_uid_mismatch():
    event = _email_event()
    seen = _seen_outbox(event, uid=9999)
    with pytest.raises(InvalidSeenState, match="uid"):
        validate_seen_outbox_invariants(seen, event)


def test_aggregator_passes_valid_event():
    event = _email_event()
    validate_email_event_invariants(event)


def test_aggregator_catches_invalid_event():
    event = _email_event(match_status="MATCHED", supplier_id=None)
    with pytest.raises(InvalidMatchResolution):
        validate_email_event_invariants(event)
