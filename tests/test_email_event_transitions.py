"""Tests for email_event transition guards.

Rule: negative tests for the **public entrypoint**
validate_email_event_transition() must have both *before* and *after*
as snapshot-valid objects. The only thing being tested is whether the
transition between them is legal.

Full finalized-latch coverage is provided via direct tests of the
private _validate_finalized_latches() helper — this is intentional:
the public entrypoint rejects snapshot-invalid *after* before reaching
the latch check, so invariant-unsafe mutations cannot be tested
through the public API without distorting the test.
"""

from __future__ import annotations

import copy
from datetime import datetime, timezone

import pytest

from layer_a_r4.domain.email_event_transitions import (
    _validate_finalized_latches,
    validate_email_event_transition,
)
from layer_a_r4.domain.exceptions import InvalidHandoffState, InvalidSeenState
from layer_a_r4.models.email_event import EmailEvent


_TS_1 = datetime(2026, 4, 23, 12, 0, 0, tzinfo=timezone.utc)
_TS_2 = datetime(2026, 4, 23, 12, 0, 1, tzinfo=timezone.utc)


def _make_captured(**overrides) -> EmailEvent:
    base = {
        "event_id": "evt-cap",
        "mailbox_name": "INBOX",
        "uidvalidity": 1,
        "uid": 1001,
        "natural_key": "INBOX:1:1001",
        "processing_state": "CAPTURED",
        "outcome_status": None,
        "match_status": None,
        "carrier_strategy": None,
        "handoff_status": "NOT_CREATED",
        "seen_status": "NOT_REQUESTED",
        "supplier_id": None,
        "matched_rule_id": None,
        "selected_attachment_index": None,
        "selected_url_index": None,
        "reject_reason": None,
        "reject_detail": None,
        "candidate_supplier_ids": None,
        "candidate_rule_ids": None,
        "received_at": _TS_1,
        "first_seen_at": _TS_1,
        "processed_at": None,
        "processing_duration_ms": None,
        "updated_at": _TS_1,
    }
    base.update(overrides)
    return EmailEvent(**base)


def _make_match_resolved(**overrides) -> EmailEvent:
    base = {
        "event_id": "evt-mr",
        "mailbox_name": "INBOX",
        "uidvalidity": 2,
        "uid": 2002,
        "natural_key": "INBOX:2:2002",
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
        "received_at": _TS_1,
        "first_seen_at": _TS_1,
        "processed_at": None,
        "processing_duration_ms": None,
        "updated_at": _TS_1,
    }
    base.update(overrides)
    return EmailEvent(**base)


def _make_finalized_ready_attachment(**overrides) -> EmailEvent:
    base = {
        "event_id": "evt-fr",
        "mailbox_name": "INBOX",
        "uidvalidity": 3,
        "uid": 3003,
        "natural_key": "INBOX:3:3003",
        "processing_state": "FINALIZED",
        "outcome_status": "HANDOFF_READY",
        "match_status": "MATCHED",
        "carrier_strategy": "ATTACHMENT",
        "handoff_status": "READY",
        "seen_status": "NOT_REQUESTED",
        "supplier_id": "supplier-42",
        "matched_rule_id": "rule-7",
        "selected_attachment_index": 0,
        "selected_url_index": None,
        "reject_reason": None,
        "reject_detail": None,
        "candidate_supplier_ids": None,
        "candidate_rule_ids": None,
        "received_at": _TS_1,
        "first_seen_at": _TS_1,
        "processed_at": _TS_1,
        "processing_duration_ms": None,
        "updated_at": _TS_1,
    }
    base.update(overrides)
    return EmailEvent(**base)


def _make_finalized_ready_url(**overrides) -> EmailEvent:
    base = {
        "event_id": "evt-fu",
        "mailbox_name": "INBOX",
        "uidvalidity": 4,
        "uid": 4004,
        "natural_key": "INBOX:4:4004",
        "processing_state": "FINALIZED",
        "outcome_status": "HANDOFF_READY",
        "match_status": "MATCHED",
        "carrier_strategy": "BODY_LINK",
        "handoff_status": "READY",
        "seen_status": "NOT_REQUESTED",
        "supplier_id": "supplier-42",
        "matched_rule_id": "rule-7",
        "selected_attachment_index": None,
        "selected_url_index": 0,
        "reject_reason": None,
        "reject_detail": None,
        "candidate_supplier_ids": None,
        "candidate_rule_ids": None,
        "received_at": _TS_1,
        "first_seen_at": _TS_1,
        "processed_at": _TS_1,
        "processing_duration_ms": None,
        "updated_at": _TS_1,
    }
    base.update(overrides)
    return EmailEvent(**base)


def _make_finalized_rejected_blocked(**overrides) -> EmailEvent:
    base = {
        "event_id": "evt-rj",
        "mailbox_name": "INBOX",
        "uidvalidity": 5,
        "uid": 5005,
        "natural_key": "INBOX:5:5005",
        "processing_state": "FINALIZED",
        "outcome_status": "REJECTED_TOO_COMPLEX",
        "match_status": None,
        "carrier_strategy": "NONE",
        "handoff_status": "BLOCKED",
        "seen_status": "NOT_REQUESTED",
        "supplier_id": None,
        "matched_rule_id": None,
        "selected_attachment_index": None,
        "selected_url_index": None,
        "reject_reason": "too complex",
        "reject_detail": None,
        "candidate_supplier_ids": None,
        "candidate_rule_ids": None,
        "received_at": _TS_1,
        "first_seen_at": _TS_1,
        "processed_at": _TS_1,
        "processing_duration_ms": None,
        "updated_at": _TS_1,
    }
    base.update(overrides)
    return EmailEvent(**base)


def test_rejects_uid_mutation():
    before = _make_captured()
    after = copy.deepcopy(before)
    after.uid = 9999
    with pytest.raises(InvalidHandoffState, match="uid is immutable"):
        validate_email_event_transition(before, after)


def test_rejects_natural_key_mutation():
    before = _make_captured()
    after = copy.deepcopy(before)
    after.natural_key = "OTHER:1:1001"
    with pytest.raises(InvalidHandoffState, match="natural_key is immutable"):
        validate_email_event_transition(before, after)


def test_rejects_backward():
    before = _make_match_resolved(processing_state="EVIDENCE_COMMITTED")
    after = copy.deepcopy(before)
    after.processing_state = "EVIDENCE_STAGED"
    with pytest.raises(InvalidHandoffState, match="cannot move backward"):
        validate_email_event_transition(before, after)


def test_rejects_skip():
    before = _make_captured()
    after = copy.deepcopy(before)
    after.processing_state = "PARSED"
    with pytest.raises(InvalidHandoffState, match="cannot skip phases"):
        validate_email_event_transition(before, after)


def test_rejects_not_created_to_published():
    before = _make_match_resolved()
    after = _make_finalized_ready_attachment(
        handoff_status="PUBLISHED",
        event_id=before.event_id,
        mailbox_name=before.mailbox_name,
        uidvalidity=before.uidvalidity,
        uid=before.uid,
        natural_key=before.natural_key,
    )
    with pytest.raises(InvalidHandoffState, match="Illegal handoff_status transition"):
        validate_email_event_transition(before, after)


def test_rejects_published_to_ready():
    before = _make_finalized_ready_attachment(handoff_status="PUBLISHED")
    after = copy.deepcopy(before)
    after.handoff_status = "READY"
    with pytest.raises(InvalidHandoffState, match="Illegal handoff_status transition"):
        validate_email_event_transition(before, after)


def test_rejects_acked_to_pending():
    before = _make_finalized_ready_attachment(seen_status="ACKED")
    after = copy.deepcopy(before)
    after.seen_status = "PENDING"
    with pytest.raises(InvalidSeenState, match="seen_status cannot regress after ACKED"):
        validate_email_event_transition(before, after)


def test_rejects_processed_at_nullification():
    before = _make_finalized_ready_attachment(processed_at=_TS_1)
    after = copy.deepcopy(before)
    after.processed_at = None
    with pytest.raises(InvalidSeenState, match="processed_at cannot move from non-NULL to NULL"):
        validate_email_event_transition(before, after)


def test_rejects_processed_at_backward():
    before = _make_finalized_ready_attachment(processed_at=_TS_2)
    after = copy.deepcopy(before)
    after.processed_at = _TS_1
    with pytest.raises(InvalidSeenState, match="processed_at cannot move backward"):
        validate_email_event_transition(before, after)


_READY_ATTACHMENT_LATCH_FIELDS = [
    "supplier_id",
    "matched_rule_id",
    "candidate_supplier_ids",
    "candidate_rule_ids",
    "selected_attachment_index",
]
_MUT_READY_ATTACHMENT = {
    "supplier_id": "other-supplier",
    "matched_rule_id": "other-rule",
    "candidate_supplier_ids": [],
    "candidate_rule_ids": [],
    "selected_attachment_index": 99,
}


@pytest.mark.parametrize("field_name", _READY_ATTACHMENT_LATCH_FIELDS, ids=_READY_ATTACHMENT_LATCH_FIELDS)
def test_finalized_latch_ready_attachment(field_name):
    before = _make_finalized_ready_attachment()
    after = copy.deepcopy(before)
    setattr(after, field_name, _MUT_READY_ATTACHMENT[field_name])
    with pytest.raises(InvalidHandoffState, match=f"{field_name} is immutable"):
        validate_email_event_transition(before, after)


@pytest.mark.parametrize("field_name", ["selected_url_index"], ids=["selected_url_index"])
def test_finalized_latch_ready_url(field_name):
    before = _make_finalized_ready_url()
    after = copy.deepcopy(before)
    setattr(after, field_name, 99)
    with pytest.raises(InvalidHandoffState, match=f"{field_name} is immutable"):
        validate_email_event_transition(before, after)


_REJECTED_LATCH_FIELDS = ["outcome_status", "reject_reason", "reject_detail"]
_MUT_REJECTED = {
    "outcome_status": "REJECTED_RELEVANT_PARTS_LIMIT",
    "reject_reason": "changed",
    "reject_detail": "changed",
}


@pytest.mark.parametrize("field_name", _REJECTED_LATCH_FIELDS, ids=_REJECTED_LATCH_FIELDS)
def test_finalized_latch_rejected(field_name):
    before = _make_finalized_rejected_blocked()
    after = copy.deepcopy(before)
    setattr(after, field_name, _MUT_REJECTED[field_name])
    with pytest.raises(InvalidHandoffState, match=f"{field_name} is immutable"):
        validate_email_event_transition(before, after)


_ALL_LATCH_FIELDS = [
    "match_status",
    "supplier_id",
    "matched_rule_id",
    "candidate_supplier_ids",
    "candidate_rule_ids",
    "carrier_strategy",
    "selected_attachment_index",
    "selected_url_index",
    "outcome_status",
    "reject_reason",
    "reject_detail",
]
_MUT_ALL = {
    "match_status": "UNMATCHED",
    "supplier_id": "other",
    "matched_rule_id": "other",
    "candidate_supplier_ids": [],
    "candidate_rule_ids": [],
    "carrier_strategy": "BODY_LINK",
    "selected_attachment_index": 99,
    "selected_url_index": 99,
    "outcome_status": "REJECTED_TOO_COMPLEX",
    "reject_reason": "mut",
    "reject_detail": "mut",
}


@pytest.mark.parametrize("field_name", _ALL_LATCH_FIELDS, ids=_ALL_LATCH_FIELDS)
def test_finalized_latch_helper_full(field_name):
    before = _make_finalized_ready_attachment()
    after = copy.deepcopy(before)
    setattr(after, field_name, _MUT_ALL[field_name])
    with pytest.raises(InvalidHandoffState, match=f"{field_name} is immutable"):
        _validate_finalized_latches(before, after)


def test_allows_match_resolved_to_finalized_handoff_ready():
    before = _make_match_resolved()
    after = copy.deepcopy(before)
    after.processing_state = "FINALIZED"
    after.outcome_status = "HANDOFF_READY"
    after.handoff_status = "READY"
    after.processed_at = _TS_1
    validate_email_event_transition(before, after)


def test_allows_finalized_ready_to_published():
    before = _make_finalized_ready_attachment(handoff_status="READY")
    after = copy.deepcopy(before)
    after.handoff_status = "PUBLISHED"
    validate_email_event_transition(before, after)
