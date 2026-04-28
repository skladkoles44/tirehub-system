from __future__ import annotations

from datetime import datetime, timezone

import pytest

from layer_a_r4.application.email_event_service import (
    ApplicationContractError,
    EmailEventApplicationService,
    EmailEventNotFound,
)
from layer_a_r4.domain.exceptions import InvalidHandoffState
from layer_a_r4.models.email_event import EmailEvent
from layer_a_r4.models.enums import (
    EmailEventHandoffStatus,
    ProcessingState,
    SeenStatus,
)


class FakeEmailEventRepository:
    def __init__(self, *events: EmailEvent) -> None:
        self.events = {event.event_id: event for event in events}
        self.saved: list[EmailEvent] = []

    def get_by_event_id(self, event_id: str) -> EmailEvent | None:
        return self.events.get(event_id)

    def save(self, event: EmailEvent) -> None:
        self.saved.append(event)
        self.events[event.event_id] = event


def _value(value):
    return value.value if hasattr(value, "value") else value


def make_event(
    *,
    event_id: str = "evt-1",
    uid: int = 10,
    uidvalidity: int = 1,
    processing_state=ProcessingState.CAPTURED,
    handoff_status=EmailEventHandoffStatus.NOT_CREATED,
    seen_status=SeenStatus.NOT_REQUESTED,
    **overrides,
) -> EmailEvent:
    now = datetime(2026, 4, 28, 0, 0, tzinfo=timezone.utc)
    values = {
        "event_id": event_id,
        "mailbox_name": "INBOX",
        "uidvalidity": uidvalidity,
        "uid": uid,
        "natural_key": f"INBOX:{uidvalidity}:{uid}",
        "processing_state": _value(processing_state),
        "handoff_status": _value(handoff_status),
        "seen_status": _value(seen_status),
        "match_status": None,
        "carrier_strategy": None,
        "outcome_status": None,
        "supplier_id": None,
        "matched_rule_id": None,
        "candidate_supplier_ids": None,
        "candidate_rule_ids": None,
        "selected_attachment_index": None,
        "selected_url_index": None,
        "reject_reason": None,
        "reject_detail": None,
        "received_at": now,
        "first_seen_at": now,
        "processed_at": None,
    }
    values.update(overrides)
    return EmailEvent(**values)


def test_register_email_event_validates_and_saves():
    event = make_event()
    repo = FakeEmailEventRepository()
    service = EmailEventApplicationService(repo)

    result = service.register_email_event(event)

    assert result is event
    assert repo.saved == [event]
    assert repo.events[event.event_id] is event


def test_register_email_event_rejects_invalid_event_without_save():
    event = make_event(uidvalidity=0, natural_key="INBOX:0:10")
    repo = FakeEmailEventRepository()
    service = EmailEventApplicationService(repo)

    with pytest.raises(Exception):
        service.register_email_event(event)

    assert repo.saved == []


def test_transition_loads_validates_and_saves_distinct_after_object():
    before = make_event(processing_state=ProcessingState.CAPTURED)
    after = make_event(processing_state=ProcessingState.EVIDENCE_STAGED)
    repo = FakeEmailEventRepository(before)
    service = EmailEventApplicationService(repo)

    result = service.transition_email_event(before.event_id, lambda current: after)

    assert result is after
    assert repo.saved == [after]
    assert repo.events[before.event_id] is after


def test_transition_rejects_missing_event_without_save():
    repo = FakeEmailEventRepository()
    service = EmailEventApplicationService(repo)

    with pytest.raises(EmailEventNotFound):
        service.transition_email_event("missing", lambda current: current)

    assert repo.saved == []


def test_transition_rejects_in_place_mutation_without_save():
    before = make_event(processing_state=ProcessingState.CAPTURED)
    repo = FakeEmailEventRepository(before)
    service = EmailEventApplicationService(repo)

    def mutate_in_place(current: EmailEvent) -> EmailEvent:
        current.processing_state = ProcessingState.EVIDENCE_STAGED.value
        return current

    with pytest.raises(ApplicationContractError):
        service.transition_email_event(before.event_id, mutate_in_place)

    assert repo.saved == []


def test_transition_delegates_invalid_state_motion_to_domain_guard():
    before = make_event(processing_state=ProcessingState.CAPTURED)
    after = make_event(processing_state=ProcessingState.PARSED)
    repo = FakeEmailEventRepository(before)
    service = EmailEventApplicationService(repo)

    with pytest.raises(InvalidHandoffState):
        service.transition_email_event(before.event_id, lambda current: after)

    assert repo.saved == []
