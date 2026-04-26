"""Pure transition guards for Layer A EmailEvent state changes.

No database access. No side effects. Operates on two ORM model instances:
before -> after.

Scope:
- processing_state same-step or next-step progression
- immutable identity fields: event_id, mailbox_name, uidvalidity, uid,
  natural_key
- finalized-state latches for terminal payload fields
- email_event handoff_status FSM (summary level)
- ACKED seen_status latch + processed_at monotonicity

This file does NOT implement a full seen_status FSM. Only the ACKED
latch and processed_at time-motion are enforced. Additional seen
regression rules belong to a separate lifecycle policy.

Out of scope for this file:
- SeenOutbox lifecycle policy
- Handoff forensic cleanliness outside EmailEvent row
"""

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

from layer_a_r4.models.enums import (
    EmailEventHandoffStatus,
    ProcessingState,
    SeenStatus,
)

from .email_event_invariants import validate_email_event_invariants
from .exceptions import InvalidHandoffState, InvalidSeenState

if TYPE_CHECKING:
    from layer_a_r4.models.email_event import EmailEvent


def _enum_value(value: str | Enum | None) -> str | None:
    """Normalize Enum/StrEnum member or plain string to its string value."""
    if value is None:
        return None
    return value.value if isinstance(value, Enum) else value


def _require_same(
    before: EmailEvent,
    after: EmailEvent,
    field_name: str,
    exc_type: type[Exception] = InvalidHandoffState,
) -> None:
    before_value = getattr(before, field_name)
    after_value = getattr(after, field_name)
    if before_value != after_value:
        raise exc_type(
            f"{field_name} is immutable in this transition: "
            f"before={before_value!r}, after={after_value!r}"
        )


_PROCESSING_STATE_ORDER: dict[str, int] = {
    ProcessingState.CAPTURED.value: 0,
    ProcessingState.EVIDENCE_STAGED.value: 1,
    ProcessingState.EVIDENCE_COMMITTED.value: 2,
    ProcessingState.PARSED.value: 3,
    ProcessingState.MATCH_RESOLVED.value: 4,
    ProcessingState.FINALIZED.value: 5,
}

_ALLOWED_HANDOFF_TRANSITIONS: dict[str, frozenset[str]] = {
    EmailEventHandoffStatus.NOT_CREATED.value: frozenset({
        EmailEventHandoffStatus.NOT_CREATED.value,
        EmailEventHandoffStatus.READY.value,
        EmailEventHandoffStatus.BLOCKED.value,
    }),
    EmailEventHandoffStatus.READY.value: frozenset({
        EmailEventHandoffStatus.READY.value,
        EmailEventHandoffStatus.PUBLISHED.value,
    }),
    EmailEventHandoffStatus.PUBLISHED.value: frozenset({
        EmailEventHandoffStatus.PUBLISHED.value,
    }),
    EmailEventHandoffStatus.BLOCKED.value: frozenset({
        EmailEventHandoffStatus.BLOCKED.value,
    }),
}


def _validate_identity_immutability(before: EmailEvent, after: EmailEvent) -> None:
    _require_same(before, after, "event_id")
    _require_same(before, after, "mailbox_name")
    _require_same(before, after, "uidvalidity")
    _require_same(before, after, "uid")
    _require_same(before, after, "natural_key")


def _validate_processing_state_transition(before: EmailEvent, after: EmailEvent) -> None:
    before_ps = _enum_value(before.processing_state)
    after_ps = _enum_value(after.processing_state)

    if before_ps not in _PROCESSING_STATE_ORDER:
        raise InvalidHandoffState(f"Unknown before.processing_state: {before_ps}")
    if after_ps not in _PROCESSING_STATE_ORDER:
        raise InvalidHandoffState(f"Unknown after.processing_state: {after_ps}")

    before_rank = _PROCESSING_STATE_ORDER[before_ps]
    after_rank = _PROCESSING_STATE_ORDER[after_ps]

    if after_rank < before_rank:
        raise InvalidHandoffState(
            f"processing_state cannot move backward: {before_ps} -> {after_ps}"
        )

    if after_rank - before_rank > 1:
        raise InvalidHandoffState(
            f"processing_state cannot skip phases: {before_ps} -> {after_ps}"
        )


def _validate_finalized_latches(before: EmailEvent, after: EmailEvent) -> None:
    before_ps = _enum_value(before.processing_state)
    if before_ps != ProcessingState.FINALIZED.value:
        return

    for field_name in (
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
    ):
        _require_same(before, after, field_name, InvalidHandoffState)


def _validate_handoff_status_transition(before: EmailEvent, after: EmailEvent) -> None:
    before_hs = _enum_value(before.handoff_status)
    after_hs = _enum_value(after.handoff_status)

    allowed_targets = _ALLOWED_HANDOFF_TRANSITIONS.get(before_hs)
    if allowed_targets is None:
        raise InvalidHandoffState(f"Unknown before.handoff_status: {before_hs}")

    if after_hs not in allowed_targets:
        raise InvalidHandoffState(
            f"Illegal handoff_status transition: {before_hs} -> {after_hs}"
        )


def _validate_seen_transition(before: EmailEvent, after: EmailEvent) -> None:
    before_ss = _enum_value(before.seen_status)
    after_ss = _enum_value(after.seen_status)

    if before_ss == SeenStatus.ACKED.value and after_ss != SeenStatus.ACKED.value:
        raise InvalidSeenState(
            f"seen_status cannot regress after ACKED: {before_ss} -> {after_ss}"
        )

    if before.processed_at is not None:
        if after.processed_at is None:
            raise InvalidSeenState(
                "processed_at cannot move from non-NULL to NULL"
            )
        if after.processed_at < before.processed_at:
            raise InvalidSeenState(
                f"processed_at cannot move backward: "
                f"{before.processed_at} -> {after.processed_at}"
            )


def validate_email_event_transition(before: EmailEvent, after: EmailEvent) -> None:
    """Validate a single EmailEvent state transition: before -> after."""
    validate_email_event_invariants(before)
    validate_email_event_invariants(after)

    _validate_identity_immutability(before, after)
    _validate_processing_state_transition(before, after)
    _validate_handoff_status_transition(before, after)
    _validate_seen_transition(before, after)
    _validate_finalized_latches(before, after)
