"""Domain invariant violation exceptions for Layer A."""

from __future__ import annotations


class InvariantViolation(Exception):
    """Base exception for all invariant violations in Layer A."""


class InvalidProcessingStateTransition(InvariantViolation):
    """Недопустимый переход processing_state."""


class InvalidMatchResolution(InvariantViolation):
    """Нарушение правил match_status coherence."""


class InvalidCarrierSelection(InvariantViolation):
    """Нарушение правил carrier_selection coherence."""


class InvalidHandoffState(InvariantViolation):
    """Нарушение правил handoff/outcome coherence."""


class InvalidSeenState(InvariantViolation):
    """Нарушение правил seen_status coherence."""
