"""Application service for Layer A EmailEvent orchestration.

Boundary rule:
- Domain modules own invariants and transition validity.
- Application service owns orchestration order.
- Infrastructure owns sessions, transactions, locks, retries and SQL.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from layer_a_r4.domain.email_event_invariants import validate_email_event_invariants
from layer_a_r4.domain.email_event_transitions import validate_email_event_transition
from layer_a_r4.models.email_event import EmailEvent

from .ports import EmailEventRepository


class EmailEventNotFound(LookupError):
    """Raised when application orchestration cannot load an EmailEvent."""


class ApplicationContractError(RuntimeError):
    """Raised when a caller violates application-layer call contract."""


TransitionBuilder = Callable[[EmailEvent], EmailEvent]


@dataclass(frozen=True, slots=True)
class EmailEventApplicationService:
    """Thin orchestration around EmailEvent domain guards."""

    repository: EmailEventRepository

    def register_email_event(self, event: EmailEvent) -> EmailEvent:
        """Validate and persist a newly captured EmailEvent.

        This method is intentionally narrow: it does not synthesize IDs,
        pull mail, write evidence, select carriers or publish handoffs.
        """
        validate_email_event_invariants(event)
        self.repository.save(event)
        return event

    def transition_email_event(
        self,
        event_id: str,
        build_after: TransitionBuilder,
    ) -> EmailEvent:
        """Load current event, build next state, validate transition, save.

        build_after must return a distinct object. In-place mutation destroys
        the before/after evidence boundary required by transition guards.
        """
        before = self.repository.get_by_event_id(event_id)
        if before is None:
            raise EmailEventNotFound(f"EmailEvent not found: {event_id}")

        after = build_after(before)
        if after is before:
            raise ApplicationContractError(
                "build_after must return a distinct EmailEvent instance"
            )

        validate_email_event_transition(before, after)
        self.repository.save(after)
        return after
