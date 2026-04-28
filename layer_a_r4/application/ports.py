"""Application ports for Layer A.

Ports define what the application service needs from infrastructure.
Concrete SQLAlchemy/session-backed implementations belong outside this
module.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from layer_a_r4.models.email_event import EmailEvent


@runtime_checkable
class EmailEventRepository(Protocol):
    """Persistence port for EmailEvent application orchestration."""

    def get_by_event_id(self, event_id: str) -> EmailEvent | None:
        """Return one EmailEvent by stable event_id, or None if absent."""
        ...

    def save(self, event: EmailEvent) -> None:
        """Persist an EmailEvent state produced by the application service."""
        ...
