"""Application-layer orchestration for Layer A mail intake.

This package must stay thin:
- no SQLAlchemy session ownership
- no database-specific repository implementation
- no duplicated domain rules
- no downstream parsing/materialization logic
"""

from .email_event_service import (
    ApplicationContractError,
    EmailEventApplicationService,
    EmailEventNotFound,
)
from .ports import EmailEventRepository

__all__ = [
    "ApplicationContractError",
    "EmailEventApplicationService",
    "EmailEventNotFound",
    "EmailEventRepository",
]
