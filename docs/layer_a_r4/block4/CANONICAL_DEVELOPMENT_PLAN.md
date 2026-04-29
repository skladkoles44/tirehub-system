КАНОНИЧЕСКИЙ ПЛАН РАЗРАБОТКИ

Последовательность патчей

```
Блок 4: Доменные сервисы и репозитории
├── Миграция block4_hash_chaining (перед P2)
├── Patch 2: TransitionLogService, ErrorLogService, EvidenceService
├── Patch 3: HandoffService
├── Patch 4: SeenService
├── Patch 5: SQLAlchemy репозитории
└── Patch 6: Интеграционные тесты

Блок 5: IMAP Capture Adapter
Блок 6: Evidence Materialization
Блок 7: Parse / Match / Carrier
```

Ключевые решения

Решение Значение
Hash algorithm HMAC-SHA256
Hash key management keyring Dict[str, bytes] + key_id, ключ вне домена
Terminal statuses FINALIZED, DEAD_LETTER
Supplier matching fallback UNMATCHED
Handoff precondition Только из статуса HANDOFF
Seen trigger При любом терминальном статусе
Retry strategy 3 попытки на hash-chain и optimistic lock конфликты
Entity immutability frozen=True + рекурсивный deep_freeze через domain/freeze.py
Validation layer Command DTO с нормализацией в post_init
Application-Infrastructure boundary ConstraintConflict вместо прямого SQLAlchemy IntegrityError

Структура пакетов

```
domain/
  __init__.py
  exceptions.py
  freeze.py
  entities/
    __init__.py
    email_event.py
    event_transition_log.py
    error_log.py
    evidence.py
    handoff.py
    seen_acknowledgment.py

application/
  __init__.py
  exceptions.py
  commands.py
  email_event_application_service.py
  ports/
    __init__.py
    event_repository.py
    transition_log_repository.py
    error_log_repository.py
    evidence_repository.py
    handoff_repository.py
    seen_repository.py
    unit_of_work.py
    error_logger.py
  services/
    __init__.py
    transition_hash_service.py
    transition_log_service.py
    error_log_service.py
    evidence_service.py
    handoff_service.py
    seen_service.py

infrastructure/
  __init__.py
  persistence/
    __init__.py
    models.py
    repositories.py
    unit_of_work.py
    transactional_error_logger.py

db/
  migrations/
    20250128120000_block4_hash_chaining.py

tests/
  conftest.py
  unit/
    domain/
      entities/
        test_email_event.py
        test_event_transition_log.py
    application/
      test_commands.py
      services/
        test_transition_hash_service.py
        test_transition_log_service.py
  integration/
    conftest.py
    test_repositories.py
    test_email_event_application_service.py
    test_unit_of_work.py
    test_migration_idempotency.py
```

Статус шаблонов

Шаблон Статус
Repository (порт) ✅
Repository (фейк/InMemory) ✅
Repository (SQLAlchemy) ✅
Unit of Work ✅
Command DTO ✅
Application Service ✅
Transactional Outbox ✅
Outbox Relay 🔲 Блок 5
Idempotency Key ✅
Hash Chain ✅
Polling 🔲 Блок 5
Materialized Evidence 🔲 Блок 6
Chain of Responsibility 🔲 Блок 7
Fallback ✅

---

DOMAIN LAYER

domain/freeze.py

```python
from copy import deepcopy
from types import MappingProxyType

def deep_freeze(value):
    if isinstance(value, dict):
        return MappingProxyType({k: deep_freeze(v) for k, v in deepcopy(value).items()})
    if isinstance(value, (list, tuple)):
        return tuple(deep_freeze(v) for v in value)
    return value

def defrost(value):
    if isinstance(value, MappingProxyType):
        return {k: defrost(v) for k, v in value.items()}
    if isinstance(value, tuple):
        return [defrost(v) for v in value]
    return value
```

domain/exceptions.py

```python
class DomainException(Exception):
    pass

class InvariantViolation(DomainException):
    pass

class IntegrityViolation(InvariantViolation):
    pass

class InvalidHandoffCreation(InvariantViolation):
    pass

class InvalidSeenOperation(InvariantViolation):
    pass

class InvalidStateTransition(InvariantViolation):
    pass

class EventNotFound(DomainException):
    pass

class ConcurrencyConflict(InvariantViolation):
    pass

class MisconfigurationError(DomainException):
    pass

class UnsupportedHashAlgorithm(InvariantViolation):
    pass

class UnsupportedHashVersion(InvariantViolation):
    pass
```

domain/entities/event_transition_log.py

```python
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional, Dict, Any
import json
import re
from domain.exceptions import InvariantViolation, UnsupportedHashAlgorithm, UnsupportedHashVersion
from domain.freeze import deep_freeze, defrost

class HashAlgorithm(str, Enum):
    HMAC_SHA256 = "HMAC-SHA256"
    HMAC_SHA512 = "HMAC-SHA512"

HASH_LENGTHS = {HashAlgorithm.HMAC_SHA256: 64, HashAlgorithm.HMAC_SHA512: 128}
SUPPORTED_HASH_VERSIONS = {1}

class ChainVerificationStatus(str, Enum):
    VALID = "VALID"
    EMPTY = "EMPTY"
    BROKEN_HASH = "BROKEN_HASH"
    BROKEN_LINK = "BROKEN_LINK"
    EVENT_NOT_FOUND = "EVENT_NOT_FOUND"
    UNSUPPORTED_HASH_VERSION = "UNSUPPORTED_HASH_VERSION"
    UNSUPPORTED_HASH_ALGORITHM = "UNSUPPORTED_HASH_ALGORITHM"

@dataclass(frozen=True)
class EventTransitionLog:
    email_event_id: str
    from_status: str
    to_status: str
    sequence_no: int
    prev_hash: Optional[str]
    row_hash: str = field(compare=False, hash=False)
    reason: str = ""
    extra: Dict[str, Any] = field(default_factory=dict, compare=False, hash=False)
    id: Optional[str] = field(default=None, compare=False, hash=False)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc).replace(microsecond=0), compare=False, hash=False)
    hash_algorithm: HashAlgorithm = field(default=HashAlgorithm.HMAC_SHA256, compare=False, hash=False)
    hash_version: int = field(default=1, compare=False, hash=False)
    key_id: str = field(default="", compare=False, hash=False)

    def __post_init__(self):
        if not isinstance(self.email_event_id, str) or not self.email_event_id.strip():
            raise InvariantViolation("email_event_id must be non-empty string")
        if not isinstance(self.from_status, str) or not self.from_status.strip():
            raise InvariantViolation("from_status must be non-empty string")
        if not isinstance(self.to_status, str) or not self.to_status.strip():
            raise InvariantViolation("to_status must be non-empty string")
        if not isinstance(self.key_id, str) or not self.key_id.strip():
            raise InvariantViolation("key_id must be non-empty string")
        if not isinstance(self.reason, str):
            raise InvariantViolation("reason must be string")
        if not isinstance(self.sequence_no, int):
            raise InvariantViolation("sequence_no must be int")
        if not isinstance(self.hash_version, int):
            raise InvariantViolation("hash_version must be int")
        if not isinstance(self.row_hash, str):
            raise InvariantViolation("row_hash must be string")
        if self.prev_hash is not None and not isinstance(self.prev_hash, str):
            raise InvariantViolation("prev_hash must be string or None")
        if not isinstance(self.created_at, datetime):
            raise InvariantViolation("created_at must be datetime")
        if self.created_at.tzinfo is None:
            raise InvariantViolation("created_at must be timezone-aware")
        normalized = self.created_at.astimezone(timezone.utc).replace(microsecond=0)
        if normalized != self.created_at:
            object.__setattr__(self, 'created_at', normalized)
        if self.sequence_no < 1:
            raise InvariantViolation("sequence_no must be >= 1")
        if self.sequence_no == 1 and self.prev_hash is not None:
            raise InvariantViolation("First transition must have prev_hash=None")
        if self.sequence_no > 1 and self.prev_hash is None:
            raise InvariantViolation("Non-first transition must have prev_hash")
        if not isinstance(self.hash_algorithm, HashAlgorithm):
            try:
                object.__setattr__(self, 'hash_algorithm', HashAlgorithm(self.hash_algorithm))
            except ValueError as e:
                raise UnsupportedHashAlgorithm(f"Unsupported hash algorithm: {self.hash_algorithm!r}") from e
        expected_len = HASH_LENGTHS.get(self.hash_algorithm)
        if expected_len is None:
            raise UnsupportedHashAlgorithm(f"Unknown hash algorithm: {self.hash_algorithm}")
        if not re.fullmatch(rf"[0-9a-f]{{{expected_len}}}", self.row_hash):
            raise InvariantViolation(f"row_hash must be lowercase hex digest of length {expected_len}")
        if self.prev_hash is not None and not re.fullmatch(r"[0-9a-f]{64}|[0-9a-f]{128}", self.prev_hash):
            raise InvariantViolation("prev_hash must be lowercase hex digest of length 64 or 128")
        if self.hash_version not in SUPPORTED_HASH_VERSIONS:
            raise UnsupportedHashVersion(f"Unsupported hash version: {self.hash_version}")
        frozen_extra = deep_freeze(self.extra or {})
        try:
            json.dumps(defrost(frozen_extra), sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        except TypeError as e:
            raise InvariantViolation("extra must be JSON-serializable") from e
        object.__setattr__(self, 'extra', frozen_extra)

    @classmethod
    def create(cls, email_event_id, from_status, to_status, sequence_no, row_hash, *, reason="", extra=None, prev_hash=None, id=None, created_at=None, hash_algorithm=HashAlgorithm.HMAC_SHA256, hash_version=1, key_id=""):
        return cls(email_event_id=email_event_id, from_status=from_status, to_status=to_status, sequence_no=sequence_no, reason=reason, extra=extra or {}, id=id, created_at=created_at or datetime.now(timezone.utc).replace(microsecond=0), prev_hash=prev_hash, row_hash=row_hash, hash_algorithm=hash_algorithm, hash_version=hash_version, key_id=key_id)

    @classmethod
    def from_db(cls, email_event_id, from_status, to_status, sequence_no, row_hash, *, reason="", extra=None, prev_hash=None, id=None, created_at, hash_algorithm="HMAC-SHA256", hash_version=1, key_id=""):
        return cls(email_event_id=email_event_id, from_status=from_status, to_status=to_status, sequence_no=sequence_no, reason=reason, extra=extra or {}, id=id, created_at=created_at, prev_hash=prev_hash, row_hash=row_hash, hash_algorithm=hash_algorithm, hash_version=hash_version, key_id=key_id)

    def canonical_payload(self) -> str:
        data = {"email_event_id": self.email_event_id, "from_status": self.from_status, "to_status": self.to_status, "sequence_no": self.sequence_no, "reason": self.reason, "extra": defrost(self.extra), "created_at": self.created_at.isoformat(), "prev_hash": self.prev_hash, "hash_algorithm": self.hash_algorithm.value, "hash_version": self.hash_version, "key_id": self.key_id}
        return json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False)

    def verify_self_hash(self, hash_service) -> bool:
        return hash_service.verify(self)
```

domain/entities/email_event.py

```python
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional, Union
from domain.exceptions import InvalidStateTransition, InvariantViolation

class EventStatus(str, Enum):
    NEW = "NEW"
    PARSING = "PARSING"
    MATCHING = "MATCHING"
    HANDOFF = "HANDOFF"
    FINALIZED = "FINALIZED"
    FAILED = "FAILED"
    RETRY_PENDING = "RETRY_PENDING"
    DEAD_LETTER = "DEAD_LETTER"

ALLOWED_TRANSITIONS = {
    EventStatus.NEW: {EventStatus.PARSING, EventStatus.DEAD_LETTER},
    EventStatus.PARSING: {EventStatus.MATCHING, EventStatus.FAILED},
    EventStatus.MATCHING: {EventStatus.HANDOFF, EventStatus.FAILED},
    EventStatus.HANDOFF: {EventStatus.FINALIZED, EventStatus.FAILED},
    EventStatus.FINALIZED: set(),
    EventStatus.FAILED: {EventStatus.RETRY_PENDING, EventStatus.DEAD_LETTER},
    EventStatus.RETRY_PENDING: set(),
    EventStatus.DEAD_LETTER: set(),
}

@dataclass(frozen=True)
class EmailEvent:
    id: str
    raw_email: str = ""
    status: EventStatus = EventStatus.NEW
    version: int = 1
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc).replace(microsecond=0))
    updated_at: Optional[datetime] = field(default=None)
    retry_from_stage: Optional[EventStatus] = field(default=None)

    def __post_init__(self):
        if not isinstance(self.id, str) or not self.id.strip():
            raise InvariantViolation("id must be non-empty string")
        object.__setattr__(self, 'id', self.id.strip())
        if not isinstance(self.version, int) or self.version < 1:
            raise InvariantViolation("version must be positive integer")
        if not isinstance(self.created_at, datetime) or self.created_at.tzinfo is None:
            raise InvariantViolation("created_at must be timezone-aware datetime")
        object.__setattr__(self, 'created_at', self.created_at.astimezone(timezone.utc).replace(microsecond=0))
        if self.updated_at is not None:
            if not isinstance(self.updated_at, datetime) or self.updated_at.tzinfo is None:
                raise InvariantViolation("updated_at must be timezone-aware datetime")
            object.__setattr__(self, 'updated_at', self.updated_at.astimezone(timezone.utc).replace(microsecond=0))
        if not isinstance(self.status, EventStatus):
            try:
                object.__setattr__(self, 'status', EventStatus(self.status))
            except ValueError as e:
                raise InvariantViolation(f"Invalid status: {self.status!r}") from e
        if self.retry_from_stage is not None:
            if not isinstance(self.retry_from_stage, EventStatus):
                try:
                    object.__setattr__(self, 'retry_from_stage', EventStatus(self.retry_from_stage))
                except ValueError as e:
                    raise InvariantViolation(f"Invalid retry_from_stage: {self.retry_from_stage!r}") from e
            if self.retry_from_stage not in {EventStatus.PARSING, EventStatus.MATCHING, EventStatus.HANDOFF}:
                raise InvariantViolation(f"retry_from_stage must be PARSING, MATCHING, or HANDOFF, got {self.retry_from_stage.value}")
        if self.status in {EventStatus.RETRY_PENDING, EventStatus.FAILED}:
            if self.retry_from_stage is None:
                raise InvariantViolation(f"retry_from_stage is required for {self.status.value}")
        else:
            if self.retry_from_stage is not None:
                raise InvariantViolation(f"retry_from_stage not allowed for {self.status.value}")

    def can_transition_to(self, target: EventStatus) -> bool:
        if self.status == EventStatus.RETRY_PENDING:
            return target in {self.retry_from_stage, EventStatus.FAILED}
        if self.status == EventStatus.FAILED and target == EventStatus.RETRY_PENDING:
            return self.retry_from_stage is not None and self.retry_from_stage not in {EventStatus.FAILED, EventStatus.RETRY_PENDING}
        if self.status == EventStatus.FAILED:
            return target in ALLOWED_TRANSITIONS.get(self.status, set())
        return target in ALLOWED_TRANSITIONS.get(self.status, set())

    def transition_to(self, target: Union[EventStatus, str]) -> 'EmailEvent':
        try:
            target = EventStatus(target)
        except ValueError as e:
            raise InvalidStateTransition(f"Unknown target status: {target!r}") from e
        if not self.can_transition_to(target):
            raise InvalidStateTransition(f"Cannot transition from {self.status.value} to {target.value}")
        new_retry_from_stage = None
        if target == EventStatus.FAILED:
            new_retry_from_stage = self.retry_from_stage if self.status == EventStatus.RETRY_PENDING and self.retry_from_stage else self.status
        elif target == EventStatus.RETRY_PENDING:
            new_retry_from_stage = self.retry_from_stage if self.retry_from_stage else self.status
        return EmailEvent(id=self.id, raw_email=self.raw_email, status=target, version=self.version + 1, created_at=self.created_at, updated_at=datetime.now(timezone.utc).replace(microsecond=0), retry_from_stage=new_retry_from_stage)

    def is_terminal(self) -> bool:
        return self.status in {EventStatus.FINALIZED, EventStatus.DEAD_LETTER}

    def __repr__(self):
        return f"EmailEvent(id={self.id!r}, status={self.status.value!r}, version={self.version})"
```

domain/entities/error_log.py

```python
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Dict, Any
import json
from domain.exceptions import InvariantViolation
from domain.freeze import deep_freeze, defrost

@dataclass(frozen=True)
class ErrorLog:
    email_event_id: Optional[str]
    correlation_id: str
    error_type: str
    error_message: str
    context: Dict[str, Any] = field(default_factory=dict)
    id: Optional[str] = field(default=None, compare=False, hash=False)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc).replace(microsecond=0), compare=False, hash=False)

    def __post_init__(self):
        if not isinstance(self.correlation_id, str) or not self.correlation_id.strip():
            raise InvariantViolation("correlation_id must be non-empty string")
        object.__setattr__(self, 'correlation_id', self.correlation_id.strip())
        if not isinstance(self.error_type, str) or not self.error_type.strip():
            raise InvariantViolation("error_type must be non-empty string")
        object.__setattr__(self, 'error_type', self.error_type.strip())
        if not isinstance(self.error_message, str) or not self.error_message.strip():
            raise InvariantViolation("error_message must be non-empty string")
        if not isinstance(self.created_at, datetime) or self.created_at.tzinfo is None:
            raise InvariantViolation("created_at must be timezone-aware datetime")
        object.__setattr__(self, 'created_at', self.created_at.astimezone(timezone.utc).replace(microsecond=0))
        frozen_context = deep_freeze(self.context or {})
        try:
            json.dumps(defrost(frozen_context), sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        except TypeError as e:
            raise InvariantViolation("context must be JSON-serializable") from e
        object.__setattr__(self, 'context', frozen_context)
```

domain/entities/evidence.py

```python
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Dict, Any
import json
from domain.exceptions import InvariantViolation
from domain.freeze import deep_freeze, defrost

@dataclass(frozen=True)
class Evidence:
    email_event_id: str
    evidence_type: str
    data: Dict[str, Any] = field(default_factory=dict)
    id: Optional[str] = field(default=None, compare=False, hash=False)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc).replace(microsecond=0), compare=False, hash=False)

    def __post_init__(self):
        if not isinstance(self.email_event_id, str) or not self.email_event_id.strip():
            raise InvariantViolation("email_event_id must be non-empty string")
        object.__setattr__(self, 'email_event_id', self.email_event_id.strip())
        if not isinstance(self.evidence_type, str) or not self.evidence_type.strip():
            raise InvariantViolation("evidence_type must be non-empty string")
        object.__setattr__(self, 'evidence_type', self.evidence_type.strip())
        if not isinstance(self.created_at, datetime) or self.created_at.tzinfo is None:
            raise InvariantViolation("created_at must be timezone-aware datetime")
        object.__setattr__(self, 'created_at', self.created_at.astimezone(timezone.utc).replace(microsecond=0))
        frozen_data = deep_freeze(self.data or {})
        try:
            json.dumps(defrost(frozen_data), sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        except TypeError as e:
            raise InvariantViolation("data must be JSON-serializable") from e
        object.__setattr__(self, 'data', frozen_data)
```

domain/entities/handoff.py

```python
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional
from domain.exceptions import InvariantViolation

class HandoffType(str, Enum):
    STANDARD = "STANDARD"
    PRIORITY = "PRIORITY"
    RETRY = "RETRY"

@dataclass(frozen=True)
class Handoff:
    email_event_id: str
    supplier_id: str
    artifact_id: str
    handoff_type: HandoffType = HandoffType.STANDARD
    id: Optional[str] = field(default=None, compare=False, hash=False)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc).replace(microsecond=0), compare=False, hash=False)

    def __post_init__(self):
        if not isinstance(self.email_event_id, str) or not self.email_event_id.strip():
            raise InvariantViolation("email_event_id must be non-empty string")
        object.__setattr__(self, 'email_event_id', self.email_event_id.strip())
        if not isinstance(self.supplier_id, str) or not self.supplier_id.strip():
            raise InvariantViolation("supplier_id must be non-empty string")
        object.__setattr__(self, 'supplier_id', self.supplier_id.strip())
        if not isinstance(self.artifact_id, str) or not self.artifact_id.strip():
            raise InvariantViolation("artifact_id must be non-empty string")
        object.__setattr__(self, 'artifact_id', self.artifact_id.strip())
        if not isinstance(self.handoff_type, HandoffType):
            try:
                object.__setattr__(self, 'handoff_type', HandoffType(self.handoff_type))
            except ValueError as e:
                raise InvariantViolation(f"Invalid handoff_type: {self.handoff_type!r}") from e
        if not isinstance(self.created_at, datetime) or self.created_at.tzinfo is None:
            raise InvariantViolation("created_at must be timezone-aware datetime")
        object.__setattr__(self, 'created_at', self.created_at.astimezone(timezone.utc).replace(microsecond=0))
```

domain/entities/seen_acknowledgment.py

```python
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional
from domain.exceptions import InvariantViolation

@dataclass(frozen=True)
class SeenAcknowledgment:
    email_event_id: str
    id: Optional[str] = field(default=None, compare=False, hash=False)
    seen_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc).replace(microsecond=0), compare=False, hash=False)

    def __post_init__(self):
        if not isinstance(self.email_event_id, str) or not self.email_event_id.strip():
            raise InvariantViolation("email_event_id must be non-empty string")
        object.__setattr__(self, 'email_event_id', self.email_event_id.strip())
        if not isinstance(self.seen_at, datetime) or self.seen_at.tzinfo is None:
            raise InvariantViolation("seen_at must be timezone-aware datetime")
        object.__setattr__(self, 'seen_at', self.seen_at.astimezone(timezone.utc).replace(microsecond=0))
```

---

APPLICATION LAYER

application/exceptions.py

```python
from typing import Optional

class ApplicationException(Exception):
    pass

class RepositoryConflict(ApplicationException):
    pass

class ConstraintConflict(RepositoryConflict):
    def __init__(self, constraint_name: Optional[str]):
        self.constraint_name = constraint_name
        super().__init__(f"Constraint conflict: {constraint_name or 'unknown'}")

class EventOutboxError(ApplicationException):
    pass
```

application/commands.py

```python
from dataclasses import dataclass
from domain.entities.email_event import EventStatus
from domain.entities.handoff import HandoffType
from domain.exceptions import InvalidHandoffCreation, InvalidSeenOperation, InvalidStateTransition, InvariantViolation

@dataclass(frozen=True)
class CreateEmailEventCommand:
    raw_email: str
    def __post_init__(self):
        if not isinstance(self.raw_email, str):
            raise InvariantViolation("raw_email must be string")

@dataclass(frozen=True)
class ProcessTransitionCommand:
    email_event_id: str
    to_status: EventStatus
    reason: str = ""
    def __post_init__(self):
        if not isinstance(self.email_event_id, str) or not self.email_event_id.strip():
            raise InvalidStateTransition("email_event_id must be non-empty string")
        object.__setattr__(self, 'email_event_id', self.email_event_id.strip())
        if self.reason is None:
            object.__setattr__(self, 'reason', "")
        if not isinstance(self.reason, str):
            raise InvalidStateTransition("reason must be string")
        try:
            object.__setattr__(self, 'to_status', EventStatus(self.to_status))
        except ValueError as e:
            raise InvalidStateTransition(f"Invalid target status: {self.to_status!r}") from e

@dataclass(frozen=True)
class CreateHandoffCommand:
    email_event_id: str
    supplier_id: str
    artifact_id: str
    handoff_type: HandoffType = HandoffType.STANDARD
    def __post_init__(self):
        if not isinstance(self.email_event_id, str) or not self.email_event_id.strip():
            raise InvalidHandoffCreation("email_event_id must be non-empty string")
        object.__setattr__(self, 'email_event_id', self.email_event_id.strip())
        if not isinstance(self.supplier_id, str) or not self.supplier_id.strip():
            raise InvalidHandoffCreation("supplier_id must be non-empty string")
        object.__setattr__(self, 'supplier_id', self.supplier_id.strip())
        if not isinstance(self.artifact_id, str) or not self.artifact_id.strip():
            raise InvalidHandoffCreation("artifact_id must be non-empty string")
        object.__setattr__(self, 'artifact_id', self.artifact_id.strip())
        try:
            object.__setattr__(self, 'handoff_type', HandoffType(self.handoff_type))
        except ValueError as e:
            raise InvalidHandoffCreation(f"Invalid handoff_type: {self.handoff_type!r}") from e

@dataclass(frozen=True)
class MarkSeenCommand:
    email_event_id: str
    def __post_init__(self):
        if not isinstance(self.email_event_id, str) or not self.email_event_id.strip():
            raise InvalidSeenOperation("email_event_id must be non-empty string")
        object.__setattr__(self, 'email_event_id', self.email_event_id.strip())
```

application/ports/event_repository.py

```python
from abc import ABC, abstractmethod
from domain.entities.email_event import EmailEvent

class EventRepository(ABC):
    @abstractmethod
    def save(self, event: EmailEvent) -> EmailEvent:
        pass
    @abstractmethod
    def get_by_id(self, event_id: str) -> EmailEvent:
        pass
    @abstractmethod
    def get_by_id_for_update(self, event_id: str) -> EmailEvent:
        pass
```

application/ports/transition_log_repository.py

```python
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Dict, Any
from domain.entities.event_transition_log import EventTransitionLog

@dataclass(frozen=True)
class RawTransitionLogEntry:
    id: str
    email_event_id: str
    sequence_no: int
    from_status: str
    to_status: str
    reason: str
    extra: Dict[str, Any]
    created_at: datetime
    prev_hash: Optional[str]
    row_hash: str
    hash_algorithm: str
    hash_version: int
    key_id: str

class TransitionLogRepository(ABC):
    @abstractmethod
    def get_last_transition_locked(self, email_event_id: str) -> Optional[EventTransitionLog]:
        pass
    @abstractmethod
    def get_ordered_by_event(self, email_event_id: str) -> List[EventTransitionLog]:
        pass
    @abstractmethod
    def get_raw_ordered_by_event(self, email_event_id: str) -> List[RawTransitionLogEntry]:
        pass
    @abstractmethod
    def save(self, entry: EventTransitionLog) -> EventTransitionLog:
        pass
```

application/ports/error_log_repository.py

```python
from abc import ABC, abstractmethod
from domain.entities.error_log import ErrorLog

class ErrorLogRepository(ABC):
    @abstractmethod
    def save(self, entry: ErrorLog) -> ErrorLog:
        pass
```

application/ports/evidence_repository.py

```python
from abc import ABC, abstractmethod
from domain.entities.evidence import Evidence

class EvidenceRepository(ABC):
    @abstractmethod
    def save(self, entry: Evidence) -> Evidence:
        pass
```

application/ports/handoff_repository.py

```python
from abc import ABC, abstractmethod
from domain.entities.handoff import Handoff

class HandoffRepository(ABC):
    @abstractmethod
    def save(self, handoff: Handoff) -> Handoff:
        pass
```

application/ports/seen_repository.py

```python
from abc import ABC, abstractmethod
from domain.entities.seen_acknowledgment import SeenAcknowledgment

class SeenRepository(ABC):
    @abstractmethod
    def save_or_get(self, record: SeenAcknowledgment) -> SeenAcknowledgment:
        pass
```

application/ports/unit_of_work.py

```python
from abc import ABC, abstractmethod

class UnitOfWork(ABC):
    @abstractmethod
    def commit(self) -> None:
        pass
    @abstractmethod
    def rollback(self) -> None:
        pass
    @abstractmethod
    def __enter__(self):
        pass
    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
```

application/ports/error_logger.py

```python
from abc import ABC, abstractmethod

class ErrorLoggerPort(ABC):
    @abstractmethod
    def log_error_in_new_transaction(self, correlation_id: str, error_type: str, error_message: str, event_id: str = "") -> None:
        pass
```

application/services/transition_hash_service.py

```python
import hashlib
import hmac
import logging
from typing import Dict, Tuple
from domain.entities.event_transition_log import EventTransitionLog, HashAlgorithm
from domain.exceptions import MisconfigurationError, UnsupportedHashAlgorithm

logger = logging.getLogger(__name__)

class TransitionHashService:
    def __init__(self, keyring: Dict[str, bytes], current_key_id: str, allow_key_fallback: bool = False):
        if not isinstance(current_key_id, str) or not current_key_id.strip():
            raise MisconfigurationError("current_key_id must be non-empty string")
        if not keyring:
            raise MisconfigurationError("TransitionHashService keyring is empty")
        if current_key_id not in keyring:
            raise MisconfigurationError(f"Current key '{current_key_id}' not in keyring")
        for kid, key in keyring.items():
            if not isinstance(kid, str) or not kid.strip():
                raise MisconfigurationError(f"key_id must be non-empty string, got {type(kid).__name__}: {kid!r}")
            if not isinstance(key, bytes) or not key:
                raise MisconfigurationError(f"HMAC key for '{kid}' must be non-empty bytes")
        self._keyring = dict(keyring)
        self._current_key_id = current_key_id
        self._allow_key_fallback = allow_key_fallback

    @property
    def current_key_id(self) -> str:
        return self._current_key_id

    def sign(self, entry: EventTransitionLog) -> Tuple[str, str]:
        if entry.key_id != self._current_key_id:
            raise MisconfigurationError(f"entry.key_id={entry.key_id!r} does not match current_key_id={self._current_key_id!r}")
        payload = entry.canonical_payload()
        key = self._keyring[self._current_key_id]
        digestmod = self._get_digestmod(entry.hash_algorithm)
        row_hash = hmac.new(key, payload.encode('utf-8'), digestmod).hexdigest()
        return row_hash, self._current_key_id

    def verify(self, entry: EventTransitionLog) -> bool:
        payload = entry.canonical_payload()
        digestmod = self._get_digestmod(entry.hash_algorithm)
        if entry.key_id in self._keyring:
            key = self._keyring[entry.key_id]
            expected = hmac.new(key, payload.encode('utf-8'), digestmod).hexdigest()
            if hmac.compare_digest(entry.row_hash, expected):
                return True
        if self._allow_key_fallback:
            for key_id, key in self._keyring.items():
                if key_id == entry.key_id:
                    continue
                expected = hmac.new(key, payload.encode('utf-8'), digestmod).hexdigest()
                if hmac.compare_digest(entry.row_hash, expected):
                    logger.warning(f"Entry {entry.id} verified with key {key_id}, expected {entry.key_id}")
                    return True
        return False

    def _get_digestmod(self, algorithm: HashAlgorithm):
        if algorithm == HashAlgorithm.HMAC_SHA256:
            return hashlib.sha256
        elif algorithm == HashAlgorithm.HMAC_SHA512:
            return hashlib.sha512
        raise UnsupportedHashAlgorithm(f"Unsupported hash algorithm: {algorithm}")
```

application/services/transition_log_service.py

```python
import logging
from typing import Optional
from domain.entities.event_transition_log import EventTransitionLog, ChainVerificationStatus
from domain.entities.email_event import EmailEvent
from domain.exceptions import IntegrityViolation, UnsupportedHashAlgorithm, UnsupportedHashVersion, InvariantViolation
from domain.freeze import defrost
from application.ports.transition_log_repository import TransitionLogRepository
from application.services.transition_hash_service import TransitionHashService

logger = logging.getLogger(__name__)

class TransitionLogService:
    def __init__(self, repository: TransitionLogRepository, hash_service: TransitionHashService):
        self._repo = repository
        self._hash = hash_service

    def log_transition(self, event: EmailEvent, from_status: str, to_status: str, reason: str, extra: Optional[dict] = None) -> EventTransitionLog:
        if from_status != event.status.value:
            raise IntegrityViolation(f"from_status mismatch: service={from_status}, event={event.status.value}")
        previous = self._repo.get_last_transition_locked(event.id)
        prev_hash = previous.row_hash if previous else None
        next_seq = (previous.sequence_no + 1) if previous else 1
        unsigned = EventTransitionLog.create(email_event_id=event.id, from_status=from_status, to_status=to_status, sequence_no=next_seq, reason=reason, extra=extra or {}, prev_hash=prev_hash, row_hash="0" * 64, key_id=self._hash.current_key_id)
        row_hash, key_id = self._hash.sign(unsigned)
        entry = EventTransitionLog.create(email_event_id=unsigned.email_event_id, from_status=unsigned.from_status, to_status=unsigned.to_status, sequence_no=unsigned.sequence_no, reason=unsigned.reason, extra=defrost(unsigned.extra), prev_hash=unsigned.prev_hash, row_hash=row_hash, created_at=unsigned.created_at, key_id=key_id, hash_algorithm=unsigned.hash_algorithm, hash_version=unsigned.hash_version)
        persisted = self._repo.save(entry)
        if not persisted.verify_self_hash(self._hash):
            raise IntegrityViolation(f"Hash mismatch after persistence for event {event.id}")
        return persisted

    def verify_chain(self, email_event_id: str) -> ChainVerificationStatus:
        raw_entries = self._repo.get_raw_ordered_by_event(email_event_id)
        if not raw_entries:
            return ChainVerificationStatus.EMPTY
        if raw_entries[0].prev_hash is not None or raw_entries[0].sequence_no != 1:
            return ChainVerificationStatus.BROKEN_LINK
        for i, raw in enumerate(raw_entries):
            try:
                entry = EventTransitionLog.from_db(email_event_id=raw.email_event_id, from_status=raw.from_status, to_status=raw.to_status, sequence_no=raw.sequence_no, reason=raw.reason, extra=raw.extra, id=raw.id, created_at=raw.created_at, prev_hash=raw.prev_hash, row_hash=raw.row_hash, hash_algorithm=raw.hash_algorithm, hash_version=raw.hash_version, key_id=raw.key_id)
            except UnsupportedHashVersion:
                return ChainVerificationStatus.UNSUPPORTED_HASH_VERSION
            except UnsupportedHashAlgorithm:
                return ChainVerificationStatus.UNSUPPORTED_HASH_ALGORITHM
            except InvariantViolation as e:
                msg = str(e)
                if "row_hash" in msg:
                    return ChainVerificationStatus.BROKEN_HASH
                return ChainVerificationStatus.BROKEN_LINK
            except Exception:
                return ChainVerificationStatus.BROKEN_LINK
            if not entry.verify_self_hash(self._hash):
                return ChainVerificationStatus.BROKEN_HASH
            if i > 0 and entry.prev_hash != raw_entries[i - 1].row_hash:
                return ChainVerificationStatus.BROKEN_LINK
            if i > 0 and entry.sequence_no != raw_entries[i - 1].sequence_no + 1:
                return ChainVerificationStatus.BROKEN_LINK
        return ChainVerificationStatus.VALID
```

application/services/error_log_service.py

```python
from typing import Optional, Dict, Any
from domain.entities.error_log import ErrorLog
from application.ports.error_log_repository import ErrorLogRepository

class ErrorLogService:
    def __init__(self, repository: ErrorLogRepository):
        self._repo = repository
    def log_error(self, correlation_id: str, error_type: str, error_message: str, event_id: str = "", context: Optional[Dict[str, Any]] = None) -> ErrorLog:
        entry = ErrorLog(email_event_id=event_id or None, correlation_id=correlation_id, error_type=error_type, error_message=error_message, context=context or {})
        return self._repo.save(entry)
```

application/services/evidence_service.py

```python
from typing import Dict, Any
from domain.entities.evidence import Evidence
from application.ports.evidence_repository import EvidenceRepository

class EvidenceService:
    def __init__(self, repository: EvidenceRepository):
        self._repo = repository
    def record_evidence(self, email_event_id: str, evidence_type: str, data: Dict[str, Any]) -> Evidence:
        entry = Evidence(email_event_id=email_event_id, evidence_type=evidence_type, data=data)
        return self._repo.save(entry)
```

application/services/handoff_service.py

```python
from domain.entities.handoff import Handoff, HandoffType
from domain.exceptions import InvalidHandoffCreation
from application.ports.handoff_repository import HandoffRepository

class HandoffService:
    def __init__(self, repository: HandoffRepository):
        self._repo = repository
    def create(self, email_event_id: str, supplier_id: str, artifact_id: str, handoff_type: HandoffType = HandoffType.STANDARD) -> Handoff:
        supplier_id = (supplier_id or "").strip()
        artifact_id = (artifact_id or "").strip()
        if not supplier_id:
            raise InvalidHandoffCreation(f"supplier_id must not be empty, got '{supplier_id}'")
        if not artifact_id:
            raise InvalidHandoffCreation(f"artifact_id must not be empty, got '{artifact_id}'")
        handoff = Handoff(email_event_id=email_event_id, supplier_id=supplier_id, artifact_id=artifact_id, handoff_type=handoff_type)
        return self._repo.save(handoff)
```

application/services/seen_service.py

```python
from domain.entities.seen_acknowledgment import SeenAcknowledgment
from application.commands import MarkSeenCommand
from application.ports.seen_repository import SeenRepository

class SeenService:
    def __init__(self, repository: SeenRepository):
        self._repo = repository
    def mark_seen(self, command: MarkSeenCommand) -> SeenAcknowledgment:
        record = SeenAcknowledgment(email_event_id=command.email_event_id)
        return self._repo.save_or_get(record)
```

application/email_event_application_service.py

```python
import uuid
import logging
from domain.entities.email_event import EmailEvent, EventStatus
from domain.entities.handoff import Handoff
from domain.exceptions import ConcurrencyConflict, InvalidHandoffCreation
from application.ports.unit_of_work import UnitOfWork
from application.ports.event_repository import EventRepository
from application.commands import CreateEmailEventCommand, ProcessTransitionCommand, CreateHandoffCommand, MarkSeenCommand
from application.services.transition_log_service import TransitionLogService
from application.services.evidence_service import EvidenceService
from application.services.handoff_service import HandoffService
from application.services.seen_service import SeenService
from application.ports.error_logger import ErrorLoggerPort
from application.exceptions import ConstraintConflict

logger = logging.getLogger(__name__)
HASH_CHAIN_CONSTRAINTS = {"uq_transition_event_sequence", "uq_transition_event_prev_hash"}
MAX_RETRIES = 3

class EmailEventApplicationService:
    def __init__(self, uow: UnitOfWork, event_repo: EventRepository, transition_log_service: TransitionLogService, evidence_service: EvidenceService, handoff_service: HandoffService, seen_service: SeenService, error_logger: ErrorLoggerPort):
        self._uow = uow
        self._event_repo = event_repo
        self._transition_log = transition_log_service
        self._evidence = evidence_service
        self._handoff = handoff_service
        self._seen = seen_service
        self._error_logger = error_logger

    def create_event(self, command: CreateEmailEventCommand) -> EmailEvent:
        event = EmailEvent(id=str(uuid.uuid4()), raw_email=command.raw_email)
        with self._uow:
            self._event_repo.save(event)
        return event

    def process_transition(self, command: ProcessTransitionCommand) -> EmailEvent:
        last_hash_chain_error = None
        last_optimistic_error = None
        for attempt in range(MAX_RETRIES):
            try:
                return self._process_transition_once(command)
            except ConstraintConflict as e:
                if e.constraint_name in HASH_CHAIN_CONSTRAINTS:
                    last_hash_chain_error = e
                    continue
                self._log_error(command, e)
                raise
            except ConcurrencyConflict as e:
                last_optimistic_error = e
                continue
            except Exception as e:
                self._log_error(command, e)
                raise
        if last_hash_chain_error is not None:
            conflict = ConcurrencyConflict(f"Hash-chain conflict for event {command.email_event_id} after {MAX_RETRIES} retries")
        else:
            conflict = ConcurrencyConflict(f"Optimistic lock conflict for event {command.email_event_id} after {MAX_RETRIES} retries")
        self._log_error(command, conflict)
        raise conflict from (last_hash_chain_error or last_optimistic_error)

    def _process_transition_once(self, command: ProcessTransitionCommand) -> EmailEvent:
        with self._uow:
            event = self._event_repo.get_by_id_for_update(command.email_event_id)
            updated = event.transition_to(command.to_status)
            self._event_repo.save(updated)
            self._transition_log.log_transition(event, event.status.value, updated.status.value, command.reason)
            self._evidence.record_evidence(event.id, f"TRANSITION_{updated.status.value}", {"from": event.status.value, "to": updated.status.value})
            if updated.is_terminal():
                self._seen.mark_seen(MarkSeenCommand(email_event_id=event.id))
            result = updated
        return result

    def create_handoff(self, command: CreateHandoffCommand) -> Handoff:
        with self._uow:
            event = self._event_repo.get_by_id_for_update(command.email_event_id)
            if event.status != EventStatus.HANDOFF:
                raise InvalidHandoffCreation(f"Handoff allowed only from HANDOFF status, current: {event.status.value}")
            result = self._handoff.create(command.email_event_id, command.supplier_id, command.artifact_id, command.handoff_type)
        return result

    def _log_error(self, command: ProcessTransitionCommand, exc: Exception) -> None:
        self._error_logger.log_error_in_new_transaction(correlation_id=command.email_event_id, error_type=type(exc).__name__, error_message=str(exc), event_id=command.email_event_id)
```

---

INFRASTRUCTURE LAYER

infrastructure/persistence/models.py

```python
import sqlalchemy as sa
from sqlalchemy import Column, String, DateTime, Text, Boolean, Integer, Index, CheckConstraint, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import declarative_base
import uuid

Base = declarative_base()

class EmailEventModel(Base):
    __tablename__ = "email_event"
    __table_args__ = (
        CheckConstraint("status IN ('NEW','PARSING','MATCHING','HANDOFF','FINALIZED','FAILED','RETRY_PENDING','DEAD_LETTER')", name="ck_email_event_status_valid"),
        CheckConstraint("retry_from_stage IS NULL OR retry_from_stage IN ('PARSING','MATCHING','HANDOFF')", name="ck_retry_from_stage_valid"),
        CheckConstraint("(status IN ('RETRY_PENDING','FAILED') AND retry_from_stage IS NOT NULL) OR (status NOT IN ('RETRY_PENDING','FAILED') AND retry_from_stage IS NULL)", name="ck_retry_from_stage_consistency"),
    )
    id = Column(String, primary_key=True)
    raw_email = Column(Text, nullable=False, default="")
    status = Column(String, nullable=False, default="NEW")
    version = Column(Integer, nullable=False, default=1)
    created_at = Column(DateTime(timezone=True), nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=True)
    retry_from_stage = Column(String, nullable=True)

class TransitionLogModel(Base):
    __tablename__ = "event_transition_log"
    __table_args__ = (
        Index("uq_transition_event_sequence", "email_event_id", "sequence_no", unique=True),
        Index("uq_transition_event_prev_hash", "email_event_id", "prev_hash", unique=True, postgresql_nulls_not_distinct=True),
        CheckConstraint("sequence_no > 0", name="ck_sequence_no_positive"),
        CheckConstraint("hash_version > 0", name="ck_hash_version_positive"),
        CheckConstraint("btrim(key_id) <> ''", name="ck_key_id_not_empty"),
        CheckConstraint("hash_algorithm IN ('HMAC-SHA256','HMAC-SHA512')", name="ck_hash_algorithm_valid"),
        CheckConstraint("(sequence_no = 1 AND prev_hash IS NULL) OR (sequence_no > 1 AND prev_hash IS NOT NULL)", name="ck_sequence_prev_hash_consistency"),
        CheckConstraint("from_status IN ('NEW','PARSING','MATCHING','HANDOFF','FINALIZED','FAILED','RETRY_PENDING','DEAD_LETTER')", name="ck_transition_from_status_valid"),
        CheckConstraint("to_status IN ('NEW','PARSING','MATCHING','HANDOFF','FINALIZED','FAILED','RETRY_PENDING','DEAD_LETTER')", name="ck_transition_to_status_valid"),
        CheckConstraint("(hash_algorithm = 'HMAC-SHA256' AND length(row_hash) = 64) OR (hash_algorithm = 'HMAC-SHA512' AND length(row_hash) = 128)", name="ck_row_hash_length_matches_algorithm"),
        CheckConstraint("prev_hash IS NULL OR length(prev_hash) IN (64, 128)", name="ck_prev_hash_length_valid"),
    )
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email_event_id = Column(String, ForeignKey("email_event.id"), nullable=False, index=True)
    sequence_no = Column(Integer, nullable=False)
    from_status = Column(String, nullable=False)
    to_status = Column(String, nullable=False)
    reason = Column(String, nullable=False, default="")
    extra = Column(JSONB, nullable=False, default=dict)
    created_at = Column(DateTime(timezone=True), nullable=False)
    prev_hash = Column(String(128), nullable=True)
    row_hash = Column(String(128), nullable=False)
    hash_algorithm = Column(String, nullable=False, default="HMAC-SHA256")
    hash_version = Column(Integer, nullable=False, default=1)
    key_id = Column(String, nullable=False, default="")

class ErrorLogModel(Base):
    __tablename__ = "error_log"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email_event_id = Column(String, ForeignKey("email_event.id"), nullable=True, index=True)
    correlation_id = Column(String, nullable=False, index=True)
    error_type = Column(String, nullable=False)
    error_message = Column(Text, nullable=False)
    context = Column(JSONB, nullable=False, default=dict)
    created_at = Column(DateTime(timezone=True), nullable=False)

class EvidenceModel(Base):
    __tablename__ = "evidence"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email_event_id = Column(String, ForeignKey("email_event.id"), nullable=False, index=True)
    evidence_type = Column(String, nullable=False)
    data = Column(JSONB, nullable=False, default=dict)
    created_at = Column(DateTime(timezone=True), nullable=False)

class HandoffModel(Base):
    __tablename__ = "handoff"
    __table_args__ = (CheckConstraint("handoff_type IN ('STANDARD','PRIORITY','RETRY')", name="ck_handoff_type_valid"),)
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email_event_id = Column(String, ForeignKey("email_event.id"), nullable=False, index=True)
    supplier_id = Column(String, nullable=False)
    artifact_id = Column(String, nullable=False)
    handoff_type = Column(String, nullable=False, default="STANDARD")
    created_at = Column(DateTime(timezone=True), nullable=False)
    processed = Column(Boolean, nullable=False, server_default=sa.false())
    processed_at = Column(DateTime(timezone=True), nullable=True)

class SeenAcknowledgmentModel(Base):
    __tablename__ = "seen_acknowledgment"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email_event_id = Column(String, ForeignKey("email_event.id"), nullable=False, unique=True)
    seen_at = Column(DateTime(timezone=True), nullable=False)
    processed = Column(Boolean, nullable=False, server_default=sa.false())
    processed_at = Column(DateTime(timezone=True), nullable=True)
```

infrastructure/persistence/repositories.py

```python
import uuid
import logging
from datetime import datetime, timezone
from typing import Optional, List
from sqlalchemy import text, update
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError as SAIntegrityError
from domain.entities.email_event import EmailEvent, EventStatus
from domain.entities.event_transition_log import EventTransitionLog
from domain.entities.error_log import ErrorLog
from domain.entities.evidence import Evidence
from domain.entities.handoff import Handoff, HandoffType
from domain.entities.seen_acknowledgment import SeenAcknowledgment
from domain.exceptions import EventNotFound, InvariantViolation, ConcurrencyConflict
from domain.freeze import defrost
from application.ports.event_repository import EventRepository
from application.ports.transition_log_repository import TransitionLogRepository, RawTransitionLogEntry
from application.ports.error_log_repository import ErrorLogRepository
from application.ports.evidence_repository import EvidenceRepository
from application.ports.handoff_repository import HandoffRepository
from application.ports.seen_repository import SeenRepository
from application.exceptions import ConstraintConflict
from .models import EmailEventModel, TransitionLogModel, ErrorLogModel, EvidenceModel, HandoffModel, SeenAcknowledgmentModel

logger = logging.getLogger(__name__)

def utc_now():
    return datetime.now(timezone.utc).replace(microsecond=0)

class SqlAlchemyEventRepository(EventRepository):
    def __init__(self, session: Session):
        self._session = session

    def get_by_id(self, event_id: str) -> EmailEvent:
        model = self._session.query(EmailEventModel).filter_by(id=event_id).first()
        if not model:
            raise EventNotFound(f"EmailEvent not found: {event_id}")
        return self._to_entity(model)

    def get_by_id_for_update(self, event_id: str) -> EmailEvent:
        model = self._session.query(EmailEventModel).filter_by(id=event_id).with_for_update().first()
        if not model:
            raise EventNotFound(f"EmailEvent not found: {event_id}")
        return self._to_entity(model)

    def save(self, event: EmailEvent) -> EmailEvent:
        existing = self._session.query(EmailEventModel).filter_by(id=event.id).first()
        if not existing:
            model = EmailEventModel(id=event.id, raw_email=event.raw_email, status=event.status.value, version=event.version, created_at=event.created_at, updated_at=event.updated_at, retry_from_stage=event.retry_from_stage.value if event.retry_from_stage else None)
            self._session.add(model)
            self._session.flush()
            return self._to_entity(model)
        expected_version = event.version - 1
        result = self._session.execute(update(EmailEventModel).where(EmailEventModel.id == event.id, EmailEventModel.version == expected_version).values(status=event.status.value, version=event.version, updated_at=event.updated_at, retry_from_stage=event.retry_from_stage.value if event.retry_from_stage else None))
        if result.rowcount != 1:
            raise ConcurrencyConflict(f"Optimistic lock conflict for event {event.id}: expected version {expected_version}")
        self._session.flush()
        return self.get_by_id(event.id)

    def _to_entity(self, model: EmailEventModel) -> EmailEvent:
        return EmailEvent(id=model.id, raw_email=model.raw_email, status=EventStatus(model.status), version=model.version, created_at=model.created_at, updated_at=model.updated_at, retry_from_stage=EventStatus(model.retry_from_stage) if model.retry_from_stage else None)

class SqlAlchemyTransitionLogRepository(TransitionLogRepository):
    def __init__(self, session: Session):
        self._session = session

    def get_last_transition_locked(self, email_event_id: str) -> Optional[EventTransitionLog]:
        model = self._session.query(TransitionLogModel).filter_by(email_event_id=email_event_id).order_by(TransitionLogModel.sequence_no.desc()).with_for_update().first()
        return self._to_entity(model) if model else None

    def get_ordered_by_event(self, email_event_id: str) -> List[EventTransitionLog]:
        models = self._session.query(TransitionLogModel).filter_by(email_event_id=email_event_id).order_by(TransitionLogModel.sequence_no.asc()).all()
        return [self._to_entity(m) for m in models]

    def get_raw_ordered_by_event(self, email_event_id: str) -> List[RawTransitionLogEntry]:
        result = self._session.execute(text("SELECT id, email_event_id, sequence_no, from_status, to_status, reason, extra, created_at, prev_hash, row_hash, hash_algorithm, hash_version, key_id FROM event_transition_log WHERE email_event_id = :eid ORDER BY sequence_no ASC"), {"eid": email_event_id})
        rows = result.mappings().all()
        return [RawTransitionLogEntry(id=str(r['id']), email_event_id=r['email_event_id'], sequence_no=r['sequence_no'], from_status=r['from_status'], to_status=r['to_status'], reason=r['reason'], extra=r['extra'] or {}, created_at=r['created_at'], prev_hash=r['prev_hash'], row_hash=r['row_hash'], hash_algorithm=r['hash_algorithm'], hash_version=r['hash_version'], key_id=r['key_id']) for r in rows]

    def save(self, entry: EventTransitionLog) -> EventTransitionLog:
        try:
            model = TransitionLogModel(id=entry.id or uuid.uuid4(), email_event_id=entry.email_event_id, sequence_no=entry.sequence_no, from_status=entry.from_status, to_status=entry.to_status, reason=entry.reason, extra=defrost(entry.extra), created_at=entry.created_at, prev_hash=entry.prev_hash, row_hash=entry.row_hash, hash_algorithm=entry.hash_algorithm.value, hash_version=entry.hash_version, key_id=entry.key_id)
            self._session.add(model)
            self._session.flush()
            return self._to_entity(model)
        except SAIntegrityError as e:
            orig = getattr(e, "orig", None)
            diag = getattr(orig, "diag", None)
            constraint_name = getattr(diag, "constraint_name", None) if diag else None
            raise ConstraintConflict(constraint_name) from e

    def _to_entity(self, model: TransitionLogModel) -> EventTransitionLog:
        return EventTransitionLog.from_db(email_event_id=model.email_event_id, from_status=model.from_status, to_status=model.to_status, sequence_no=model.sequence_no, reason=model.reason, extra=model.extra or {}, id=str(model.id), created_at=model.created_at, prev_hash=model.prev_hash, row_hash=model.row_hash, hash_algorithm=model.hash_algorithm, hash_version=model.hash_version, key_id=model.key_id)

class SqlAlchemyErrorLogRepository(ErrorLogRepository):
    def __init__(self, session: Session):
        self._session = session

    def save(self, entry: ErrorLog) -> ErrorLog:
        model = ErrorLogModel(id=entry.id or uuid.uuid4(), email_event_id=entry.email_event_id, correlation_id=entry.correlation_id, error_type=entry.error_type, error_message=entry.error_message, context=defrost(entry.context), created_at=entry.created_at or utc_now())
        self._session.add(model)
        self._session.flush()
        return self._to_entity(model)

    def _to_entity(self, model: ErrorLogModel) -> ErrorLog:
        return ErrorLog(email_event_id=model.email_event_id, correlation_id=model.correlation_id, error_type=model.error_type, error_message=model.error_message, context=model.context, id=str(model.id), created_at=model.created_at)

class SqlAlchemyEvidenceRepository(EvidenceRepository):
    def __init__(self, session: Session):
        self._session = session

    def save(self, entry: Evidence) -> Evidence:
        model = EvidenceModel(id=entry.id or uuid.uuid4(), email_event_id=entry.email_event_id, evidence_type=entry.evidence_type, data=defrost(entry.data), created_at=entry.created_at or utc_now())
        self._session.add(model)
        self._session.flush()
        return self._to_entity(model)

    def _to_entity(self, model: EvidenceModel) -> Evidence:
        return Evidence(email_event_id=model.email_event_id, evidence_type=model.evidence_type, data=model.data, id=str(model.id), created_at=model.created_at)

class SqlAlchemyHandoffRepository(HandoffRepository):
    def __init__(self, session: Session):
        self._session = session

    def save(self, handoff: Handoff) -> Handoff:
        model = HandoffModel(id=handoff.id or uuid.uuid4(), email_event_id=handoff.email_event_id, supplier_id=handoff.supplier_id, artifact_id=handoff.artifact_id, handoff_type=handoff.handoff_type.value, created_at=handoff.created_at or utc_now())
        self._session.add(model)
        self._session.flush()
        return self._to_entity(model)

    def _to_entity(self, model: HandoffModel) -> Handoff:
        try:
            handoff_type = HandoffType(model.handoff_type)
        except ValueError:
            raise InvariantViolation(f"Unknown handoff_type '{model.handoff_type}' for handoff {model.id}")
        return Handoff(email_event_id=model.email_event_id, supplier_id=model.supplier_id, artifact_id=model.artifact_id, handoff_type=handoff_type, id=str(model.id), created_at=model.created_at)

class SqlAlchemySeenRepository(SeenRepository):
    def __init__(self, session: Session):
        self._session = session

    def save_or_get(self, record: SeenAcknowledgment) -> SeenAcknowledgment:
        self._session.execute(text("INSERT INTO seen_acknowledgment (id, email_event_id, seen_at, processed) VALUES (:id, :email_event_id, :seen_at, false) ON CONFLICT (email_event_id) DO NOTHING"), {"id": record.id or str(uuid.uuid4()), "email_event_id": record.email_event_id, "seen_at": record.seen_at or utc_now()})
        self._session.flush()
        result = self._session.execute(text("SELECT id::text, email_event_id, seen_at FROM seen_acknowledgment WHERE email_event_id = :eid"), {"eid": record.email_event_id})
        row = result.fetchone()
        if row is None:
            raise InvariantViolation(f"seen_acknowledgment record missing for {record.email_event_id}")
        return SeenAcknowledgment(email_event_id=row.email_event_id, id=row.id, seen_at=row.seen_at)
```

infrastructure/persistence/unit_of_work.py

```python
from sqlalchemy.orm import Session
from application.ports.unit_of_work import UnitOfWork

class SqlAlchemyUnitOfWork(UnitOfWork):
    def __init__(self, session: Session):
        self._session = session
        self._stack: list = []

    def __enter__(self):
        if self._session.in_transaction():
            tx = self._session.begin_nested()
        else:
            tx = self._session.begin()
        self._stack.append({"tx": tx, "completed": False})
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self._stack:
            return False
        frame = self._stack.pop()
        tx = frame["tx"]
        try:
            if frame["completed"]:
                return False
            if exc_type:
                if tx.is_active:
                    tx.rollback()
            else:
                if tx.is_active:
                    tx.commit()
        except Exception:
            if tx.is_active:
                tx.rollback()
            raise
        return False

    def commit(self):
        if not self._stack:
            return
        frame = self._stack[-1]
        tx = frame["tx"]
        if not frame["completed"] and tx.is_active:
            tx.commit()
            frame["completed"] = True

    def rollback(self):
        if not self._stack:
            return
        frame = self._stack[-1]
        tx = frame["tx"]
        if not frame["completed"] and tx.is_active:
            tx.rollback()
            frame["completed"] = True
```

infrastructure/persistence/transactional_error_logger.py

```python
import logging
from sqlalchemy.orm import Session
from application.ports.error_logger import ErrorLoggerPort
from infrastructure.persistence.repositories import SqlAlchemyErrorLogRepository
from infrastructure.persistence.models import EmailEventModel
from domain.entities.error_log import ErrorLog

logger = logging.getLogger(__name__)

class TransactionalErrorLogger(ErrorLoggerPort):
    def __init__(self, session_factory):
        self._session_factory = session_factory

    def log_error_in_new_transaction(self, correlation_id: str, error_type: str, error_message: str, event_id: str = "") -> None:
        try:
            session: Session = self._session_factory()
            try:
                with session.begin():
                    event_exists = bool(event_id) and session.query(session.query(EmailEventModel).filter_by(id=event_id).exists()).scalar()
                    safe_message = str(error_message).strip() or error_type or "unknown error"
                    repo = SqlAlchemyErrorLogRepository(session)
                    repo.save(ErrorLog(email_event_id=event_id if event_exists else None, correlation_id=correlation_id, error_type=error_type, error_message=safe_message[:500], context={"attempted_event_id": event_id, "event_exists": event_exists}))
            finally:
                session.close()
        except Exception as e:
            logger.critical(f"Failed to log error for correlation_id {correlation_id}: {e}")
```

---

MIGRATION

db/migrations/20250128120000_block4_hash_chaining.py

```python
from alembic import op
import sqlalchemy as sa
import hashlib
import hmac
import json
import os
from datetime import timezone

revision = '20250128120000_block4_hash_chaining'
down_revision = '20250127100000_initial'
branch_labels = None
depends_on = None

def _column_exists(conn, table_name, column_name):
    result = conn.execute(sa.text("SELECT 1 FROM information_schema.columns WHERE table_name = :table AND column_name = :column AND table_schema = current_schema()"), {"table": table_name, "column": column_name}).fetchone()
    return result is not None

def _constraint_exists(conn, constraint_name, table_name):
    result = conn.execute(sa.text("SELECT 1 FROM pg_constraint WHERE conname = :name AND conrelid = to_regclass(:table)"), {"name": constraint_name, "table": table_name}).fetchone()
    return result is not None

def _index_exists(conn, index_name):
    result = conn.execute(sa.text("SELECT 1 FROM pg_indexes WHERE indexname = :name AND schemaname = current_schema()"), {"name": index_name}).fetchone()
    return result is not None

def _fk_exists(conn, fk_name, table_name):
    result = conn.execute(sa.text("SELECT 1 FROM pg_constraint WHERE conname = :name AND conrelid = to_regclass(:table) AND contype = 'f'"), {"name": fk_name, "table": table_name}).fetchone()
    return result is not None

def _is_nullable(conn, table_name, column_name):
    result = conn.execute(sa.text("SELECT is_nullable FROM information_schema.columns WHERE table_name = :table AND column_name = :column AND table_schema = current_schema()"), {"table": table_name, "column": column_name}).fetchone()
    return result and result[0] == 'YES'

def _batch_query(conn):
    last_event = ''
    last_seq = 0
    while True:
        result = conn.execute(sa.text("SELECT id, email_event_id, from_status, to_status, sequence_no, reason, extra, created_at FROM event_transition_log WHERE row_hash IS NULL AND (:last_event = '' OR (email_event_id, sequence_no) > (:last_event, :last_seq)) ORDER BY email_event_id, sequence_no LIMIT :limit"), {"last_event": last_event, "last_seq": last_seq, "limit": 1000})
        rows = [dict(r._mapping) for r in result]
        if not rows:
            break
        yield rows
        last_event = rows[-1]['email_event_id']
        last_seq = rows[-1]['sequence_no']

def canonicalize(email_event_id, from_status, to_status, sequence_no, reason, extra, created_at, prev_hash, key_id=""):
    normalized = created_at.astimezone(timezone.utc).replace(microsecond=0)
    data = {"email_event_id": email_event_id, "from_status": from_status, "to_status": to_status, "sequence_no": sequence_no, "reason": reason, "extra": extra or {}, "created_at": normalized.isoformat(), "prev_hash": prev_hash, "hash_algorithm": "HMAC-SHA256", "hash_version": 1, "key_id": key_id}
    return json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False)

def compute_hash(content):
    key = os.environ.get("HASH_CHAIN_KEY")
    if not key:
        raise RuntimeError("HASH_CHAIN_KEY is required for migration")
    return hmac.new(key.encode(), content.encode('utf-8'), hashlib.sha256).hexdigest()

constraints = [
    ('ck_sequence_no_positive', 'event_transition_log', 'sequence_no > 0'),
    ('ck_hash_version_positive', 'event_transition_log', 'hash_version > 0'),
    ('ck_key_id_not_empty', 'event_transition_log', "btrim(key_id) <> ''"),
    ('ck_hash_algorithm_valid', 'event_transition_log', "hash_algorithm IN ('HMAC-SHA256','HMAC-SHA512')"),
    ('ck_sequence_prev_hash_consistency', 'event_transition_log', '(sequence_no = 1 AND prev_hash IS NULL) OR (sequence_no > 1 AND prev_hash IS NOT NULL)'),
    ('ck_transition_from_status_valid', 'event_transition_log', "from_status IN ('NEW','PARSING','MATCHING','HANDOFF','FINALIZED','FAILED','RETRY_PENDING','DEAD_LETTER')"),
    ('ck_transition_to_status_valid', 'event_transition_log', "to_status IN ('NEW','PARSING','MATCHING','HANDOFF','FINALIZED','FAILED','RETRY_PENDING','DEAD_LETTER')"),
    ('ck_row_hash_length_matches_algorithm', 'event_transition_log', "(hash_algorithm = 'HMAC-SHA256' AND length(row_hash) = 64) OR (hash_algorithm = 'HMAC-SHA512' AND length(row_hash) = 128)"),
    ('ck_prev_hash_length_valid', 'event_transition_log', "prev_hash IS NULL OR length(prev_hash) IN (64, 128)"),
    ('ck_email_event_status_valid', 'email_event', "status IN ('NEW','PARSING','MATCHING','HANDOFF','FINALIZED','FAILED','RETRY_PENDING','DEAD_LETTER')"),
    ('ck_retry_from_stage_valid', 'email_event', "retry_from_stage IS NULL OR retry_from_stage IN ('PARSING','MATCHING','HANDOFF')"),
    ('ck_retry_from_stage_consistency', 'email_event', "(status IN ('RETRY_PENDING','FAILED') AND retry_from_stage IS NOT NULL) OR (status NOT IN ('RETRY_PENDING','FAILED') AND retry_from_stage IS NULL)"),
]

def upgrade():
    conn = op.get_bind()
    ver = conn.execute(sa.text("SHOW server_version_num")).scalar()
    if int(ver) < 150000:
        raise RuntimeError("PostgreSQL 15+ required for NULLS NOT DISTINCT")
    key_id = os.environ.get("HASH_CHAIN_KEY_ID")
    if not key_id:
        raise RuntimeError("HASH_CHAIN_KEY_ID is required for migration")

    for table, col_spec in [
        ('email_event', sa.Column('retry_from_stage', sa.String, nullable=True)),
        ('event_transition_log', sa.Column('sequence_no', sa.Integer, nullable=True)),
        ('event_transition_log', sa.Column('hash_algorithm', sa.String, nullable=True)),
        ('event_transition_log', sa.Column('hash_version', sa.Integer, nullable=True)),
        ('event_transition_log', sa.Column('key_id', sa.String, nullable=True)),
        ('event_transition_log', sa.Column('prev_hash', sa.VARCHAR(128), nullable=True)),
        ('event_transition_log', sa.Column('row_hash', sa.VARCHAR(128), nullable=True)),
    ]:
        if not _column_exists(conn, table, col_spec.name):
            op.add_column(table, col_spec)

    if _column_exists(conn, 'email_event', 'retry_from_stage'):
        conn.execute(sa.text("""
            UPDATE email_event e
            SET retry_from_stage = COALESCE(
                (SELECT l.from_status
                 FROM event_transition_log l
                 WHERE l.email_event_id = e.id
                   AND l.to_status = 'FAILED'
                   AND l.from_status IN ('PARSING', 'MATCHING', 'HANDOFF')
                 ORDER BY l.created_at DESC, l.id DESC
                 LIMIT 1),
                'PARSING'
            )
            WHERE e.status IN ('FAILED', 'RETRY_PENDING')
              AND e.retry_from_stage IS NULL
        """))

    if _column_exists(conn, 'event_transition_log', 'sequence_no') and _is_nullable(conn, 'event_transition_log', 'sequence_no'):
        if conn.execute(sa.text("SELECT count(*) FROM event_transition_log WHERE sequence_no IS NULL")).scalar() > 0:
            conn.execute(sa.text("UPDATE event_transition_log t SET sequence_no = sq.seq FROM (SELECT id, ROW_NUMBER() OVER (PARTITION BY email_event_id ORDER BY created_at, id) AS seq FROM event_transition_log) sq WHERE t.id = sq.id"))
        op.alter_column('event_transition_log', 'sequence_no', nullable=False)

    for batch in _batch_query(conn):
        by_event = {}
        for row in batch:
            by_event.setdefault(row['email_event_id'], []).append(row)
        for event_id, entries in by_event.items():
            result = conn.execute(sa.text("SELECT row_hash FROM event_transition_log WHERE email_event_id = :ev AND row_hash IS NOT NULL ORDER BY sequence_no DESC LIMIT 1"), {"ev": event_id}).fetchone()
            prev_hash = result[0] if result else None
            for row in sorted(entries, key=lambda r: r['sequence_no']):
                rh = compute_hash(canonicalize(row['email_event_id'], row['from_status'], row['to_status'], row['sequence_no'], row['reason'], row['extra'], row['created_at'], prev_hash, key_id))
                conn.execute(sa.text("UPDATE event_transition_log SET prev_hash = :ph, row_hash = :rh, hash_algorithm = 'HMAC-SHA256', hash_version = 1, key_id = :kid WHERE id = :id"), {"ph": prev_hash, "rh": rh, "kid": key_id, "id": row['id']})
                prev_hash = rh

    for col in ['row_hash', 'hash_algorithm', 'hash_version', 'key_id']:
        if _column_exists(conn, 'event_transition_log', col) and _is_nullable(conn, 'event_transition_log', col):
            nulls = conn.execute(sa.text(f"SELECT count(*) FROM event_transition_log WHERE {col} IS NULL")).scalar()
            if nulls > 0:
                raise RuntimeError(f"Found {nulls} NULLs in event_transition_log.{col} after backfill")
            op.alter_column('event_transition_log', col, nullable=False)

    if not _column_exists(conn, 'error_log', 'correlation_id'):
        op.add_column('error_log', sa.Column('correlation_id', sa.String, nullable=True))
    if _column_exists(conn, 'error_log', 'correlation_id'):
        if conn.execute(sa.text("SELECT count(*) FROM error_log WHERE correlation_id IS NULL")).scalar() > 0:
            conn.execute(sa.text("UPDATE error_log SET correlation_id = COALESCE(email_event_id, id::text) WHERE correlation_id IS NULL"))
        if _is_nullable(conn, 'error_log', 'correlation_id'):
            op.alter_column('error_log', 'correlation_id', nullable=False)
    if _column_exists(conn, 'error_log', 'email_event_id') and not _is_nullable(conn, 'error_log', 'email_event_id'):
        op.alter_column('error_log', 'email_event_id', nullable=True)

    for name, table, expr in constraints:
        if not _constraint_exists(conn, name, table):
            op.create_check_constraint(name, table, expr)

    if not _index_exists(conn, 'uq_transition_event_sequence'):
        op.execute(sa.text("CREATE UNIQUE INDEX uq_transition_event_sequence ON event_transition_log (email_event_id, sequence_no)"))
    if not _index_exists(conn, 'uq_transition_event_prev_hash'):
        op.execute(sa.text("CREATE UNIQUE INDEX uq_transition_event_prev_hash ON event_transition_log (email_event_id, prev_hash) NULLS NOT DISTINCT"))
    if not _index_exists(conn, 'ix_error_log_correlation_id'):
        op.create_index('ix_error_log_correlation_id', 'error_log', ['correlation_id'])

    if not _fk_exists(conn, 'fk_event_transition_log_email_event', 'event_transition_log'):
        orphans = conn.execute(sa.text("SELECT count(*) FROM event_transition_log l LEFT JOIN email_event e ON e.id = l.email_event_id WHERE e.id IS NULL")).scalar()
        if orphans > 0:
            raise RuntimeError(f"Found {orphans} orphan transition logs. Repair before adding FK.")
        op.create_foreign_key("fk_event_transition_log_email_event", "event_transition_log", "email_event", ["email_event_id"], ["id"])

def downgrade():
    raise RuntimeError("Downgrade is not supported for audit hash-chain migration")
```

---

```
БЛОК 4 ЗАВЕРШЁН.
ГОТОВ К БЛОКУ 5.
