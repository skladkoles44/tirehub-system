# tirehub-system

Система ingestion, normalization и current-state обновления для поставщиков автотоваров.

Репозиторий описывает и реализует путь от входящего payload к двум разным текущим состояниям:

- supplier reality
- marketplace reality

Главный принцип: один и тот же вход при одинаковых pinned версиях должен приводить к воспроизводимому результату.

## System model

Raw Intake Layer -> Normalized Supplier Offers Current -> Marketplace Current

### Raw Intake Layer
Хранит факт получения и оригинальный payload.
Не содержит бизнес-нормализации.
Является неизменяемым слоем факта получения.

### Normalized Supplier Offers Current
Хранит текущее каноническое состояние предложений поставщика.
Является результатом normalize stage.
Отражает supplier reality, а не витринную модель.

### Marketplace Current
Формируется только после matching и publish policy.
Содержит только публикабельные офферы.
Не пополняется напрямую из raw intake.

## Contracts

Основной reference-layer для архитектуры и data contracts находится в:

- specs/contracts/README.md

Ключевые документы:

- specs/contracts/01_architecture_layers.md
- specs/contracts/02_lifecycle.md
- specs/contracts/03_canonical_supplier_offer.md
- specs/contracts/06_quality_gates.md
- specs/contracts/07_current_state_model.md
- specs/contracts/08_supplier_vs_marketplace.md
- specs/contracts/10_schema_drift_handling.md
- specs/contracts/11_versioning.md
- specs/contracts/16_data_contracts.md

## Lifecycle

Ниже приведён condensed summary lifecycle.
Полный normative lifecycle задаётся в `specs/contracts/02_lifecycle.md`.

1. Приём данных из источника
2. Сохранение raw payload
3. Structural extraction
4. Normalization
5. Quality gate
6. Current-state diff/update
7. Matching
8. Marketplace publish update

## Bounded contexts

### Ingestion / Normalize
Готовит supplier reality.
Не имеет права напрямую создавать marketplace reality.

### Marketplace
Работает только после matching и publish policy.
Использует publishable subset supplier current layer.

## Environments

PHONE / TEST / PROD используют одинаковую концептуальную модель.
Различаются только окружением выполнения, путями и operational policy.

## Git

- main — стабильная ветка
- test — интеграционная ветка разработки
- feature/*, refactor/*, prep/* — короткоживущие рабочие ветки

## Legacy note

Старые документы и старые ingestion-only описания не являются главным законом системы.
Главным reference-layer теперь является пакет в specs/contracts/.

## LLM docs

- docs/architecture/MASTER_SYSTEM_MAP_LLM.md
- docs/architecture/COMPONENT_MAP_LLM.md
- docs/architecture/DATA_FLOW_DIAGRAM_LLM.md
- docs/architecture/SYSTEM_LAYERS_DIAGRAM_LLM.md

## Engineering docs

- [New engineer onboarding](docs/onboarding/new_engineer.md)
- [Legacy donor map](docs/legacy_donor/legacy_donor_map.md)
- [Atomic rows contract](docs/contracts/atomic_rows_contract.md)
- [Canonical supplier offer contract](docs/contracts/canonical_supplier_offer_contract.md)
