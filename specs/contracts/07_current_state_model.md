# Current State Model

## Хранимые сущности
- `runs_journal`
- `supplier_offers_current`
- `recent_changes`
- `raw_archive` вне hot path
- `marketplace_offers_current`

## Layer mapping
- `raw_archive` соответствует Raw Intake Layer.
- `supplier_offers_current` соответствует Normalized Supplier Offers Current.
- `marketplace_offers_current` соответствует Marketplace Current.
- `runs_journal` и `recent_changes` являются operational/supporting сущностями, а не самостоятельными архитектурными слоями.

## Принцип
- Основная правда системы по остаткам и предложениям это current state.
- Полная историзация не является обязательной частью базового hot path.
- Полная историзация является отдельным режимом, а не default behavior.

## Current-state granularity
- Единица supplier current state это одна запись на `supplier_id + offer_key + warehouse_key`.
- Marketplace current использует собственную publishable granularity после matching и publish policy.

## Update semantics
- Current state обновляется через diff и controlled update.
- Совпадающий current key должен приводить к update/replace одной и той же logical row, а не к бесконтрольному накоплению дублей.
- Current state не должен строиться через прямой truncate-and-reload без явной причины.

## Missing-in-run semantics
- Исчезновение предложения в новом run не должно приводить к молчаливому hard delete без policy.
- Missing offer должен переводиться в `inactive`, `stale`, zero-state или иной явно заданный статус по operational policy.
- Семантика исчезновения должна быть одинаково воспроизводимой для одинакового входа и одинаковой policy.

## Retention
- `raw_archive`: 90 дней
- `recent_changes`: определяется операционной политикой
- `supplier_offers_current`: indefinite
- `marketplace_offers_current`: определяется publish policy и operational policy
- `runs_journal`: определяется операционной политикой и требованиями трассировки
