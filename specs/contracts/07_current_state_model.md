# Current State Model

## Хранимые сущности
- `runs_journal`
- `supplier_offers_current`
- `recent_changes`
- `raw_archive` вне hot path

## Принцип
- Основная правда системы по остаткам и предложениям это current state.
- Полная историзация не является обязательной частью базового hot path.
- Полная историзация является отдельным режимом, а не default behavior.

## Retention
- `raw_archive`: 90 дней
- `recent_changes`: определяется операционной политикой
- `supplier_offers_current`: indefinite
- `runs_journal`: определяется операционной политикой и требованиями трассировки

## Ограничения
- Current state не должен строиться через прямой truncate-and-reload без явной причины.
- Current state обновляется через diff и controlled update.
