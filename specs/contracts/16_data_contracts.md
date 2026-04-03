# Data Contracts

## Определение
Data contract это формальное соглашение между producer слоя intake/extract и consumer слоя normalization.

## Назначение
Data contract фиксирует:
- какую структуру входных данных ожидает система
- какие поля обязательны
- какие source columns мапятся в canonical fields
- какие quality expectations применяются
- как трактуются breaking changes

## Структура контракта

```yaml
# reference-data/contracts/{supplier}_{domain}.yaml
contract_version: 1
domain: wheel_passenger
source: brinex
sheet_name: Автодиски
header_row_index: 1

required_columns:
  - Диаметр
  - Ширина
  - PCD

column_mapping:
  diameter: ["Диаметр", "DIA"]
  width: ["Ширина", "WIDTH"]
  pcd: ["PCD", "PCD / Разболтовка"]

quality_expectations:
  min_rows: 1
  max_null_rate: 0.05
```

## Правила
- Один contract применяется к одному `source + domain + layout`.
- Если layout меняется существенно, это требует нового contract или новой версии contract.
- Contract должен быть загружен и применён до normalize stage.
- Contract не может быть "молча проигнорирован".

## Валидация
- При старте run контракт загружается.
- Нарушение `required_columns` приводит к `REJECT`.
- Нарушение `max_null_rate` приводит к `QUARANTINE`.
- Добавление новых необязательных колонок приводит к `WARNING` и классифицируется как drift class `optional_added`.

## Версионирование
- `contract_version` увеличивается при breaking changes.
- Run manifest содержит `contract_version`, использованный в конкретном run.
- Historical run должен быть трассируем к конкретной версии контракта.

## Связь с другими контрактами
- Drift classification задаётся в `10_schema_drift_handling.md`.
- Gate action задаётся в `06_quality_gates.md`.
- Replay versioning задаётся в `11_versioning.md`.
