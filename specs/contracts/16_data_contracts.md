# Data Contracts

## Определение
Data contract это формальное соглашение между upstream stage, который интерпретирует source layout, и normalization stage, который строит canonical fields.

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
- Добавление новых необязательных колонок должно создавать drift event класса `optional_added`.
- Само по себе наличие drift event не подменяет отдельное gate decision.

## Версионирование
- `contract_version` увеличивается при breaking changes.
- Run manifest содержит `contract_version`, использованный в конкретном run.
- Historical run должен быть трассируем к конкретной версии контракта.

## Связь с другими контрактами
- Drift classification задаётся в `10_schema_drift_handling.md`.
- Gate action задаётся в `06_quality_gates.md`.
- Replay versioning задаётся в `11_versioning.md`.

## Связь с inventory_facts contract

Для location-aware supplier inventory введён отдельный extraction/output contract:

`specs/contracts/17_inventory_facts_contract.md`

Он фиксирует минимальный contract для `inventory_facts.ndjson` и покрывает оба source-pattern:
- `explicit_row_location`
- `implicit_header_location`

Этот контракт не заменяет domain/layout contracts, а дополняет их как отдельный inventory extraction layer до downstream inventory processing.

