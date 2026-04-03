# Versioning Contracts

## Версии
- `extract_contract_version`
- `normalize_contract_version`
- `publish_contract_version`
- `manifest_schema_version`

## Принцип
- Версия нужна не только коду, но и контрактам.
- При изменении контракта должна увеличиваться соответствующая версия.
- Старые данные сохраняют свою historical version.
- Новые run используют актуальную pinned version.
- Каждый run manifest содержит pinned версии всех применённых контрактов.

## Replay
- Replay использует pinned версии из manifest, а не текущие версии в репозитории.
- Replay должен воспроизводить historical behavior, а не текущее состояние кода по умолчанию.
