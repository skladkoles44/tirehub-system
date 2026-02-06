# tirehub-system

ETL-система для приёма прайс-листов поставщиков и построения витрины поверх проверенных данных.  
Ключевой принцип: SSOT (append-only) + NDJSON-first, бизнес-логика только в Curated/Offers.

## Docs
- **ETL Canon v1 (обязателен)**: [docs/etl/ETL_CANON_V1.md](docs/etl/ETL_CANON_V1.md)

## Architecture
Emitter → Gate → Ingestion → Curated / Offers

## Run (Kolobox end-to-end)
- VPS (etl): scripts/run_kolobox_full_v1.sh (emitter → gate → ingest → curated)

## Repo layout (WIP)
- `docs/` — спецификации и контракты
- `mappings/` — mapping-файлы поставщиков
- `inputs/` — входные файлы (локально/не коммитить)
- `out/` — артефакты прогонов (локально/не коммитить)
