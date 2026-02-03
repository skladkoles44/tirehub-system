# SSOT v1.4a ingestion preflight lock (fact)

Этот файл фиксирует подтверждённое состояние окружения перед включением ingestion пайплайна.

## Schemas
- ssot_ingestion
- ssot_catalog

## Required tables (present)
- ssot_ingestion.canonical_items_source
- ssot_ingestion.canonical_snapshots
- ssot_ingestion.warehouse_aliases

## canonical_snapshots discriminator (present)
- column: ssot_ingestion.canonical_snapshots.parser_id
- nullable: NO
- default: 'seed'

## Indexes (present)
- ssot_ingestion.canonical_snapshots: idx_canonical_snapshots_parser_id

## Grants for role `etl` on ssot_ingestion (sufficient)
- SELECT, INSERT, UPDATE, DELETE, TRUNCATE
