# 2026-03-18 — repo transition archive wave2

## Что сделано
Внутри repo выполнена вторая волна очистки: старые, но полезные transition-docs перенесены в docs/archive/transition_legacy_2026-03.

## Что перенесено
- docs/architecture/pipeline.md
- docs/architecture/PIPELINE_CANONICAL_FLOW.md
- docs/contracts/MAPPING_REGISTRY.md
- docs/contracts/NDJSON_INGESTION_PRODUCTION_CONTRACT.md
- docs/contracts/SSOT_V1_4A_PARSER_ID_CONTRACT.md
- docs/spec/canonical_storage_contract.md
- docs/spec/backfill_snapshots_v1.6_plus.md
- docs/spec/decomposer_v1.7_content_first_ssot.md
- docs/status/2026-03-11_ingestion_mvp.md

## Что оставлено живым
- current L0/L1 docs and contracts
- docs/journal/**
- docs/archive/**
- docs/spec/ETL_EXTRACTOR_SPEC_FINAL_v3_3.md
- docs/spec/parser_framework_v1.md
- current scripts/etl, normalization, probe, dq, connectors, serving, curated

## Что ещё осталось на следующий шаг
- patch legacy references in:
  - config/suppliers_registry.yaml
  - scripts/etl/unknown_header_harvest.py
  - scripts/probe/mass_probe_v2.py

## Результат
Repo стал чище: старые transition-docs больше не лежат в живых разделах architecture/contracts/spec/status.
