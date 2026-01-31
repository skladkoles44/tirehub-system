# Canonical storage contract (v1)

## Goal
Зафиксировать границу ответственности между:
- артефактами Extractor v3.3 (NDJSON),
- хранилищем/версионированием (SQLite snapshots),
- downstream доступом (VIEW-only).

## Rule (hard)
1) `out/<supplier>/canonical.ndjson` соответствует ETL v3.3 JSON Schema и НЕ обязан содержать snapshot/quality/version поля.
2) Версионность и качество хранятся в SQLite таблице `canonical_items_source`:
   - snapshot_id
   - ruleset_versions (JSON)
   - decomposer_version
   - processed_at
   - quality_flags (array/text/json)
   - supplier_quality_state
3) Downstream читает только через VIEW (`canonical_items_live`), который фильтрует `blocked_for_aggregation`.

## Rationale
- NDJSON остаётся простым экстракторным контрактом (универсальный для любых поставщиков).
- Operational контуры (snapshots/backfill/quality gating) закреплены на уровне БД и governance.
