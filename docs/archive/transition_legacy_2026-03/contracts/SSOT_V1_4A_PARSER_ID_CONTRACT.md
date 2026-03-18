# SSOT v1.4a: parser_id contract

## Purpose
`ssot_ingestion.canonical_snapshots.parser_id` is a mandatory technical discriminator that separates:
- seed/smoke infrastructure runs
- ingestion (real supplier pricelists) runs

This is required to keep observability and P0 gates correct while using the same SSOT tables for smoke and ingestion.

## Allowed semantics
- `parser_id = 'seed'` — reserved for seed/smoke contour
- `parser_id = '<supplier>_<format>_v<version>'` — ingestion contour identifier (example: `kolobox_xls_v1`)

## Hard rules
1. Seed/smoke MUST explicitly write `parser_id='seed'` (no reliance on defaults).
2. Ingestion MUST explicitly write its own `parser_id` (never `'seed'`).
3. Any ingestion P0 gate that reads history MUST filter by:
   - `parser_id = '<ingestion_parser_id>'`
   - `status = 'success'` (or the authoritative success state in v1.4a)
4. `parser_id` is NOT a business attribute. It is an infrastructure tag.

## Canonical query (previous successful snapshot)

```sql
SELECT *
FROM ssot_ingestion.canonical_snapshots
WHERE supplier_id = $1
  AND parser_id   = $2
  AND status      = 'success'
ORDER BY created_at DESC
LIMIT 1;
```
