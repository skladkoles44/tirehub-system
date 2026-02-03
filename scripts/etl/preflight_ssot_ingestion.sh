#!/usr/bin/env bash
set -euo pipefail
DB="${1:-tirehub_etl}"

psql -X -q -t -d "$DB" <<'EOF'
SELECT 'schema' AS kind, nspname
FROM pg_namespace
WHERE nspname IN ('ssot_ingestion','ssot_catalog','ssot_curated')
ORDER BY nspname;

SELECT 'table' AS kind, table_schema, table_name
FROM information_schema.tables
WHERE table_schema='ssot_ingestion'
  AND table_name IN ('canonical_snapshots','canonical_items_source','warehouse_aliases')
ORDER BY table_name;

SELECT 'parser_id' AS kind, table_schema, is_nullable, column_default
FROM information_schema.columns
WHERE table_schema='ssot_ingestion'
  AND table_name='canonical_snapshots'
  AND column_name='parser_id';

SELECT 'idx' AS kind, schemaname, tablename, indexname
FROM pg_indexes
WHERE schemaname='ssot_ingestion'
  AND tablename='canonical_snapshots'
  AND indexdef ILIKE '%parser_id%';

SELECT 'grant' AS kind, table_schema, grantee, privilege_type
FROM information_schema.role_table_grants
WHERE table_schema='ssot_ingestion'
  AND table_name='canonical_snapshots'
ORDER BY grantee, privilege_type;
EOF
