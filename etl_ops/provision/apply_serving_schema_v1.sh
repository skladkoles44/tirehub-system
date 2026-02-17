#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SQL_FILE="$REPO_ROOT/sql/serving/serving_schema_v1.sql"

command -v psql >/dev/null 2>&1 || { echo "ERROR: psql not found (install postgresql-client)"; exit 1; }

DB_URL="${DB_URL:-}"
if [[ -z "$DB_URL" ]]; then
  echo "ERROR: DB_URL is not set"
  echo "Example: export DB_URL=postgresql://user:pass@host:5432/dbname"
  exit 1
fi

echo "Applying serving schema v1..."
echo "SQL file: $SQL_FILE"
psql "$DB_URL" -f "$SQL_FILE" -v ON_ERROR_STOP=1

echo ""
echo "Verification:"
psql -v ON_ERROR_STOP=1 "$DB_URL" -c "\
SELECT schemaname, tablename, tableowner\
FROM pg_tables\
WHERE schemaname = 'public'\
  AND tablename IN ('master_products','supplier_sku_map','supplier_offers_latest','schema_migrations')\
ORDER BY tablename;\
"\
 -v ON_ERROR_STOP=1

echo ""
echo "OK: schema v1 applied"
