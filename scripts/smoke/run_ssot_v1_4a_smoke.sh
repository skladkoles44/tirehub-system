#!/usr/bin/env bash
set -euo pipefail

DB="${1:-tirehub_etl}"
SQL_FILE="scripts/smoke/ssot_v1_4a_smoke.sql"

if [[ ! -f "$SQL_FILE" ]]; then
  echo "NOT FOUND: $SQL_FILE" >&2
  exit 1
fi

echo "== SSOT v1.4a smoke =="
echo "DB=$DB"
echo "SQL=$SQL_FILE"
echo

if sudo -n true 2>/dev/null; then
  : # ok
else
  echo "NO_SUDO_CACHE" >&2
  exit 1
fi

# Run smoke (fail-fast), keep full output visible
sudo -n -u postgres psql -d "$DB" -v ON_ERROR_STOP=1 -f "$SQL_FILE"

echo
echo "OK: smoke passed"
