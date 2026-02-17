#!/usr/bin/env bash
set -euo pipefail

# create/rotate DB creds for tirehub on a host where you can run psql as postgres
# NOTE: password is rotated on each run.

DB_NAME="${DB_NAME:-tirehub}"
DB_USER="${DB_USER:-tirehub}"
DB_HOST="${DB_HOST:-89.111.171.170}"
DB_PORT="${DB_PORT:-5432}"

if ! command -v psql >/dev/null 2>&1; then
  echo "ERROR: psql not found" >&2
  exit 2
fi

if command -v pg_isready >/dev/null 2>&1; then
  pg_isready -q || { echo "ERROR: postgres not ready" >&2; exit 2; }
fi

PW="$(python3 - <<'PY'
import secrets, string
alphabet = string.ascii_letters + string.digits
print("".join(secrets.choice(alphabet) for _ in range(32)))
PY
)"

# choose runner (must execute as postgres)
if command -v sudo >/dev/null 2>&1; then
  PSQL="sudo -u postgres psql -v ON_ERROR_STOP=1"
  CREATEDB="sudo -u postgres createdb"
else
  PSQL="psql -U postgres -v ON_ERROR_STOP=1"
  CREATEDB="createdb -U postgres"
fi

# 1) create/rotate role (allowed in DO)
eval "$PSQL" <<SQL
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${DB_USER}') THEN
    CREATE ROLE ${DB_USER} LOGIN PASSWORD '${PW}';
  ELSE
    ALTER ROLE ${DB_USER} LOGIN PASSWORD '${PW}';
  END IF;
END
\$\$;
SQL

# 2) create DB (cannot be inside DO). Idempotent check + createdb.
DB_EXISTS="$(eval "$PSQL" -tAc "SELECT 1 FROM pg_database WHERE datname='${DB_NAME}'" || true)"
if [ "${DB_EXISTS}" != "1" ]; then
  eval "$CREATEDB" -O "${DB_USER}" "${DB_NAME}"
fi

# 3) privileges
eval "$PSQL" -v ON_ERROR_STOP=1 -c "GRANT ALL PRIVILEGES ON DATABASE ${DB_NAME} TO ${DB_USER};"

echo "OK: role=user=${DB_USER}"
echo "OK: database=${DB_NAME}"
echo "NOTE: password rotated on each run"
echo "PASSWORD=${PW}"
echo "DB_URL=postgresql://${DB_USER}:${PW}@${DB_HOST}:${DB_PORT}/${DB_NAME}"
