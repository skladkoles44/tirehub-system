#!/usr/bin/env bash
set -euo pipefail

DB="${1:-tirehub_etl}"
KEEP_DAYS="${KEEP_DAYS:-7}"
LIST_FILE="scripts/smoke/ssot_v1_4a_migrations.list"

ts(){ date '+%Y-%m-%d %H:%M:%S'; }
die(){ echo "[$(ts)] ERROR: $*" >&2; exit 1; }

echo "[$(ts)] == SSOT v1.4a seed =="
echo "[$(ts)] DB=$DB"

# migrations as postgres
sudo -n true 2>/dev/null || die "sudo -n is required (configure NOPASSWD for etl on VPS)"
echo "[$(ts)] Applying migrations as postgres (sudo -n)..."
while IFS= read -r f; do
  [[ -z "$f" ]] && continue
  echo "[$(ts)]   apply: $f"
  sudo -n -u postgres psql -d "$DB" -v ON_ERROR_STOP=1 -P pager=off -f "sql/migrations/$f" >/dev/null
done < "$LIST_FILE"
echo "[$(ts)] Migrations: OK"

# seed + bridge write as postgres, return run_id via SELECT (single line)
RUN_ID="$(sudo -n -u postgres psql -d "$DB" -v ON_ERROR_STOP=1 -P pager=off -At <<SQL
WITH ins AS (
  DELETE FROM ssot_catalog.smoke_runs_v14a
   WHERE created_at < now() - make_interval(days => ${KEEP_DAYS})
  RETURNING 1
),
gen AS (
  SELECT
    gen_random_uuid() AS run_id,
    gen_random_uuid() AS snapshot_id,
    gen_random_uuid() AS item_id,
    gen_random_uuid() AS artifact_id
),
seed AS (
  -- warehouse infra (idempotent)
  INSERT INTO ssot_ingestion.warehouse_keys(warehouse_key, display_name)
  VALUES ('seed', 'msk_dc', 'Moscow DC (smoke)'), ('spb_dc', 'SPB DC (smoke)')
  ON CONFLICT (warehouse_key) DO UPDATE SET display_name = EXCLUDED.display_name
  RETURNING 1
),
alias AS (
  INSERT INTO ssot_ingestion.warehouse_aliases(supplier_id, supplier_warehouse_name, warehouse_key)
  SELECT 'test_supplier','msk_warehouse','msk_dc'
  UNION ALL SELECT 'test_supplier','spb_stock','spb_dc'
  UNION ALL SELECT 'test_supplier','__default__','msk_dc'
  ON CONFLICT (supplier_id, supplier_warehouse_name) DO UPDATE SET warehouse_key = EXCLUDED.warehouse_key
  RETURNING 1
),
snap AS (
  INSERT INTO ssot_ingestion.canonical_snapshots(
    parser_id, snapshot_id, ruleset_versions, decomposer_version, created_at, status, sealed_at
  )
  SELECT
    'seed'::text,
    g.snapshot_id,
    '{}'::jsonb,
    'smoke_v1_4a',
    now() - interval '5 minutes',
    'open',
    NULL
  FROM gen g
  RETURNING snapshot_id
),
item AS (
  INSERT INTO ssot_ingestion.canonical_items_source(id, snapshot_id, supplier_id, raw, quality_flags)
  SELECT
    g.item_id,
    g.snapshot_id,
    'test_supplier',
    jsonb_build_object(
      'supplier_warehouse_name', 'msk_warehouse',
      'sku_candidate_key', 'michelin|225/65r17|102h|smoke',
      'price', '8500.00',
      'qty', '10',
      'currency', 'RUB'
    ),
    '[]'::jsonb
  FROM gen g
  RETURNING id
),
seal AS (
  UPDATE ssot_ingestion.canonical_snapshots cs
     SET status='sealed', sealed_at=now()
    FROM gen g
   WHERE cs.snapshot_id=g.snapshot_id
  RETURNING cs.snapshot_id
),
bridge AS (
  INSERT INTO ssot_catalog.smoke_runs_v14a(run_id, snapshot_id, artifact_id, item_id, db_name, notes)
  SELECT
    g.run_id, g.snapshot_id, g.artifact_id, g.item_id, current_database(),
    jsonb_build_object('seed','v1.4a','supplier_id','test_supplier')
  FROM gen g
  RETURNING run_id
)
SELECT run_id::text FROM bridge;
SQL
)"
[[ -n "$RUN_ID" ]] || die "seed failed: RUN_ID is empty"

# validation (as postgres): bridge exists + snapshot sealed
sudo -n -u postgres psql -d "$DB" -v ON_ERROR_STOP=1 -P pager=off -At <<SQL >/dev/null
SELECT 1 FROM ssot_catalog.smoke_runs_v14a WHERE run_id='${RUN_ID}'::uuid;
SELECT 1 FROM ssot_ingestion.canonical_snapshots cs
JOIN ssot_catalog.smoke_runs_v14a r ON r.snapshot_id=cs.snapshot_id
WHERE r.run_id='${RUN_ID}'::uuid AND cs.status='sealed' AND cs.sealed_at IS NOT NULL;
SQL

echo "RUN_ID=$RUN_ID"
