-- schema_v1.sql (contract-level SSOT)
-- Minimal coherent subset: snapshots + canonical_items_source + audit + signatures + downstream views.

CREATE TABLE IF NOT EXISTS canonical_snapshots (
  snapshot_id TEXT PRIMARY KEY,
  parent_snapshot_id TEXT REFERENCES canonical_snapshots(snapshot_id),
  ruleset_versions TEXT NOT NULL,       -- JSON string
  decomposer_version TEXT NOT NULL,
  snapshot_type TEXT NOT NULL,          -- initial|backfill_batch|backfill_signature
  created_at TEXT NOT NULL,
  created_by TEXT NOT NULL,
  backfill_reason TEXT,
  signature_core TEXT,
  supplier_id TEXT,
  date_range TEXT,
  total_rows INTEGER,
  rows_modified INTEGER,
  snapshot_status TEXT DEFAULT 'processing',
  snapshot_checksum TEXT
);

CREATE TABLE IF NOT EXISTS canonical_items_source (
  id TEXT PRIMARY KEY,
  snapshot_id TEXT NOT NULL REFERENCES canonical_snapshots(snapshot_id),
  version_created_at TEXT NOT NULL,

  -- ETL v3.3 trace
  supplier TEXT NOT NULL,
  source_file TEXT NOT NULL,
  source_table TEXT NOT NULL,
  row_index INTEGER NOT NULL,
  qty_column_index INTEGER NOT NULL,
  raw TEXT NOT NULL,                    -- JSON array string

  -- business fields (subset)
  article TEXT,
  name TEXT,
  brand TEXT,
  qty REAL,
  price REAL,
  warehouse TEXT,
  currency TEXT,

  -- quality/versioning (operational layer)
  processed_at TEXT NOT NULL,
  quality_flags TEXT,                   -- JSON array string
  supplier_quality_state TEXT
);

CREATE INDEX IF NOT EXISTS idx_canonical_items_supplier_snapshot
  ON canonical_items_source(supplier, snapshot_id);

CREATE TABLE IF NOT EXISTS decomposition_row_audit (
  id TEXT PRIMARY KEY,
  canonical_item_id TEXT NOT NULL REFERENCES canonical_items_source(id),
  signature_core TEXT NOT NULL,          -- 16 hex
  signature_detail TEXT,                 -- JSON
  field TEXT NOT NULL,
  l1_claim TEXT NOT NULL,                -- JSON
  l2_challenge TEXT,                     -- JSON
  confidence_delta REAL,
  raw_text TEXT NOT NULL,
  context_tokens TEXT,                   -- JSON array
  detected_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_row_audit_signature_core
  ON decomposition_row_audit(signature_core, canonical_item_id);

CREATE TABLE IF NOT EXISTS decomposition_conflict_signatures (
  signature_core TEXT PRIMARY KEY,       -- 16 hex
  core_key TEXT NOT NULL,                -- preimage (canonical cause)
  normalization_version TEXT NOT NULL,    -- e.g. 1.0
  field TEXT NOT NULL,
  description TEXT,
  first_seen_at TEXT NOT NULL,
  last_seen_at TEXT NOT NULL,
  total_count INTEGER DEFAULT 1,
  affected_suppliers TEXT,               -- JSON array
  estimated_impact_score REAL DEFAULT 0.0,

  status TEXT DEFAULT 'new',             -- new|triaged|investigating|patched|wonfix|needs_data
  patch_rule_version TEXT,
  backfill_status TEXT
);

-- downstream view (filters blocked_for_aggregation)
-- quality_flags is JSON array string; filter uses LIKE as placeholder at contract level.
CREATE VIEW IF NOT EXISTS canonical_items_live AS
SELECT *
FROM canonical_items_source
WHERE quality_flags IS NULL OR quality_flags NOT LIKE '%"blocked_for_aggregation"%';
