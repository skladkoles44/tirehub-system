-- SSOT v1.4a: add parser_id discriminator for canonical_snapshots
-- Domain split: seed/smoke vs ingestion (real pricelists)
-- Static, explicit, idempotent. Requires ssot_ingestion.canonical_snapshots to exist.

ALTER TABLE ssot_ingestion.canonical_snapshots
  ADD COLUMN IF NOT EXISTS parser_id TEXT;

UPDATE ssot_ingestion.canonical_snapshots
   SET parser_id = 'seed'
 WHERE parser_id IS NULL;

ALTER TABLE ssot_ingestion.canonical_snapshots
  ALTER COLUMN parser_id SET DEFAULT 'seed';

ALTER TABLE ssot_ingestion.canonical_snapshots
  ALTER COLUMN parser_id SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_canonical_snapshots_parser_id
  ON ssot_ingestion.canonical_snapshots(parser_id);
