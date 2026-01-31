-- v1.4a SSOT boundaries + curated artifacts + API surface

-- === SCHEMAS ===
CREATE SCHEMA IF NOT EXISTS ssot_ingestion;
CREATE SCHEMA IF NOT EXISTS ssot_curated_internal;
CREATE SCHEMA IF NOT EXISTS ssot_curated_api;
CREATE SCHEMA IF NOT EXISTS ssot_catalog;

-- === ROLES ===
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='etl_writer') THEN CREATE ROLE etl_writer; END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='downstream_reader') THEN CREATE ROLE downstream_reader; END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='catalog_writer') THEN CREATE ROLE catalog_writer; END IF;
END$$;

REVOKE ALL ON SCHEMA ssot_curated_internal FROM PUBLIC;
GRANT USAGE ON SCHEMA ssot_curated_api TO downstream_reader;
GRANT USAGE ON SCHEMA ssot_curated_api TO etl_writer;

-- === INGESTION (минимум) ===
CREATE TABLE IF NOT EXISTS ssot_ingestion.canonical_snapshots (
  snapshot_id UUID PRIMARY KEY,
  ruleset_versions JSONB NOT NULL,
  decomposer_version TEXT NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ssot_ingestion.canonical_items_source (
  id UUID PRIMARY KEY,
  snapshot_id UUID NOT NULL REFERENCES ssot_ingestion.canonical_snapshots(snapshot_id),
  supplier_id TEXT NOT NULL,
  raw JSONB NOT NULL,
  quality_flags JSONB DEFAULT '[]'::jsonb
);

-- === WAREHOUSES (стабильные ключи + aliases) ===
CREATE TABLE IF NOT EXISTS ssot_ingestion.warehouse_keys (
  warehouse_key TEXT PRIMARY KEY,
  display_name TEXT NOT NULL,
  is_active BOOLEAN DEFAULT true,
  created_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ssot_ingestion.warehouse_aliases (
  supplier_id TEXT NOT NULL,
  supplier_warehouse_name TEXT NOT NULL,
  warehouse_key TEXT NOT NULL REFERENCES ssot_ingestion.warehouse_keys(warehouse_key),
  first_seen_at TIMESTAMP DEFAULT now(),
  last_seen_at TIMESTAMP,
  PRIMARY KEY (supplier_id, supplier_warehouse_name)
);

-- === CURATED ARTIFACTS ===
CREATE TABLE IF NOT EXISTS ssot_curated_internal.curated_artifacts (
  artifact_id UUID PRIMARY KEY,
  snapshot_id UUID NOT NULL,
  curated_version TEXT NOT NULL,
  fingerprint BYTEA NOT NULL UNIQUE,
  fingerprint_input JSONB NOT NULL,
  checksum BYTEA,
  published_at TIMESTAMP NOT NULL DEFAULT now(),
  published_by TEXT NOT NULL,
  status TEXT DEFAULT 'active'
);

CREATE TABLE IF NOT EXISTS ssot_curated_internal.artifact_pointers (
  environment TEXT NOT NULL,
  channel TEXT NOT NULL,
  artifact_id UUID NOT NULL REFERENCES ssot_curated_internal.curated_artifacts(artifact_id),
  valid_from TIMESTAMP NOT NULL,
  valid_to TIMESTAMP,
  reason TEXT,
  PRIMARY KEY (environment, channel, valid_from)
);

CREATE TABLE IF NOT EXISTS ssot_curated_internal.offers_v1 (
  artifact_id UUID NOT NULL REFERENCES ssot_curated_internal.curated_artifacts(artifact_id),
  offer_id UUID NOT NULL,
  canonical_item_id UUID NOT NULL,
  supplier_id TEXT NOT NULL,
  warehouse_key TEXT NOT NULL,
  sku_candidate_key TEXT NOT NULL,
  price DECIMAL,
  qty INTEGER,
  currency TEXT,
  quality_flags JSONB DEFAULT '[]'::jsonb,
  PRIMARY KEY (artifact_id, offer_id)
);

-- === API SURFACE ===
CREATE OR REPLACE FUNCTION ssot_curated_api.get_current_artifact(
  p_environment TEXT DEFAULT 'production',
  p_channel TEXT DEFAULT 'default'
) RETURNS UUID AS $$
  SELECT artifact_id
  FROM ssot_curated_internal.artifact_pointers
  WHERE environment=p_environment AND channel=p_channel
    AND valid_from<=now() AND (valid_to IS NULL OR valid_to>now())
  ORDER BY valid_from DESC LIMIT 1;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION ssot_curated_api.get_offers_by_sku(
  p_artifact_id UUID,
  p_sku_candidate_key TEXT
) RETURNS TABLE (
  offer_id UUID,
  supplier_id TEXT,
  warehouse_key TEXT,
  price DECIMAL,
  qty INTEGER,
  currency TEXT,
  quality_flags JSONB
) AS $$
  SELECT offer_id, supplier_id, warehouse_key, price, qty, currency, quality_flags
  FROM ssot_curated_internal.offers_v1
  WHERE artifact_id=p_artifact_id
    AND sku_candidate_key=p_sku_candidate_key
    AND NOT (quality_flags ? 'blocked_for_aggregation');
$$ LANGUAGE SQL;

GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA ssot_curated_api TO downstream_reader;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA ssot_curated_api TO etl_writer;

-- === MISSING: publish_curated (stub) ===
CREATE OR REPLACE FUNCTION ssot_curated_api.publish_curated(
  p_snapshot_id UUID,
  p_curated_version TEXT DEFAULT 'v1',
  p_published_by TEXT DEFAULT 'system'
) RETURNS UUID AS $$
DECLARE
  v_artifact_id UUID;
BEGIN
  -- Заглушка: фиксируем сам факт публикации, но не генерим offers_v1
  v_artifact_id := gen_random_uuid();

  INSERT INTO ssot_curated_internal.curated_artifacts(
    artifact_id, snapshot_id, curated_version, fingerprint, fingerprint_input, published_by
  ) VALUES (
    v_artifact_id,
    p_snapshot_id,
    p_curated_version,
    decode(substr(encode(digest((p_snapshot_id::text || '|' || p_curated_version)::bytea, 'sha256'), 'hex'), 1, 64), 'hex'),          -- placeholder
    jsonb_build_object('snapshot_id', p_snapshot_id, 'curated_version', p_curated_version),
    p_published_by
  );

  RETURN v_artifact_id;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;


-- === FK provenance (optional but adds traceability) ===
ALTER TABLE ssot_curated_internal.offers_v1
  ADD CONSTRAINT IF NOT EXISTS fk_offers_v1_canonical_item
  FOREIGN KEY (canonical_item_id)
  REFERENCES ssot_ingestion.canonical_items_source(id);

-- === INDEXES ===
CREATE INDEX IF NOT EXISTS idx_offers_v1_artifact_sku
  ON ssot_curated_internal.offers_v1(artifact_id, sku_candidate_key);

CREATE INDEX IF NOT EXISTS idx_pointers_env_channel_time
  ON ssot_curated_internal.artifact_pointers(environment, channel, valid_from DESC);

-- === BASIC GRANTS for ETL writer ===
GRANT USAGE ON SCHEMA ssot_ingestion TO etl_writer;
GRANT INSERT, SELECT ON ssot_ingestion.canonical_snapshots TO etl_writer;
GRANT INSERT, SELECT ON ssot_ingestion.canonical_items_source TO etl_writer;
GRANT INSERT, SELECT ON ssot_ingestion.warehouse_keys TO etl_writer;
GRANT INSERT, SELECT ON ssot_ingestion.warehouse_aliases TO etl_writer;

