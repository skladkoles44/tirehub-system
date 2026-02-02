BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE OR REPLACE FUNCTION ssot_curated_internal.assert_can_generate(p_snapshot_id uuid)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $function$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM ssot_ingestion.canonical_snapshots
    WHERE snapshot_id = p_snapshot_id AND status = 'sealed'
  ) THEN
    RAISE EXCEPTION 'assert_can_generate: snapshot % is not sealed', p_snapshot_id;
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM ssot_ingestion.canonical_items_source
    WHERE snapshot_id = p_snapshot_id
  ) THEN
    RAISE EXCEPTION 'assert_can_generate: snapshot % has no items', p_snapshot_id;
  END IF;

  IF NOT EXISTS (SELECT 1 FROM ssot_ingestion.warehouse_keys LIMIT 1) THEN
    RAISE EXCEPTION 'assert_can_generate: warehouse_keys is empty';
  END IF;
END;
$function$;

CREATE OR REPLACE FUNCTION ssot_curated_internal.generate_offers_v1(
  p_snapshot_id uuid,
  p_ruleset_version text,
  p_artifact_id uuid
) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $function$
DECLARE
  v_fp bytea;
  v_lock bigint;
  v_existing text;
BEGIN
  -- защита от параллельных вызовов на один artifact_id
  v_lock := hashtext('generate_offers_v1|' || p_artifact_id::text);
  PERFORM pg_advisory_xact_lock(v_lock);

  PERFORM ssot_curated_internal.assert_can_generate(p_snapshot_id);

  SELECT status_v14a INTO v_existing
  FROM ssot_curated_internal.curated_artifacts
  WHERE artifact_id = p_artifact_id;

  IF NOT FOUND THEN
    v_fp := digest((p_snapshot_id::text || '|' || p_ruleset_version || '|' || p_artifact_id::text)::text, 'sha256');

    INSERT INTO ssot_curated_internal.curated_artifacts(
      artifact_id, snapshot_id, curated_version,
      fingerprint, generation_fingerprint,
      fingerprint_input, generated_at, status_v14a,
      published_by
    ) VALUES (
      p_artifact_id, p_snapshot_id, p_ruleset_version,
      v_fp, v_fp,
      jsonb_build_object('snapshot_id', p_snapshot_id, 'curated_version', p_ruleset_version, 'artifact_id', p_artifact_id),
      NOW(), 'generated',
      'generator'
    );
  ELSIF v_existing IS DISTINCT FROM 'generated' THEN
    RAISE EXCEPTION 'generate_offers_v1: artifact % status_v14a=% (expected generated or null)', p_artifact_id, v_existing;
  END IF;

  -- идемпотентность по artifact_id
  DELETE FROM ssot_curated_internal.offers_v1 WHERE artifact_id = p_artifact_id;

  INSERT INTO ssot_curated_internal.offers_v1(
    artifact_id, offer_id, canonical_item_id,
    supplier_id, warehouse_key, sku_candidate_key,
    price, qty, currency, quality_flags
  )
  SELECT
    p_artifact_id,
    gen_random_uuid(),
    cis.id,
    cis.supplier_id,
    COALESCE(
      wa.warehouse_key,
      (SELECT wk.warehouse_key FROM ssot_ingestion.warehouse_keys wk WHERE wk.warehouse_key = cis.raw->>'warehouse_key' LIMIT 1)
    ) AS warehouse_key,
    COALESCE(cis.raw->>'sku_candidate_key', 'unknown_sku'),
    NULLIF(cis.raw->>'price','')::numeric,
    NULLIF(cis.raw->>'qty','')::int,
    COALESCE(cis.raw->>'currency','RUB'),
    COALESCE(cis.quality_flags,'[]'::jsonb)
  FROM ssot_ingestion.canonical_items_source cis
  LEFT JOIN ssot_ingestion.warehouse_aliases wa
    ON wa.supplier_id = cis.supplier_id
   AND wa.supplier_warehouse_name = cis.raw->>'supplier_warehouse_name'
  WHERE cis.snapshot_id = p_snapshot_id
    AND COALESCE(
      wa.warehouse_key,
      (SELECT wk.warehouse_key FROM ssot_ingestion.warehouse_keys wk WHERE wk.warehouse_key = cis.raw->>'warehouse_key' LIMIT 1)
    ) IS NOT NULL;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'generate_offers_v1: no items with valid warehouse mapping for snapshot %', p_snapshot_id;
  END IF;

  UPDATE ssot_curated_internal.curated_artifacts
  SET generated_at = NOW()
  WHERE artifact_id = p_artifact_id;
END;
$function$;

CREATE OR REPLACE FUNCTION ssot_curated_internal.assert_artifact_integrity(p_artifact_id uuid)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $function$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM ssot_curated_internal.curated_artifacts
    WHERE artifact_id = p_artifact_id AND status_v14a IN ('generated','published')
  ) THEN
    RAISE EXCEPTION 'assert_artifact_integrity: artifact % not found or bad status_v14a', p_artifact_id;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM ssot_curated_internal.offers_v1
    WHERE artifact_id = p_artifact_id
  ) THEN
    RAISE EXCEPTION 'assert_artifact_integrity: artifact % has no offers', p_artifact_id;
  END IF;
END;
$function$;

-- индексы (безопасно)
CREATE INDEX IF NOT EXISTS idx_canonical_items_source_snapshot_id
ON ssot_ingestion.canonical_items_source(snapshot_id);

CREATE INDEX IF NOT EXISTS idx_offers_v1_artifact_id
ON ssot_curated_internal.offers_v1(artifact_id);

COMMIT;
