BEGIN;

-- v1.4a: make status_v14a authoritative for integrity checks

CREATE OR REPLACE FUNCTION ssot_curated_internal.generate_offers_v1(
  p_snapshot_id uuid,
  p_ruleset_version text,
  p_artifact_id uuid
) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $function$
DECLARE
  v_lock_id bigint;
  v_status text;
BEGIN
  v_lock_id := hashtext('generate_offers_v1|' || p_artifact_id::text);
  PERFORM pg_advisory_xact_lock(v_lock_id);

  PERFORM ssot_curated_internal.assert_can_generate(p_snapshot_id);

  SELECT status INTO v_status
  FROM ssot_curated_internal.curated_artifacts
  WHERE artifact_id = p_artifact_id;

  IF v_status IS NULL THEN
    INSERT INTO ssot_curated_internal.curated_artifacts(
      artifact_id, snapshot_id, curated_version,
      fingerprint, generation_fingerprint, fingerprint_input,
      generated_at, status, status_v14a, published_by
    ) VALUES (
      p_artifact_id, p_snapshot_id, p_ruleset_version,
      digest((p_snapshot_id::text || '|' || p_ruleset_version || '|' || p_artifact_id::text)::text,'sha256'),
      digest((p_snapshot_id::text || '|' || p_ruleset_version || '|' || p_artifact_id::text)::text,'sha256'),
      jsonb_build_object('snapshot_id', p_snapshot_id, 'curated_version', p_ruleset_version, 'artifact_id', p_artifact_id),
      now(), 'generated', 'generated', 'generator'
    );
  ELSIF v_status IS DISTINCT FROM 'generated' THEN
    RAISE EXCEPTION 'generate_offers_v1: artifact % status=% (expected generated or null)', p_artifact_id, v_status;
  END IF;

  -- if legacy row has status=generated but status_v14a is NULL - heal it
  UPDATE ssot_curated_internal.curated_artifacts
  SET status_v14a = 'generated'
  WHERE artifact_id = p_artifact_id
    AND status = 'generated'
    AND status_v14a IS NULL;

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
    wa.warehouse_key,
    COALESCE(cis.raw->>'sku_candidate_key', 'unknown_sku'),
    NULLIF(cis.raw->>'price','')::numeric,
    NULLIF(cis.raw->>'qty','')::integer,
    COALESCE(cis.raw->>'currency', 'RUB'),
    COALESCE(cis.quality_flags, '[]'::jsonb)
  FROM ssot_ingestion.canonical_items_source cis
  JOIN ssot_ingestion.warehouse_aliases wa
    ON wa.supplier_id = cis.supplier_id
   AND wa.supplier_warehouse_name = COALESCE(NULLIF(cis.raw->>'supplier_warehouse_name',''), '__default__')
  WHERE cis.snapshot_id = p_snapshot_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'generate_offers_v1: no items mapped to warehouse (supplier_warehouse_name missing and no __default__ alias?) snapshot=%', p_snapshot_id;
  END IF;

  UPDATE ssot_curated_internal.curated_artifacts
  SET generated_at = now()
  WHERE artifact_id = p_artifact_id;
END;
$function$;

CREATE OR REPLACE FUNCTION ssot_curated_api.publish_artifact(
  p_artifact_id uuid,
  p_published_by text DEFAULT 'system'::text
) RETURNS uuid
LANGUAGE plpgsql
SECURITY DEFINER
AS $function$
DECLARE
  v_current_status text;
  v_current_status_v14a text;
BEGIN
  SELECT status, status_v14a
    INTO v_current_status, v_current_status_v14a
  FROM ssot_curated_internal.curated_artifacts
  WHERE artifact_id = p_artifact_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'publish_artifact: artifact % not found', p_artifact_id;
  END IF;

  IF v_current_status_v14a IS DISTINCT FROM 'generated' THEN
    RAISE EXCEPTION 'publish_artifact: artifact % status_v14a=% (expected generated)', p_artifact_id, v_current_status_v14a;
  END IF;

  UPDATE ssot_curated_internal.curated_artifacts
  SET
    status_v14a  = 'published',
    published_at = NOW(),
    published_by = p_published_by
  WHERE artifact_id = p_artifact_id;

  RETURN p_artifact_id;
END;
$function$;

-- backfill: if somebody already generated artifacts but forgot status_v14a
UPDATE ssot_curated_internal.curated_artifacts
SET status_v14a = 'generated'
WHERE status = 'generated' AND status_v14a IS NULL;

COMMIT;
