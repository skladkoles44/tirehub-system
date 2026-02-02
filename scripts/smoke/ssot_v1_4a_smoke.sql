BEGIN;

SELECT pg_advisory_xact_lock(hashtext('ssot_smoke_v1_4a|production|default'));

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_proc p
    WHERE p.pronamespace='ssot_curated_internal'::regnamespace
      AND p.proname='assert_can_generate'
  ) THEN
    RAISE EXCEPTION 'SMOKE_GATE: missing ssot_curated_internal.assert_can_generate(uuid)';
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_proc p
    WHERE p.pronamespace='ssot_curated_internal'::regnamespace
      AND p.proname='generate_offers_v1'
  ) THEN
    RAISE EXCEPTION 'SMOKE_GATE: missing ssot_curated_internal.generate_offers_v1(uuid,text,uuid)';
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_proc p
    WHERE p.pronamespace='ssot_curated_internal'::regnamespace
      AND p.proname='assert_artifact_integrity'
  ) THEN
    RAISE EXCEPTION 'SMOKE_GATE: missing ssot_curated_internal.assert_artifact_integrity(uuid)';
  END IF;
END $$;

CREATE TEMP TABLE IF NOT EXISTS smoke_ctx(
  snapshot_id uuid NOT NULL,
  artifact_id uuid NOT NULL,
  item_id uuid NOT NULL,
  supplier_id text NOT NULL,
  supplier_wh_name text NOT NULL,
  sku_candidate_key text NOT NULL
) ON COMMIT DROP;

TRUNCATE smoke_ctx;

DO $$
DECLARE
  v_snapshot_id uuid := gen_random_uuid();
  v_artifact_id uuid := gen_random_uuid();
  v_item_id uuid := gen_random_uuid();
  v_supplier_id text := 'test_supplier';
  v_supplier_wh_name text := 'msk_warehouse';
  v_sku text := 'michelin|225/65r17|102h|smoke';

  v_has_publish_artifact boolean;
  v_has_warehouse_display_name boolean;
  v_has_warehouse_description boolean;
BEGIN
  INSERT INTO smoke_ctx(snapshot_id, artifact_id, item_id, supplier_id, supplier_wh_name, sku_candidate_key)
  VALUES (v_snapshot_id, v_artifact_id, v_item_id, v_supplier_id, v_supplier_wh_name, v_sku);

  SELECT EXISTS(
    SELECT 1 FROM pg_proc p
    WHERE p.pronamespace='ssot_curated_api'::regnamespace
      AND p.proname='publish_artifact'
  ) INTO v_has_publish_artifact;

  SELECT EXISTS(
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='ssot_ingestion'
      AND table_name='warehouse_keys'
      AND column_name='display_name'
  ) INTO v_has_warehouse_display_name;

  SELECT EXISTS(
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='ssot_ingestion'
      AND table_name='warehouse_keys'
      AND column_name='description'
  ) INTO v_has_warehouse_description;

  IF v_has_warehouse_display_name THEN
    EXECUTE $sql$
      INSERT INTO ssot_ingestion.warehouse_keys(warehouse_key, display_name)
      VALUES
        ('msk_dc', 'Moscow DC (smoke)'),
        ('spb_dc', 'SPB DC (smoke)')
      ON CONFLICT (warehouse_key) DO UPDATE
      SET display_name = EXCLUDED.display_name
    $sql$;
  ELSIF v_has_warehouse_description THEN
    EXECUTE $sql$
      INSERT INTO ssot_ingestion.warehouse_keys(warehouse_key, description)
      VALUES
        ('msk_dc', 'Moscow DC (smoke)'),
        ('spb_dc', 'SPB DC (smoke)')
      ON CONFLICT (warehouse_key) DO UPDATE
      SET description = EXCLUDED.description
    $sql$;
  ELSE
    EXECUTE $sql$
      INSERT INTO ssot_ingestion.warehouse_keys(warehouse_key)
      VALUES ('msk_dc'), ('spb_dc')
      ON CONFLICT (warehouse_key) DO NOTHING
    $sql$;
  END IF;

  INSERT INTO ssot_ingestion.warehouse_aliases(supplier_id, supplier_warehouse_name, warehouse_key)
  VALUES
    (v_supplier_id, v_supplier_wh_name, 'msk_dc'),
    (v_supplier_id, 'spb_stock', 'spb_dc')
  ON CONFLICT (supplier_id, supplier_warehouse_name) DO UPDATE
  SET warehouse_key = EXCLUDED.warehouse_key,
      last_seen_at = now();

  INSERT INTO ssot_ingestion.canonical_snapshots(
    parser_id, snapshot_id, ruleset_versions, decomposer_version, created_at, status, sealed_at
  ) VALUES (
    'seed',
    v_snapshot_id,
    '{}'::jsonb,
    'smoke_v1_4a',
    now() - interval '5 minutes',
    'open',
    NULL
  );

  INSERT INTO ssot_ingestion.canonical_items_source(
    id, snapshot_id, supplier_id, raw, quality_flags
  ) VALUES (
    v_item_id,
    v_snapshot_id,
    v_supplier_id,
    jsonb_build_object(
      'supplier_warehouse_name', v_supplier_wh_name,
      'sku_candidate_key', v_sku,
      'price', '8500.00',
      'qty', '10',
      'currency', 'RUB'
    ),
    '[]'::jsonb
  );

  UPDATE ssot_ingestion.canonical_snapshots
  SET status='sealed', sealed_at=now()
  WHERE snapshot_id=v_snapshot_id;

  PERFORM ssot_curated_internal.generate_offers_v1(v_snapshot_id, 'v1', v_artifact_id);
  PERFORM ssot_curated_internal.assert_artifact_integrity(v_artifact_id);

  IF v_has_publish_artifact THEN
    PERFORM ssot_curated_api.publish_artifact(v_artifact_id, 'smoke');
  ELSE
    PERFORM ssot_curated_api.publish_curated(v_snapshot_id, 'v1', 'smoke');
  END IF;
END $$;

SELECT 'ids' AS kind, snapshot_id, artifact_id, item_id, sku_candidate_key
FROM smoke_ctx;

SELECT
  'artifact' AS kind,
  ca.artifact_id,
  ca.snapshot_id,
  ca.curated_version,
  ca.status,
  ca.published_at,
  ca.published_by
FROM ssot_curated_internal.curated_artifacts ca
JOIN smoke_ctx ctx ON ctx.artifact_id = ca.artifact_id;

SELECT
  'offers_count' AS kind,
  ov.artifact_id,
  COUNT(*)::int AS cnt
FROM ssot_curated_internal.offers_v1 ov
JOIN smoke_ctx ctx ON ctx.artifact_id = ov.artifact_id
GROUP BY ov.artifact_id;

WITH cur AS (SELECT artifact_id FROM smoke_ctx LIMIT 1),
closed AS (
  UPDATE ssot_curated_internal.artifact_pointers
  SET valid_to = now()
  WHERE environment='production' AND channel='default' AND valid_to IS NULL
  RETURNING 1
),
ins AS (
  INSERT INTO ssot_curated_internal.artifact_pointers(
    environment, channel, artifact_id, valid_from, valid_to, reason
  )
  SELECT 'production','default',cur.artifact_id,now(),NULL,'smoke pointer (v1.4a)'
  FROM cur
  RETURNING environment, channel, artifact_id, valid_from, valid_to, reason
)
SELECT 'pointer_set' AS kind, environment, channel, artifact_id, valid_from, valid_to, reason
FROM ins;

WITH ptr AS (
  SELECT artifact_id, reason
  FROM ssot_curated_internal.artifact_pointers
  WHERE environment='production' AND channel='default' AND valid_to IS NULL
),
ptr_cnt AS (SELECT COUNT(*)::int AS c1 FROM ptr),
picked AS (SELECT reason AS c2 FROM ptr LIMIT 1),
cur AS (SELECT ssot_curated_api.get_current_artifact('production','default') AS c3),
flags AS (
  SELECT
    'ok_exactly_one_current='||((SELECT c1 FROM ptr_cnt)=1)::text
    ||'; ok_reason='||((SELECT c2 FROM picked)='smoke pointer (v1.4a)')::text
    ||'; ok_current_matches_pointer='||((SELECT c3 FROM cur)=(SELECT artifact_id FROM ptr LIMIT 1))::text
    AS c4
)
SELECT
  'assert_current_pointer' AS kind,
  (SELECT c1 FROM ptr_cnt) AS c1_current_pointers_count,
  (SELECT c2 FROM picked) AS c2_picked_reason,
  (SELECT c3 FROM cur) AS c3_current_artifact,
  (SELECT c4 FROM flags) AS c4_flags
UNION ALL
SELECT
  'ERROR_multiple_current_pointers' AS kind,
  (SELECT c1 FROM ptr_cnt) AS c1_current_pointers_count,
  NULL::text,
  NULL::uuid,
  NULL::text
WHERE (SELECT c1 FROM ptr_cnt) <> 1;

ROLLBACK;
