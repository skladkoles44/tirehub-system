BEGIN;

SELECT pg_advisory_xact_lock(hashtext('ssot_curated_internal.artifact_pointers|production|default'));

WITH const AS (
  SELECT
    '00000000-0000-0000-0000-00000000a401'::uuid AS snapshot_id,
    '00000000-0000-0000-0000-00000000c401'::uuid AS offer_id,
    '00000000-0000-0000-0000-00000000d401'::uuid AS canonical_item_id
)
SELECT 'ids' AS kind, snapshot_id, offer_id FROM const;

DELETE FROM ssot_curated_internal.offers_v1
WHERE offer_id='00000000-0000-0000-0000-00000000c401'::uuid;

DELETE FROM ssot_curated_internal.curated_artifacts
WHERE snapshot_id='00000000-0000-0000-0000-00000000a401'::uuid AND curated_version='v1';

DELETE FROM ssot_ingestion.canonical_snapshots
WHERE snapshot_id='00000000-0000-0000-0000-00000000a401'::uuid;

INSERT INTO ssot_ingestion.canonical_snapshots(snapshot_id, ruleset_versions, decomposer_version)
VALUES ('00000000-0000-0000-0000-00000000a401'::uuid, '{}'::jsonb, 'smoke');

WITH pub AS (
  SELECT ssot_curated_api.publish_curated(
           '00000000-0000-0000-0000-00000000a401'::uuid,
           'v1',
           'smoke'
         ) AS artifact_id
)
SELECT 'published' AS kind, artifact_id
FROM pub
WHERE artifact_id IS NOT NULL;

WITH pub AS (
  SELECT artifact_id
  FROM ssot_curated_internal.curated_artifacts
  WHERE snapshot_id='00000000-0000-0000-0000-00000000a401'::uuid
    AND curated_version='v1'
),
close_prev AS (
  UPDATE ssot_curated_internal.artifact_pointers
  SET valid_to = now()
  WHERE environment='production'
    AND channel='default'
    AND valid_to IS NULL
  RETURNING artifact_id
),
close_stats AS (
  SELECT count(*) AS closed_rows FROM close_prev
),
close_assert AS (
  SELECT
    CASE WHEN (SELECT closed_rows FROM close_stats) <= 1
         THEN 1
         ELSE (SELECT 1/0)
    END AS ok
),
ins AS (
  INSERT INTO ssot_curated_internal.artifact_pointers(environment, channel, artifact_id, valid_from, valid_to, reason)
  SELECT 'production','default', pub.artifact_id, now(), NULL, 'smoke pointer'
  FROM pub
  RETURNING environment, channel, artifact_id, valid_from, valid_to, reason
)
SELECT 'pointer_set' AS kind, * FROM ins;

WITH ptr AS (
  SELECT artifact_id, reason, valid_from, valid_to
  FROM ssot_curated_internal.artifact_pointers
  WHERE environment='production'
    AND channel='default'
    AND valid_from <= now()
    AND (valid_to IS NULL OR valid_to > now())
),
ptr_cnt AS (
  SELECT count(*) AS cnt FROM ptr
),
ptr_list AS (
  SELECT artifact_id, reason, valid_from, valid_to
  FROM ptr
  ORDER BY valid_from DESC, artifact_id DESC
),
ptr_one AS (
  SELECT * FROM ptr_list
  LIMIT 1
),
cur AS (
  SELECT ssot_curated_api.get_current_artifact('production','default') AS current_artifact
)
SELECT
  'assert_current_pointer' AS kind,
  (SELECT cnt FROM ptr_cnt)::text AS c1_current_pointers_count,
  COALESCE((SELECT reason FROM ptr_one), '') AS c2_picked_reason,
  COALESCE((SELECT current_artifact::text FROM cur), '') AS c3_current_artifact,
  (
    'ptr_artifact=' || COALESCE((SELECT artifact_id::text FROM ptr_one), '') ||
    '; ok_exactly_one_current=' || ((SELECT cnt FROM ptr_cnt) = 1)::text ||
    '; ok_reason=' || (COALESCE((SELECT reason FROM ptr_one), '') = 'smoke pointer')::text ||
    '; ok_current_matches_pointer=' || ((SELECT current_artifact FROM cur) = (SELECT artifact_id FROM ptr_one))::text
  ) AS c4_flags
UNION ALL
SELECT
  'ERROR_multiple_current_pointers' AS kind,
  artifact_id::text AS c1_artifact_id,
  reason AS c2_reason,
  valid_from::text AS c3_valid_from,
  COALESCE(valid_to::text, '') AS c4_valid_to
FROM ptr_list
WHERE (SELECT cnt FROM ptr_cnt) > 1;

WITH ensure_snap AS (
  INSERT INTO ssot_ingestion.canonical_snapshots(snapshot_id, ruleset_versions, decomposer_version)
  VALUES (
    '00000000-0000-0000-0000-00000000a401'::uuid,
    '{}'::jsonb,
    'smoke'
  )
  ON CONFLICT (snapshot_id) DO NOTHING
  RETURNING snapshot_id
),
ensure_item AS (
  INSERT INTO ssot_ingestion.canonical_items_source(id, snapshot_id, supplier_id, raw, quality_flags)
  VALUES (
    '00000000-0000-0000-0000-00000000d401'::uuid,
    '00000000-0000-0000-0000-00000000a401'::uuid,
    'test_supplier',
    '{}'::jsonb,
    '[]'::jsonb
  )
  ON CONFLICT (id) DO UPDATE SET
    snapshot_id = EXCLUDED.snapshot_id,
    supplier_id = EXCLUDED.supplier_id,
    raw = EXCLUDED.raw
  RETURNING id
),
cur_art AS (
  SELECT ssot_curated_api.get_current_artifact('production','default') AS artifact_id
)
INSERT INTO ssot_curated_internal.offers_v1(
  artifact_id, offer_id, canonical_item_id, supplier_id, warehouse_key, sku_candidate_key,
  price, qty, currency, quality_flags
)
SELECT
  cur_art.artifact_id,
  '00000000-0000-0000-0000-00000000c401'::uuid,
  (SELECT id FROM ensure_item),
  'test_supplier',
  'msk_dc',
  'michelin|225/65r17|102h',
  8500.00,
  10,
  'RUB',
  '[]'::jsonb
;
UPDATE ssot_curated_internal.offers_v1
SET quality_flags = jsonb_build_array('blocked_for_aggregation')
WHERE offer_id='00000000-0000-0000-0000-00000000c401'::uuid;

SELECT 'offers_after_block_count' AS kind, count(*) AS rows
FROM ssot_curated_api.get_offers_by_sku(
  ssot_curated_api.get_current_artifact('production','default'),
  'michelin|225/65r17|102h'
);

ROLLBACK;
