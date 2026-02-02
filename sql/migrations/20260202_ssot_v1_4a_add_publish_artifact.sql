BEGIN;

CREATE OR REPLACE FUNCTION ssot_curated_api.publish_artifact(
  p_artifact_id uuid,
  p_published_by text DEFAULT 'system'::text
) RETURNS uuid
LANGUAGE plpgsql
SECURITY DEFINER
AS $function$
DECLARE
  v_status_v14a text;
BEGIN
  SELECT status_v14a INTO v_status_v14a
  FROM ssot_curated_internal.curated_artifacts
  WHERE artifact_id = p_artifact_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'publish_artifact: artifact % not found', p_artifact_id;
  END IF;

  IF v_status_v14a IS DISTINCT FROM 'generated' THEN
    RAISE EXCEPTION 'publish_artifact: artifact % status_v14a=% (expected generated)',
      p_artifact_id, v_status_v14a;
  END IF;

  UPDATE ssot_curated_internal.curated_artifacts
  SET status_v14a = 'published',
      published_at = NOW(),
      published_by = p_published_by
  WHERE artifact_id = p_artifact_id;

  RETURN p_artifact_id;
END;
$function$;

COMMENT ON FUNCTION ssot_curated_api.publish_artifact(uuid, text) IS
'v1.4a: publish by artifact_id using status_v14a lifecycle. Legacy publish_curated(snapshot_id,...) stays.';

COMMIT;
