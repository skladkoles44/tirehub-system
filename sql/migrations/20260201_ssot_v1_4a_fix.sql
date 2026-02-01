BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- 1) canonical_snapshots: sealed gate
ALTER TABLE ssot_ingestion.canonical_snapshots
  ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'open',
  ADD COLUMN IF NOT EXISTS sealed_at timestamptz;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'canonical_snapshots_status_chk'
      AND conrelid = 'ssot_ingestion.canonical_snapshots'::regclass
  ) THEN
    ALTER TABLE ssot_ingestion.canonical_snapshots
      ADD CONSTRAINT canonical_snapshots_status_chk
      CHECK (status IN ('open','sealed'));
  END IF;
END $$;

-- Baseline-mode: всё существующее считаем sealed (можно поменять позже на более тонкую политику)
UPDATE ssot_ingestion.canonical_snapshots
SET status = 'sealed',
    sealed_at = COALESCE(sealed_at, created_at)
WHERE status IS NULL OR status <> 'sealed';

-- 2) curated_artifacts: lifecycle v1.4a (через отдельные поля, без ломания старого status)
ALTER TABLE ssot_curated_internal.curated_artifacts
  ADD COLUMN IF NOT EXISTS generation_fingerprint bytea,
  ADD COLUMN IF NOT EXISTS generated_at timestamptz,
  ADD COLUMN IF NOT EXISTS status_v14a text;

UPDATE ssot_curated_internal.curated_artifacts
SET status_v14a = COALESCE(status_v14a, 'published'),
    generation_fingerprint = COALESCE(
      generation_fingerprint,
      fingerprint,
      digest((snapshot_id::text || '|' || curated_version)::text, 'sha256')
    ),
    generated_at = COALESCE(generated_at, published_at);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'curated_artifacts_status_chk'
      AND conrelid = 'ssot_curated_internal.curated_artifacts'::regclass
  ) THEN
    ALTER TABLE ssot_curated_internal.curated_artifacts
      ADD CONSTRAINT curated_artifacts_status_chk
      CHECK (status_v14a IN ('generated','published','archived'));
  END IF;
END $$;

-- 3) HARD GATE: дубликаты по (snapshot_id, curated_version) запрещены перед уникальным индексом
DO $$
DECLARE v_dups bigint;
BEGIN
  SELECT COUNT(*) - COUNT(DISTINCT (snapshot_id, curated_version))
  INTO v_dups
  FROM ssot_curated_internal.curated_artifacts;

  IF v_dups > 0 THEN
    RAISE EXCEPTION 'v1.4a_fix blocked: duplicates in curated_artifacts by (snapshot_id, curated_version) = %', v_dups;
  END IF;
END $$;

-- 4) unique(snapshot_id, curated_version)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'ssot_curated_internal'
      AND c.relname = 'uq_curated_snapshot_version'
      AND c.relkind = 'i'
  ) THEN
    EXECUTE 'CREATE UNIQUE INDEX uq_curated_snapshot_version ON ssot_curated_internal.curated_artifacts(snapshot_id, curated_version)';
  END IF;
END $$;

COMMIT;
