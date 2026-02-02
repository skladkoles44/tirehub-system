BEGIN;

CREATE SCHEMA IF NOT EXISTS ssot_catalog;

CREATE TABLE IF NOT EXISTS ssot_catalog.smoke_runs_v14a (
  run_id uuid PRIMARY KEY,
  created_at timestamptz NOT NULL DEFAULT now(),
  snapshot_id uuid NOT NULL,
  item_id uuid NOT NULL,
  artifact_id uuid NOT NULL,
  db_name text NOT NULL DEFAULT current_database(),
  note text NULL
);

CREATE INDEX IF NOT EXISTS idx_smoke_runs_v14a_created_at
  ON ssot_catalog.smoke_runs_v14a (created_at DESC);

REVOKE ALL ON SCHEMA ssot_catalog FROM PUBLIC;
REVOKE ALL ON TABLE ssot_catalog.smoke_runs_v14a FROM PUBLIC;

-- etl only needs read access (seed writes as postgres)
GRANT USAGE ON SCHEMA ssot_catalog TO etl;
GRANT SELECT ON TABLE ssot_catalog.smoke_runs_v14a TO etl;

COMMIT;
