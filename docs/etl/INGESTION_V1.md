INGESTION v1 (SSOT segmented by run_id)
Inputs: good.ndjson + stats.json + verdict.json (+ mapping.yaml optional)
Gate rule: ingest only if verdict in PASS/WARN
Idempotency: accepted_runs/<run_id>.json exists => exit 0 (already_ingested)
Locking: locks/<run_id>.lock created atomically (O_EXCL); if exists => exit 1
Validation: every NDJSON line must contain required fields and types; run_id/effective_at/mapping_hash/mapping_version must match stats
Commit: write tmp/<run_id>.ndjson.tmp then atomic rename to facts/<supplier>/<parser>/<YYYY-MM>/<run_id>.ndjson
After commit: write manifests/<run_id>.json then accepted_runs/<run_id>.json
On FAIL: no marker created; tmp cleaned; lock released
Exit codes: 0 OK; 1 ARGS/LOCK; 20 FAIL
