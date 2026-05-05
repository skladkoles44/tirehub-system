#!/usr/bin/env bash
set -euo pipefail

echo "===== BEGIN 4TOCHKI READ BASELINE ====="

# STEP 0: Verify artifact manifest (enforced gate)
echo "[STEP 0] MANIFEST VERIFY"
python3 scripts/ops/verify_artifact_manifest.py
echo "[MANIFEST_VERIFY] GATE=PASS"

# STEP 1: Run exact tests
echo "[STEP 1] RUN TESTS"
 python3 -m pytest tests/probes/test_4tochki_read_baseline.py -m exact -q --tb=no

# STEP 2: Report
echo "===== END 4TOCHKI READ BASELINE ====="
