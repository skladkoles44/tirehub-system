#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="${FOURTOCHKI_ROOT:-/opt/canonical-core}"
cd "$ROOT"

TS="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_DIR="${FOURTOCHKI_R2_OUT_DIR:-var/probes/4tochki/test_runs}"
REPORT="$OUT_DIR/r2_gate_4tochki_read_baseline_${TS}.txt"
PY="${FOURTOCHKI_PROBE_PY:-.venvs/fourtochki-probe/bin/python}"
EVIDENCE_ROOT="docs/integrations/4tochki/evidence_bundle/extracted_final/4tochki_source_bundle_20260424T100241Z"
MANIFEST="docs/integrations/4tochki/MANIFEST.local.sha256"

mkdir -p "$OUT_DIR"

pytest_rc=0

require_file() {
  local f="$1"
  if test -f "$f"; then
    printf "OK %s " "$f"
    wc -l -c "$f"
  else
    echo "MISSING $f"
    exit 20
  fi
}

require_exec() {
  local f="$1"
  if test -x "$f"; then
    echo "OK_EXEC $f"
  else
    echo "MISSING_EXEC $f"
    exit 21
  fi
}

require_absent() {
  local f="$1"
  if test ! -e "$f"; then
    echo "ABSENT_OK $f"
  else
    echo "UNEXPECTED_PRESENT $f"
    exit 22
  fi
}

require_manifest_entry() {
  local rel="$1"
  if grep -F "  ./$rel" "$MANIFEST" >/dev/null; then
    echo "MANIFEST_OK ./$rel"
  else
    echo "MANIFEST_MISSING ./$rel"
    exit 23
  fi
}

hash_pair() {
  local active="$1"
  local evidence="$2"
  local ah=""
  local eh=""
  ah="$(sha256sum "$active" | awk "{print \$1}")"
  eh="$(sha256sum "$evidence" | awk "{print \$1}")"
  if test "$ah" = "$eh"; then
    echo "PARITY_OK $active $ah"
  else
    echo "PARITY_FAIL $active active=$ah evidence=$eh"
    exit 24
  fi
}

{
  echo "[R2 4TOCHKI READ BASELINE GATE]"
  echo "TS=$TS"
  echo "ROOT=$ROOT"
  echo "REPORT=$REPORT"

  echo "[CRITICAL_SOURCE_FILES]"
  for f in \
    "pytest.ini" \
    "src/integrations/fourtochki/__init__.py" \
    "src/integrations/fourtochki/client.py" \
    "src/integrations/fourtochki/models.py" \
    "src/integrations/fourtochki/errors.py" \
    "scripts/probes/capture_4tochki_probe.py" \
    "tests/probes/probe_inputs.json" \
    "tests/probes/test_4tochki_read_baseline.py"
  do
    require_file "$f"
  done

  echo "[CRITICAL_DOC_FILES]"
  for f in \
    "docs/integrations/4tochki/baseline.md" \
    "docs/integrations/4tochki/IMPORT_STATUS.md" \
    "docs/integrations/4tochki/MANIFEST.local.sha256"
  do
    require_file "$f"
  done
  require_absent "docs/integrations/4tochki/BASELINE_MISSING.md"

  echo "[CRITICAL_EVIDENCE_FILES]"
  for f in \
    "$EVIDENCE_ROOT/MANIFEST.sha256" \
    "$EVIDENCE_ROOT/README_STATUS_4tochki.md" \
    "$EVIDENCE_ROOT/files/pytest.ini" \
    "$EVIDENCE_ROOT/files/scripts/probes/capture_4tochki_probe.py" \
    "$EVIDENCE_ROOT/files/src/integrations/fourtochki/__init__.py" \
    "$EVIDENCE_ROOT/files/src/integrations/fourtochki/client.py" \
    "$EVIDENCE_ROOT/files/src/integrations/fourtochki/models.py" \
    "$EVIDENCE_ROOT/files/src/integrations/fourtochki/errors.py" \
    "$EVIDENCE_ROOT/files/tests/probes/probe_inputs.json" \
    "$EVIDENCE_ROOT/files/tests/probes/test_4tochki_read_baseline.py" \
    "$EVIDENCE_ROOT/meta/env_keys_redacted.txt" \
    "$EVIDENCE_ROOT/meta/runtime_inventory.txt" \
    "$EVIDENCE_ROOT/test_runs/latest_pytest_exact.txt" \
    "$EVIDENCE_ROOT/test_runs/latest_pytest_exact_rc.txt" \
    "docs/integrations/4tochki/evidence_bundle/remote_bundle/4tochki_source_bundle_20260424T100241Z.tar.gz.sha256"
  do
    require_file "$f"
  done

  echo "[CRITICAL_LIVE_ARTIFACTS]"
  for f in \
    "$EVIDENCE_ROOT/files/var/probes/4tochki/20260424T074315Z_ping.json" \
    "$EVIDENCE_ROOT/files/var/probes/4tochki/20260424T074615Z_goods-exact.json" \
    "$EVIDENCE_ROOT/files/var/probes/4tochki/20260424T075222Z_goods-wrh-232.json" \
    "$EVIDENCE_ROOT/files/var/probes/4tochki/20260424T080000Z_goods-occurrence.json" \
    "$EVIDENCE_ROOT/files/var/probes/4tochki/20260424T083102Z_tyre.json" \
    "$EVIDENCE_ROOT/files/var/probes/4tochki/20260424T083846Z_disk-candidate.json"
  do
    require_file "$f"
  done

  echo "[MANIFEST_GATE]"
  require_manifest_entry "baseline.md"
  require_manifest_entry "IMPORT_STATUS.md"
  require_manifest_entry "MANIFEST.local.sha256"
  require_manifest_entry "evidence_bundle/extracted_final/4tochki_source_bundle_20260424T100241Z/MANIFEST.sha256"
  require_manifest_entry "evidence_bundle/extracted_final/4tochki_source_bundle_20260424T100241Z/README_STATUS_4tochki.md"
  require_manifest_entry "evidence_bundle/extracted_final/4tochki_source_bundle_20260424T100241Z/files/src/integrations/fourtochki/client.py"
  require_manifest_entry "evidence_bundle/extracted_final/4tochki_source_bundle_20260424T100241Z/files/tests/probes/test_4tochki_read_baseline.py"
  grep -n "./baseline.md" "$MANIFEST"

  echo "[ACTIVE_VS_EVIDENCE_PARITY]"
  hash_pair "pytest.ini" "$EVIDENCE_ROOT/files/pytest.ini"
  hash_pair "src/integrations/fourtochki/__init__.py" "$EVIDENCE_ROOT/files/src/integrations/fourtochki/__init__.py"
  hash_pair "src/integrations/fourtochki/client.py" "$EVIDENCE_ROOT/files/src/integrations/fourtochki/client.py"
  hash_pair "src/integrations/fourtochki/models.py" "$EVIDENCE_ROOT/files/src/integrations/fourtochki/models.py"
  hash_pair "src/integrations/fourtochki/errors.py" "$EVIDENCE_ROOT/files/src/integrations/fourtochki/errors.py"
  hash_pair "scripts/probes/capture_4tochki_probe.py" "$EVIDENCE_ROOT/files/scripts/probes/capture_4tochki_probe.py"
  hash_pair "tests/probes/probe_inputs.json" "$EVIDENCE_ROOT/files/tests/probes/probe_inputs.json"
  hash_pair "tests/probes/test_4tochki_read_baseline.py" "$EVIDENCE_ROOT/files/tests/probes/test_4tochki_read_baseline.py"

  echo "[BASELINE_HASH]"
  sha256sum "docs/integrations/4tochki/baseline.md"
  grep -F "1a987ecf763654a36b661fc80ea4ca4d061319c782f56d500537c5d2c3e12cf8  ./baseline.md" "$MANIFEST" >/dev/null \
    && echo "BASELINE_HASH_MANIFEST_OK" \
    || { echo "BASELINE_HASH_MANIFEST_FAIL"; exit 25; }

  echo "[PROBE_VENV_GATE]"
  require_exec "$PY"
  "$PY" -V
  "$PY" -m pytest --version

  echo "[LOAD_ENV_KEYS_REDACTED]"
  if test -f ".env"; then
    set -a
    # shellcheck disable=SC1091
    source ".env"
    set +a
    echo ".env=LOADED"
  else
    echo ".env=MISSING"
  fi
  env | awk -F= "/(4TOCHKI|FOURTOCHKI|SOAP|WSDL|API)/ {print \$1}" | LC_ALL=C sort

  echo "[HASHES]"
  sha256sum \
    "pytest.ini" \
    "src/integrations/fourtochki/__init__.py" \
    "src/integrations/fourtochki/client.py" \
    "src/integrations/fourtochki/models.py" \
    "src/integrations/fourtochki/errors.py" \
    "scripts/probes/capture_4tochki_probe.py" \
    "tests/probes/probe_inputs.json" \
    "tests/probes/test_4tochki_read_baseline.py" \
    "docs/integrations/4tochki/baseline.md" \
    "$MANIFEST"

  echo "[PY_COMPILE]"
  "$PY" -m py_compile \
    "src/integrations/fourtochki/__init__.py" \
    "src/integrations/fourtochki/client.py" \
    "src/integrations/fourtochki/models.py" \
    "src/integrations/fourtochki/errors.py" \
    "scripts/probes/capture_4tochki_probe.py" \
    "tests/probes/test_4tochki_read_baseline.py"
  echo "PY_COMPILE_OK"

  echo "[PYTEST_EXACT]"
  set +e
  "$PY" -m pytest -q -m "probe and exact" "tests/probes/test_4tochki_read_baseline.py"
  pytest_rc="$?"
  set -e
  echo "PYTEST_EXACT_RC=$pytest_rc"
  if test "$pytest_rc" -eq 0; then
    echo "PYTEST_EXACT_OK=1"
  else
    echo "PYTEST_EXACT_OK=0"
  fi

  echo "[GIT_STATUS_SCOPE]"
  git status --short \
    "docs/integrations/4tochki" \
    "src/integrations/fourtochki" \
    "scripts/probes/capture_4tochki_probe.py" \
    "tests/probes/probe_inputs.json" \
    "tests/probes/test_4tochki_read_baseline.py" \
    "scripts/ops/4tochki_read_baseline.sh" \
    "pytest.ini" || true

  echo "[DONE]"
} > "$REPORT" 2>&1

cat "$REPORT"
exit "$pytest_rc"
