#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel 2>/dev/null || true)"
[ -n "$REPO_ROOT" ] || { echo "Repo root not found"; exit 1; }

cd "$REPO_ROOT" && \
set -a && . ./.env.phone && set +a && \
export PYTHON_BIN="${PYTHON_BIN:-$(command -v python3)}" && \
export TMP_ROOT="${TMP_ROOT:-${ETL_VAR_ROOT:?ETL_VAR_ROOT not set}/tmp}" && \
export BRINEX_XLSX_PATH="${BRINEX_XLSX_PATH:?BRINEX_XLSX_PATH not set}" && \
unset BRINEX_RUN_ROOT && export BRINEX_RUN_ROOT="${ETL_VAR_ROOT:?ETL_VAR_ROOT not set}/tmp/brinex_$(date +%Y%m%d_%H%M%S)" && \
bash "$REPO_ROOT/scripts/ingestion/brinex/run_brinex_xlsx_all_sheets_portable_v1.sh"
