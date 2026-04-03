#!/usr/bin/env bash
set -e

export ETL_REPO_ROOT="${ETL_REPO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
export ETL_VAR_ROOT="${ETL_VAR_ROOT:-/storage/emulated/0/Download/ETL}"
export ETL_DATA_ROOT="${ETL_DATA_ROOT:-$ETL_VAR_ROOT/data}"
export ETL_DROP_ROOT="${ETL_DROP_ROOT:-$ETL_VAR_ROOT/drop}"
export PYDEPS_ROOT="${PYDEPS_ROOT:-$ETL_REPO_ROOT/.pydeps/termux-py312}"
export PYTHONPATH="$ETL_REPO_ROOT:$PYDEPS_ROOT"

mkdir -p \
  "$ETL_VAR_ROOT" \
  "$ETL_DATA_ROOT" \
  "$ETL_DROP_ROOT" \
  "$ETL_VAR_ROOT/artifacts" \
  "$ETL_VAR_ROOT/normalized" \
  "$ETL_VAR_ROOT/logs" \
  "$ETL_VAR_ROOT/run"

if [ "${ETL_DEBUG:-0}" = "1" ]; then
  echo "ETL ENV:"
  echo "  REPO  = $ETL_REPO_ROOT"
  echo "  VAR   = $ETL_VAR_ROOT"
  echo "  DATA  = $ETL_DATA_ROOT"
  echo "  DROP  = $ETL_DROP_ROOT"
  echo "  PYDEPS= $PYDEPS_ROOT"
  echo "  PYTHONPATH=$PYTHONPATH"
fi
