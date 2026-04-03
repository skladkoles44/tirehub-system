#!/usr/bin/env bash
set -e

REPO=${ETL_REPO_ROOT:-$(pwd)}
PYDEPS="$REPO/.pydeps/termux-py312"

if [ -n "${VIRTUAL_ENV:-}" ]; then
  echo "ERROR: active venv detected: $VIRTUAL_ENV"
  echo "Deactivate first: deactivate"
  exit 1
fi

mkdir -p "$PYDEPS"

echo "== PYTHON =="
python -V

echo "== INSTALL TARGET =="
echo "$PYDEPS"

echo "== INSTALL REQUIREMENTS =="
python -m pip install --no-cache-dir --upgrade --target "$PYDEPS" -r "$REPO/requirements/base.txt"

echo "== VERIFY IMPORTS =="
PYTHONPATH="$REPO:$PYDEPS" python - <<'PY'
import yaml, openpyxl, xlrd, requests, sqlalchemy
print("IMPORTS_OK")
print("yaml", yaml.__version__)
print("openpyxl", openpyxl.__version__)
print("xlrd", xlrd.__version__)
print("requests", requests.__version__)
print("sqlalchemy", sqlalchemy.__version__)
PY
