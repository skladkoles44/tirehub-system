#!/usr/bin/env python3
import argparse, sys, subprocess
from pathlib import Path

_BOOTSTRAP_ROOT = next((c for c in (Path(__file__).resolve().parent, *Path(__file__).resolve().parent.parents) if (c / "common" / "paths.py").exists()), None)
if _BOOTSTRAP_ROOT and str(_BOOTSTRAP_ROOT) not in sys.path:
    sys.path.insert(0, str(_BOOTSTRAP_ROOT))

from common.paths import repo_path

def main():
  ap = argparse.ArgumentParser(description="Generic NDJSON emitter v1 (v0 wrapper)")
  ap.add_argument("--supplier-id", required=True)
  ap.add_argument("--input", required=True)
  ap.add_argument("--mapping", required=True)
  ap.add_argument("--effective-at", required=True)
  ap.add_argument("--run-id", required=True)
  ap.add_argument("--out-dir", required=True)
  args = ap.parse_args()

  if args.supplier_id != "kolobox":
    print(f"NOT_IMPLEMENTED: supplier_id={args.supplier_id} (v0 wrapper supports only kolobox)", file=sys.stderr)
    return 2

  cmd = [
    sys.executable,
    str(repo_path("scripts", "ingestion", "kolobox", "emit_kolobox_ndjson_v1.py", start=Path(__file__))),
    "--input", args.input,
    "--mapping", args.mapping,
    "--effective-at", args.effective_at,
    "--run-id", args.run_id,
    "--out-dir", args.out_dir,
  ]
  return subprocess.call(cmd)

if __name__ == "__main__":
  raise SystemExit(main())
