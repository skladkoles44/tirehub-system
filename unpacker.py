#!/usr/bin/env python3
from pathlib import Path
import runpy
import sys

ROOT = Path(__file__).resolve().parent
TARGET = ROOT / 'scripts/connectors/mail_unpacker_v1.py'

if not TARGET.exists():
    raise SystemExit(f"ENTRYPOINT_TARGET_NOT_FOUND: {TARGET}")

sys.argv[0] = str(TARGET)
runpy.run_path(str(TARGET), run_name="__main__")
