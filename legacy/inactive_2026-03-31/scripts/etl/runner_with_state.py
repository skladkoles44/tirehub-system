#!/usr/bin/env python3
"""
runner_with_state.py — production version

- direct call to runner core (no subprocess)
- state-driven execution
- zero_rows detection
- file-type firewall (magic + csv text)
"""

import sys

def run_with_timeout(cmd, timeout=120):
    """Запускает команду с таймаутом"""
    import subprocess
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        print(f"💀 TIMEOUT after {timeout}s")
        return subprocess.CompletedProcess(cmd, 1, "", f"timeout after {timeout}s")



MAX_RETRIES = 3

def run_with_retry(cmd):
    import subprocess, time

    for attempt in range(1, MAX_RETRIES + 1):
        result = run_with_retry(cmd, capture_output=True, text=True)

        if result.returncode == 0:
            return result

        print(f"⚠️ retry {attempt}/{MAX_RETRIES} failed")

        if attempt < MAX_RETRIES:
            time.sleep(2 * attempt)

    return result
import json
from pathlib import Path

# repo import root
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from scripts.etl.state_engine_v1 import load_state, save_state, should_process, mark
from scripts.etl.runner_v5_6_3 import run as runner_core


# ==================== FILE TYPE DETECTION ====================

def looks_like_text(path: Path, sample: int = 512) -> bool:
    try:
        with open(path, "rb") as f:
            chunk = f.read(sample)
        return b'\x00' not in chunk
    except Exception:
        return False


def detect_file_type(path: Path):
    try:
        with open(path, "rb") as f:
            header = f.read(8)
    except Exception:
        return ("unknown", None)

    # XLS
    if header.startswith(b'\xd0\xcf\x11\xe0'):
        return ("xls", None)

    # ZIP (xlsx / ods)
    if header.startswith(b'PK'):
        try:
            import zipfile
            with zipfile.ZipFile(path, 'r') as z:
                names = z.namelist()
                if any(n.startswith("xl/") for n in names):
                    return ("xlsx", "xlsx")
                if "content.xml" in names:
                    return ("ods", "ods")
            return ("unknown_zip", None)
        except Exception:
            return ("corrupt_zip", None)

    return ("unknown", None)


# ==================== MAIN ====================

def run_with_state(file_path: Path, artifacts_dir: Path):
    state = load_state()

    need, file_hash, err = should_process(file_path, state)

    if not need:
        print(json.dumps({
            "status": "skipped",
            "hash": file_hash,
            "reason": err or "already_processed"
        }))
        return 0

    if err:
        print(json.dumps({
            "status": "error",
            "hash": file_hash,
            "error": err
        }))
        return 1

    name = file_path.name.lower()

    # ==================== NAME PREFILTER ====================

    valid_exts = ['.xls', '.xlsx', '.xlsm', '.ods', '.csv']

    if not any(
        name.endswith(ext) or (ext + "_") in name or ("_" + ext) in name
        for ext in valid_exts
    ):
        mark(state, file_hash, "failed", error=f"bad_extension_by_name: {file_path.name}")
        save_state(state)
        print(json.dumps({
            "status": "failed",
            "hash": file_hash,
            "error": "bad_extension_by_name"
        }))
        return 1

    # ==================== CONTENT CHECK ====================

    ftype, _ = detect_file_type(file_path)

    if ftype == "unknown":
        if name.endswith(".csv") or ".csv_" in name:
            if not looks_like_text(file_path):
                mark(state, file_hash, "failed", error="bad_csv_binary")
                save_state(state)
                print(json.dumps({
                    "status": "failed",
                    "hash": file_hash,
                    "error": "bad_csv_binary"
                }))
                return 1
            ftype = "csv"
        else:
            mark(state, file_hash, "failed", error="bad_format_unknown")
            save_state(state)
            print(json.dumps({
                "status": "failed",
                "hash": file_hash,
                "error": "bad_format_unknown"
            }))
            return 1

    if ftype == "unknown_zip":
        mark(state, file_hash, "failed", error="bad_format_unknown_zip")
        save_state(state)
        print(json.dumps({
            "status": "failed",
            "hash": file_hash,
            "error": "bad_format_unknown_zip"
        }))
        return 1

    if ftype not in ["xls", "xlsx", "ods", "csv"]:
        mark(state, file_hash, "failed", error=f"bad_format:{ftype}")
        save_state(state)
        print(json.dumps({
            "status": "failed",
            "hash": file_hash,
            "error": f"bad_format:{ftype}"
        }))
        return 1

    # ==================== RUN ====================

    run_id = f"run_{int(__import__('time').time())}_{file_hash[:8]}"
    run_dir = artifacts_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    try:
        runner_core(file_path, run_dir)
        success = True
        stderr = ""
    except Exception as e:
        success = False
        stderr = str(e)

    # ==================== RESULT ====================

    atomic_file = run_dir / "atomic_rows.ndjson"
    rows = 0

    if atomic_file.exists():
        with open(atomic_file) as f:
            rows = sum(1 for _ in f)

    if not success:
        mark(state, file_hash, "failed", error=f"runner_fail: {stderr[:200]}", artifact=run_id)
        save_state(state)
        print(json.dumps({
            "status": "failed",
            "hash": file_hash,
            "error": stderr[:200]
        }))
        return 1

    if rows == 0:
        mark(state, file_hash, "failed", error="zero_rows", artifact=run_id)
        save_state(state)
        print(json.dumps({
            "status": "failed",
            "hash": file_hash,
            "error": "zero_rows",
            "artifact": run_id
        }))
        return 1

    mark(state, file_hash, "processed", rows=rows, artifact=run_id)
    save_state(state)

    print(json.dumps({
        "status": "processed",
        "hash": file_hash,
        "rows": rows,
        "artifact": run_id
    }))

    return 0


# ==================== CLI ====================

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("usage: runner_with_state.py <file> <artifacts_dir>")
        sys.exit(1)

    sys.exit(run_with_state(Path(sys.argv[1]), Path(sys.argv[2])))
