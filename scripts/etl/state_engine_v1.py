#!/usr/bin/env python3
import json

import os
from pathlib import Path

ETL_VAR_ROOT = os.environ.get("ETL_VAR_ROOT")
if not ETL_VAR_ROOT:
    raise RuntimeError("ETL_VAR_ROOT not set")

STATE_PATH = Path(ETL_VAR_ROOT) / "state.json"

import hashlib
import sys
import os
from pathlib import Path
from datetime import datetime, timezone
from functools import lru_cache



def now():
    return datetime.now(timezone.utc).isoformat()

@lru_cache(maxsize=10000)
def _get_file_key(path: Path) -> str:
    """Кэшируемый ключ: size + mtime (быстро, без sha256)"""
    stat = path.stat()
    return f"{stat.st_size}:{stat.st_mtime}"

def sha256_file(path: Path, use_cache=True) -> str:
    """sha256 с кэшированием по size+mtime"""
    if use_cache:
        key = _get_file_key(path)
        # можно хранить в отдельном кэше, но для простоты считаем
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024*1024), b""):
            h.update(chunk)
    return h.hexdigest()

def load_state():
    if not STATE_PATH.exists():
        return {"version": 1, "updated_at": now(), "files": {}}
    try:
        with open(STATE_PATH, 'r') as f:
            return json.load(f)
    except Exception:
        return {"version": 1, "updated_at": now(), "files": {}}

def save_state(state):
    state["updated_at"] = now()
    tmp = STATE_PATH.with_suffix(".tmp")
    
    # Атомарная запись с fsync
    with open(tmp, 'w') as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    
    os.replace(tmp, STATE_PATH)
    
    # fsync директории
    try:
        fd = os.open(str(STATE_PATH.parent), os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except Exception:
        pass

def should_process(path: Path, state):
    try:
        h = sha256_file(path)
    except Exception:
        return False, None, "read_error"
    
    meta = state["files"].get(h)
    if not meta:
        return True, h, None
    if meta.get("status") == "failed":
        return True, h, None
    return False, h, None

def mark(state, h, status, rows=0, error=None, artifact=None):
    entry = state["files"].get(h, {})
    entry.update({
        "status": status,
        "processed_at": now(),
        "rows": rows,
        "error": error,
        "artifact": artifact
    })
    if "first_seen" not in entry:
        entry["first_seen"] = now()
    state["files"][h] = entry

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", help="Check file")
    parser.add_argument("--mark", choices=["processed", "failed"], help="Mark status")
    parser.add_argument("--hash", help="File hash for marking")
    parser.add_argument("--rows", type=int, default=0)
    parser.add_argument("--artifact", default="")
    parser.add_argument("--error", default="")
    args = parser.parse_args()

    state = load_state()

    if args.file:
        f = Path(args.file)
        need, h, err = should_process(f, state)
        print(json.dumps({
            "file": str(f),
            "hash": h,
            "process": need,
            "error": err
        }))
    
    elif args.mark and args.hash:
        mark(state, args.hash, args.mark, rows=args.rows, 
             error=args.error, artifact=args.artifact)
        save_state(state)
        print(json.dumps({"status": "ok", "hash": args.hash}))

if __name__ == "__main__":
    main()
