import os
#!/usr/bin/env python3
"""
runner_with_fs_state.py — state = существование артефакта (по хешу)
Полная версия со всеми фиксами:
- TTL для tmp_dir
- Atomic lock через mkdir
- Передача hash в runner
- Правильная обработка путей
"""

import sys
import json
import shutil
import time
import hashlib
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from scripts.etl.runner_v5_6_3 import run as runner_core


def get_file_hash(path, source_hash_env=None):
    """Получает хеш файла (из env или вычисляет)"""
    if source_hash_env:
        return source_hash_env
    import hashlib
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024*1024), b""):
            h.update(chunk)
    return h.hexdigest()
    """Вычисляет SHA256 файла"""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def run_with_fs_state(file_path, artifacts_dir):
    file_path = Path(file_path)
    artifacts_dir = Path(artifacts_dir)
    # ЯВНОЕ ПРИВЕДЕНИЕ К PATH
    file_path = Path(str(file_path))
    artifacts_dir = Path(str(artifacts_dir))
    
    # Создаём директорию для артефактов
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    
    source_hash_env = os.environ.get("SOURCE_FILE_HASH")
    file_hash_val = get_file_hash(file_path, source_hash_env)
    run_id = f"run_{file_hash_val[:16]}"
    final_dir = artifacts_dir / run_id
    tmp_dir = artifacts_dir / (run_id + ".tmp")
    
    print(f"DEBUG: final_dir type = {type(final_dir)}", file=sys.stderr)
    print(f"DEBUG: tmp_dir type = {type(tmp_dir)}", file=sys.stderr)
    
    if final_dir.exists():
        print(json.dumps({
            "status": "skipped",
            "hash": file_hash_val,
            "reason": "already_processed"
        }))
        return 0
    
    # SAFE ATOMIC LOCK с TTL
    try:
        tmp_dir.mkdir(parents=True, exist_ok=False)
    except FileExistsError:
        age = time.time() - tmp_dir.stat().st_mtime
        if age > 600:
            shutil.rmtree(tmp_dir, ignore_errors=True)
            try:
                tmp_dir.mkdir(parents=True, exist_ok=False)
            except FileExistsError:
                print(json.dumps({
                    "status": "skipped",
                    "hash": file_hash_val,
                    "reason": "already_processing"
                }))
                return 0
        else:
            print(json.dumps({
                "status": "skipped",
                "hash": file_hash_val,
                "reason": "already_processing"
            }))
            return 0
    
    try:
        runner_core(str(file_path), str(tmp_dir), file_hash=file_hash_val)
        success = True
        stderr = ""
    except Exception as e:
        import traceback
        success = False
        stderr = traceback.format_exc()
        print(stderr, file=sys.stderr)
    
    atomic_file = tmp_dir / "atomic_rows.ndjson"
    rows = 0
    if atomic_file.exists():
        with open(atomic_file) as f:
            rows = sum(1 for _ in f)
    
    if not success:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        print(json.dumps({
            "status": "failed",
            "hash": file_hash_val,
            "error": stderr[:200],
            "artifact": run_id
        }))
        return 1
    
    if rows == 0:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        print(json.dumps({
            "status": "failed",
            "hash": file_hash_val,
            "error": "zero_rows",
            "artifact": run_id
        }))
        return 1
    
    try:
        tmp_dir.rename(final_dir)
    except Exception as e:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        print(json.dumps({
            "status": "failed",
            "hash": file_hash_val,
            "error": f"rename_failed: {e}",
            "artifact": run_id
        }))
        return 1
    
    print(json.dumps({
        "status": "processed",
        "hash": file_hash_val,
        "rows": rows,
        "artifact": run_id
    }))
    return 0

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("usage: runner_with_fs_state.py <input> <artifacts_dir>")
        sys.exit(1)

    sys.exit(run_with_fs_state(sys.argv[1], sys.argv[2]))
