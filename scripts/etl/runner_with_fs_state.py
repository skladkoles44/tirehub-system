#!/usr/bin/env python3
import inspect
import json
import os
import re
import shutil
import sys
import threading
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from scripts.etl.runner_v5_6_3 import run as runner_core

CHUNK_SIZE = 1024 * 1024
TMP_TTL_SECONDS = 600
HEARTBEAT_INTERVAL_SECONDS = 60
SUPPLIER_ID_RE = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")
REQUIRED_ATOMIC_KEYS = ("row_id", "source_file", "file_hash", "ingestion_id")
ERROR_SUMMARY_LIMIT = 200


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def json_print(obj: dict[str, Any]) -> None:
    print(json.dumps(obj, ensure_ascii=False))


def write_json(path: Path, obj: dict[str, Any]) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def write_text(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def summarize_error(text: str) -> str:
    lines = [x.strip() for x in text.splitlines() if x.strip()]
    if not lines:
        return "unknown_error"
    return lines[-1][:ERROR_SUMMARY_LIMIT]


def validate_supplier_id(supplier_id: str) -> str:
    supplier_id = str(supplier_id or "").strip()
    if not supplier_id:
        raise ValueError("supplier_id_required")
    if not SUPPLIER_ID_RE.fullmatch(supplier_id):
        raise ValueError("supplier_id_invalid_format")
    return supplier_id


def get_file_hash(path: Path, source_hash_env: str | None = None) -> str:
    if source_hash_env:
        return source_hash_env
    import hashlib
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(CHUNK_SIZE), b""):
            h.update(chunk)
    return h.hexdigest()


def build_meta(
    *,
    input_path: Path,
    artifacts_dir: Path,
    file_hash_val: str,
    run_id: str,
    supplier_id: str,
    started_at: str,
    status: str,
    finished_at: str | None = None,
    rows: int = 0,
    error_summary: str | None = None,
    error_full_path: str | None = None,
    final_dir: Path | None = None,
    failed_dir: Path | None = None,
    supplier_id_forwarded: bool | None = None,
) -> dict[str, Any]:
    return {
        "input_path": str(input_path),
        "artifacts_dir": str(artifacts_dir),
        "file_hash": file_hash_val,
        "run_id": run_id,
        "supplier_id": supplier_id,
        "started_at": started_at,
        "finished_at": finished_at,
        "status": status,
        "rows": rows,
        "error_summary": error_summary,
        "error_full_path": error_full_path,
        "final_dir": str(final_dir) if final_dir else None,
        "failed_dir": str(failed_dir) if failed_dir else None,
        "supplier_id_forwarded": supplier_id_forwarded,
    }


def preserve_failed_tmp(tmp_dir: Path, failed_dir: Path) -> None:
    if not tmp_dir.exists():
        return
    if failed_dir.exists():
        shutil.rmtree(failed_dir, ignore_errors=True)
    tmp_dir.rename(failed_dir)


def validate_existing_final_dir(final_dir: Path, expected_hash: str) -> tuple[bool, str]:
    meta_path = final_dir / "run_meta.json"
    atomic_path = final_dir / "atomic_rows.ndjson"

    if not meta_path.exists():
        return False, "existing_artifact_meta_missing"
    if not atomic_path.exists():
        return False, "existing_artifact_atomic_missing"
    if atomic_path.stat().st_size == 0:
        return False, "existing_artifact_atomic_empty"

    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return False, "existing_artifact_meta_unreadable"

    if meta.get("file_hash") != expected_hash:
        return False, "existing_artifact_hash_mismatch"

    return True, "already_processed"


def validate_atomic_rows(atomic_file: Path, expected_file_hash: str) -> tuple[int, str | None]:
    rows = 0
    with atomic_file.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                return rows, f"empty_line_at_{line_no}"
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as e:
                return rows, f"json_decode_error_at_{line_no}: {e}"
            for key in REQUIRED_ATOMIC_KEYS:
                if key not in obj:
                    return rows, f"missing_required_key_{key}_at_{line_no}"
            if obj.get("file_hash") != expected_file_hash:
                return rows, f"file_hash_mismatch_at_{line_no}"
            rows += 1
    if rows == 0:
        return rows, "zero_rows"
    return rows, None


class TmpHeartbeat:
    def __init__(self, path: Path, interval_seconds: int = HEARTBEAT_INTERVAL_SECONDS):
        self.path = path
        self.interval_seconds = interval_seconds
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def _run(self) -> None:
        while not self._stop.wait(self.interval_seconds):
            try:
                self.path.touch(exist_ok=True)
            except Exception:
                pass

    def start(self) -> None:
        self.path.touch(exist_ok=True)
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=1.0)


def call_runner_core(file_path: Path, tmp_dir: Path, file_hash_val: str, supplier_id: str) -> bool:
    sig = inspect.signature(runner_core)
    kwargs: dict[str, Any] = {}
    if "file_hash" in sig.parameters:
        kwargs["file_hash"] = file_hash_val
    forwarded = False
    if "supplier_id" in sig.parameters:
        kwargs["supplier_id"] = supplier_id
        forwarded = True
    runner_core(str(file_path), str(tmp_dir), **kwargs)
    return forwarded


def run_with_fs_state(file_path: str | Path, artifacts_dir: str | Path, supplier_id: str) -> int:
    file_path = Path(str(file_path))
    artifacts_dir = Path(str(artifacts_dir))
    started_at = now_utc()

    try:
        supplier_id = validate_supplier_id(supplier_id)
    except ValueError as e:
        json_print({
            "status": "failed",
            "error": str(e),
        })
        return 1

    if not file_path.exists():
        json_print({
            "status": "failed",
            "error": "input_not_found",
            "input_path": str(file_path),
            "supplier_id": supplier_id,
        })
        return 1

    artifacts_dir.mkdir(parents=True, exist_ok=True)

    source_hash_env = os.environ.get("SOURCE_FILE_HASH")
    file_hash_val = get_file_hash(file_path, source_hash_env)
    run_id = f"run_{file_hash_val[:16]}"
    final_dir = artifacts_dir / run_id
    tmp_dir = artifacts_dir / f"{run_id}.tmp"
    failed_dir = artifacts_dir / f"{run_id}.failed"

    if final_dir.exists():
        ok, reason = validate_existing_final_dir(final_dir, file_hash_val)
        if ok:
            json_print({
                "status": "skipped",
                "hash": file_hash_val,
                "reason": reason,
                "artifact": run_id,
                "supplier_id": supplier_id,
            })
            return 0
        json_print({
            "status": "failed",
            "hash": file_hash_val,
            "error": reason,
            "artifact": run_id,
            "supplier_id": supplier_id,
        })
        return 1

    try:
        tmp_dir.mkdir(parents=True, exist_ok=False)
    except FileExistsError:
        age = time.time() - tmp_dir.stat().st_mtime
        if age > TMP_TTL_SECONDS:
            shutil.rmtree(tmp_dir, ignore_errors=True)
            try:
                tmp_dir.mkdir(parents=True, exist_ok=False)
            except FileExistsError:
                json_print({
                    "status": "skipped",
                    "hash": file_hash_val,
                    "reason": "already_processing",
                    "artifact": run_id,
                    "supplier_id": supplier_id,
                })
                return 0
        else:
            json_print({
                "status": "skipped",
                "hash": file_hash_val,
                "reason": "already_processing",
                "artifact": run_id,
                "supplier_id": supplier_id,
            })
            return 0

    heartbeat = TmpHeartbeat(tmp_dir)
    supplier_id_forwarded: bool | None = None

    try:
        heartbeat.start()

        try:
            supplier_id_forwarded = call_runner_core(file_path, tmp_dir, file_hash_val, supplier_id)
        except KeyboardInterrupt:
            err_full = "KeyboardInterrupt"
            err_path = tmp_dir / "error_full.txt"
            write_text(err_path, err_full)
            meta = build_meta(
                input_path=file_path,
                artifacts_dir=artifacts_dir,
                file_hash_val=file_hash_val,
                run_id=run_id,
                supplier_id=supplier_id,
                started_at=started_at,
                finished_at=now_utc(),
                status="failed",
                rows=0,
                error_summary="KeyboardInterrupt",
                error_full_path=str(err_path),
                failed_dir=failed_dir,
                supplier_id_forwarded=supplier_id_forwarded,
            )
            write_json(tmp_dir / "run_meta.json", meta)
            preserve_failed_tmp(tmp_dir, failed_dir)
            raise
        except Exception:
            err_full = traceback.format_exc()
            err_path = tmp_dir / "error_full.txt"
            write_text(err_path, err_full)
            meta = build_meta(
                input_path=file_path,
                artifacts_dir=artifacts_dir,
                file_hash_val=file_hash_val,
                run_id=run_id,
                supplier_id=supplier_id,
                started_at=started_at,
                finished_at=now_utc(),
                status="failed",
                rows=0,
                error_summary=summarize_error(err_full),
                error_full_path=str(err_path),
                failed_dir=failed_dir,
                supplier_id_forwarded=supplier_id_forwarded,
            )
            write_json(tmp_dir / "run_meta.json", meta)
            preserve_failed_tmp(tmp_dir, failed_dir)
            json_print({
                "status": "failed",
                "hash": file_hash_val,
                "error": meta["error_summary"],
                "artifact": run_id,
                "supplier_id": supplier_id,
                "failed_dir": str(failed_dir),
                "supplier_id_forwarded": supplier_id_forwarded,
            })
            return 1

        atomic_file = tmp_dir / "atomic_rows.ndjson"
        if not atomic_file.exists():
            meta = build_meta(
                input_path=file_path,
                artifacts_dir=artifacts_dir,
                file_hash_val=file_hash_val,
                run_id=run_id,
                supplier_id=supplier_id,
                started_at=started_at,
                finished_at=now_utc(),
                status="failed",
                rows=0,
                error_summary="atomic_rows_missing",
                failed_dir=failed_dir,
                supplier_id_forwarded=supplier_id_forwarded,
            )
            write_json(tmp_dir / "run_meta.json", meta)
            preserve_failed_tmp(tmp_dir, failed_dir)
            json_print({
                "status": "failed",
                "hash": file_hash_val,
                "error": "atomic_rows_missing",
                "artifact": run_id,
                "supplier_id": supplier_id,
                "failed_dir": str(failed_dir),
                "supplier_id_forwarded": supplier_id_forwarded,
            })
            return 1

        rows, validation_error = validate_atomic_rows(atomic_file, file_hash_val)
        if validation_error:
            meta = build_meta(
                input_path=file_path,
                artifacts_dir=artifacts_dir,
                file_hash_val=file_hash_val,
                run_id=run_id,
                supplier_id=supplier_id,
                started_at=started_at,
                finished_at=now_utc(),
                status="failed",
                rows=rows,
                error_summary=validation_error[:ERROR_SUMMARY_LIMIT],
                failed_dir=failed_dir,
                supplier_id_forwarded=supplier_id_forwarded,
            )
            write_json(tmp_dir / "run_meta.json", meta)
            preserve_failed_tmp(tmp_dir, failed_dir)
            json_print({
                "status": "failed",
                "hash": file_hash_val,
                "error": validation_error[:ERROR_SUMMARY_LIMIT],
                "artifact": run_id,
                "rows": rows,
                "supplier_id": supplier_id,
                "failed_dir": str(failed_dir),
                "supplier_id_forwarded": supplier_id_forwarded,
            })
            return 1

        try:
            tmp_dir.rename(final_dir)
        except Exception as e:
            err = f"rename_failed: {e}"
            meta = build_meta(
                input_path=file_path,
                artifacts_dir=artifacts_dir,
                file_hash_val=file_hash_val,
                run_id=run_id,
                supplier_id=supplier_id,
                started_at=started_at,
                finished_at=now_utc(),
                status="failed",
                rows=rows,
                error_summary=err[:ERROR_SUMMARY_LIMIT],
                failed_dir=failed_dir,
                supplier_id_forwarded=supplier_id_forwarded,
            )
            write_json(tmp_dir / "run_meta.json", meta)
            preserve_failed_tmp(tmp_dir, failed_dir)
            json_print({
                "status": "failed",
                "hash": file_hash_val,
                "error": err[:ERROR_SUMMARY_LIMIT],
                "artifact": run_id,
                "supplier_id": supplier_id,
                "failed_dir": str(failed_dir),
                "supplier_id_forwarded": supplier_id_forwarded,
            })
            return 1

        final_meta = build_meta(
            input_path=file_path,
            artifacts_dir=artifacts_dir,
            file_hash_val=file_hash_val,
            run_id=run_id,
            supplier_id=supplier_id,
            started_at=started_at,
            finished_at=now_utc(),
            status="processed",
            rows=rows,
            final_dir=final_dir,
            supplier_id_forwarded=supplier_id_forwarded,
        )
        write_json(final_dir / "run_meta.json", final_meta)

        json_print({
            "status": "processed",
            "hash": file_hash_val,
            "rows": rows,
            "artifact": run_id,
            "supplier_id": supplier_id,
            "final_dir": str(final_dir),
            "supplier_id_forwarded": supplier_id_forwarded,
        })
        return 0

    finally:
        heartbeat.stop()


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("usage: runner_with_fs_state.py <input> <artifacts_dir> <supplier_id>")
        sys.exit(1)

    sys.exit(run_with_fs_state(sys.argv[1], sys.argv[2], sys.argv[3]))
