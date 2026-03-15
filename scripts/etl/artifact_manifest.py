import json
import hashlib
import time
from pathlib import Path

def file_sha256(path: Path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return f"sha256:{h.hexdigest()}"

def build_manifest(source_file: Path, fingerprint: str, mapping_id: str, stats: dict):
    return {
        "runner": {
            "version": "4.1"
        },
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "input": {
            "file": str(source_file),
            "file_hash": file_sha256(source_file),
            "size_bytes": source_file.stat().st_size
        },
        "layout": {
            "fingerprint": fingerprint,
            "mapping_id": mapping_id
        },
        "stats": stats
    }

def write_manifest(output_dir: Path, manifest: dict):
    output_dir.mkdir(parents=True, exist_ok=True)
    tmp = output_dir / "manifest.json.tmp"
    final = output_dir / "manifest.json"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)
    tmp.rename(final)
