#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

import yaml

from scripts.ingestion.runner_v4_1 import RunnerV41


def load_layout_registry(path: Path) -> dict:
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    layouts = data.get("layouts") or {}
    if not isinstance(layouts, dict):
        raise SystemExit(f"LAYOUT_REGISTRY_INVALID: {path}")
    normalized = {}
    for fingerprint, entry in layouts.items():
        if not isinstance(entry, dict):
            raise SystemExit(f"LAYOUT_REGISTRY_ENTRY_INVALID: {fingerprint}")
        mapping = entry.get("mapping")
        if not mapping:
            raise SystemExit(f"LAYOUT_REGISTRY_MAPPING_MISSING: {fingerprint}")
        normalized[str(fingerprint)] = {
            "mapping": str((path.parent.parent / mapping).resolve()) if not Path(mapping).is_absolute() else str(Path(mapping))
        }
    return normalized


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True)
    ap.add_argument("--layout-registry", default=None)
    ap.add_argument("--cache-dir", default="cache/artifacts_runner_v4_1")
    args = ap.parse_args()

    file_path = Path(args.file).resolve()
    cache_dir = Path(args.cache_dir).resolve()

    if not file_path.exists():
        raise SystemExit(f"INPUT_NOT_FOUND: {file_path}")

    layout_registry = {}
    if args.layout_registry:
        reg_path = Path(args.layout_registry).resolve()
        if not reg_path.exists():
            raise SystemExit(f"LAYOUT_REGISTRY_NOT_FOUND: {reg_path}")
        layout_registry = load_layout_registry(reg_path)

    runner = RunnerV41(layout_registry=layout_registry)
    manifest = runner.run(file_path, cache_dir=cache_dir)
    print(json.dumps(manifest, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
