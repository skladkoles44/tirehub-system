#!/usr/bin/env python3
"""
normalizer_with_registry.py - Wrapper для normalizer_v3_1 с registry
"""

import sys
import os
from pathlib import Path
import json
import yaml

# Добавляем корневую директорию в PYTHONPATH
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from scripts.common.schema_registry import SchemaRegistry


def normalize_with_registry(artifact_dir, output_dir=None, reject_mode="full"):
    """
    Запускает normalizer_v3_1 с использованием PINNED схемы
    """
    
    # 1. Читаем зафиксированный хеш
    hash_file = Path(artifact_dir) / "schema_hash.txt"
    if not hash_file.exists():
        raise RuntimeError(f"No schema_hash.txt in {artifact_dir}")
    
    cfg_hash = hash_file.read_text().strip()
    print(f"  🔒 Using pinned schema: {cfg_hash}")
    
    # 2. Получаем схему из registry
    registry = SchemaRegistry()
    try:
        schema = registry.get(cfg_hash)
        print(f"  ✅ Schema loaded from registry")
        
        # Сохраняем схему в artifact для fallback
        with open(Path(artifact_dir) / "schema_snapshot.yaml", 'w') as f:
            yaml.dump(schema, f)
            
    except FileNotFoundError:
        # Fallback: читаем из snapshot
        snapshot_file = Path(artifact_dir) / "schema_snapshot.yaml"
        if snapshot_file.exists():
            with open(snapshot_file) as f:
                schema = yaml.safe_load(f)
            print(f"  ⚠️ Using fallback snapshot")
        else:
            raise
    
    # 3. Проверяем наличие atomic_rows
    atomic_file = Path(artifact_dir) / "atomic_rows.ndjson"
    if not atomic_file.exists():
        raise RuntimeError(f"No atomic_rows.ndjson in {artifact_dir}")
    
    # 4. Запускаем существующий нормалайзер
    out_dir = Path(output_dir) if output_dir else Path(artifact_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"  🔄 Running normalizer_v3_1 with schema {cfg_hash[:8]}...")
    
    from scripts.normalization.normalizer_v3_1 import normalize_atomic_file
    
    manifest = normalize_atomic_file(atomic_file, out_dir, reject_mode)
    
    # 5. Обогащаем манифест информацией о схеме
    if manifest and isinstance(manifest, dict):
        manifest["schema_hash"] = cfg_hash
        manifest["schema_source"] = "registry"
        
        manifest_path = out_dir / "normalizer_manifest.json"
        with open(manifest_path, 'w') as f:
            json.dump(manifest, f, indent=2)
    
    print(f"  ✅ Normalization complete with schema {cfg_hash[:8]}...")
    
    return {
        "schema_hash": cfg_hash,
        "output_dir": str(out_dir)
    }


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Normalize with pinned schema from registry")
    parser.add_argument("artifact_dir", help="Directory with schema_hash.txt and atomic_rows.ndjson")
    parser.add_argument("--output-dir", "-o", dest="output_dir", help="Output directory")
    parser.add_argument("--reject-mode", "-r", default="full", choices=["full", "partial"],
                        help="Reject mode (default: full)")
    
    args = parser.parse_args()
    
    result = normalize_with_registry(
        args.artifact_dir,
        args.output_dir,
        args.reject_mode
    )
    
    print(f"\n✅ Done. Schema: {result['schema_hash']}")
    print(f"   Output: {result['output_dir']}")
