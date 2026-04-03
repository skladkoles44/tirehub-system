#!/usr/bin/env python3
"""
enrich_roles.py — добавляет роли колонкам в atomic_rows.ndjson
"""

import sys
import json
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from scripts.etl.column_classifier import classify_columns


def enrich(inp: Path, out: Path):
    """Добавляет роли колонкам на основе заголовков"""
    
    inp = Path(inp)
    out = Path(out)
    
    # Читаем первую строку безопасно
    with inp.open() as f:
        first_line = f.readline()
        if not first_line:
            print("⚠️ Empty file, skip")
            return out
        
        first = json.loads(first_line)
        headers = [c.get("header") for c in first.get("columns", [])]
        samples = [[c.get("value")] for c in first.get("columns", [])]
        roles = classify_columns(headers, samples) if headers else []
    
    # Обрабатываем все строки
    with inp.open() as f, out.open("w") as w:
        for line in f:
            row = json.loads(line)
            cols = row.get("columns", [])
            
            for i, c in enumerate(cols):
                role = roles[i] if i < len(roles) else "unknown"
                c["role"] = role
            
            w.write(json.dumps(row, ensure_ascii=False) + "\n")
    
    print(f"✅ Enriched: {inp} -> {out}")
    return out


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("usage: enrich_roles.py <input.ndjson> <output.ndjson>")
        sys.exit(1)
    
    enrich(Path(sys.argv[1]), Path(sys.argv[2]))
