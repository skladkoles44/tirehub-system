#!/usr/bin/env python3
"""
apply_roles.py — применяет роли из файла к потоку строк
"""

import sys
import json

def main():
    if len(sys.argv) < 2:
        print("usage: apply_roles.py <roles.json>", file=sys.stderr)
        sys.exit(1)
    
    with open(sys.argv[1]) as f:
        roles = json.load(f)
    
    for line in sys.stdin:
        if not line.strip():
            continue
        row = json.loads(line)
        for col, role in zip(row["columns"], roles):
            col["role"] = role
        print(json.dumps(row, ensure_ascii=False))

if __name__ == "__main__":
    main()
