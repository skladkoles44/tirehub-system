#!/usr/bin/env python3
"""
compute_roles.py — вычисляет роли по sample из первых N строк
"""

import sys
import json
from collections import defaultdict

from scripts.etl.column_classifier import classify_columns

SAMPLE_SIZE = 20

def main():
    rows = []
    for i, line in enumerate(sys.stdin):
        line = line.strip()
        if not line:
            continue
        row = json.loads(line)
        rows.append(row)
        if i >= SAMPLE_SIZE - 1:
            break
    
    if not rows:
        return
    
    headers = [c.get("header") for c in rows[0]["columns"]]
    samples = defaultdict(list)
    for row in rows:
        for i, col in enumerate(row["columns"]):
            samples[i].append(col.get("value"))
    
    samples_list = [samples[i] for i in range(len(headers))]
    roles = classify_columns(headers, samples_list)
    print(json.dumps(roles))

if __name__ == "__main__":
    main()
