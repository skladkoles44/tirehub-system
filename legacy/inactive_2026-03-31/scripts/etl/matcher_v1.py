#!/usr/bin/env python3
"""
matcher_v1.py — группировка товаров по identity_key
"""

import sys
import json
from collections import defaultdict


def main():
    groups = defaultdict(list)

    # читаем good.ndjson
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        row = json.loads(line)
        key = row.get("identity_key", "unknown")

        groups[key].append(row)

    # вывод агрегатов
    for key, items in groups.items():
        prices = []
        stocks = []

        for r in items:
            p = r.get("price")
            if isinstance(p, (int, float)):
                prices.append(p)

            s = r.get("stock")
            if isinstance(s, (int, float)):
                stocks.append(s)

        out = {
            "identity_key": key,
            "offers_count": len(items),
            "best_price": min(prices) if prices else None,
            "stock_total": sum(stocks) if stocks else None,
            "offers": items
        }

        print(json.dumps(out, ensure_ascii=False))


if __name__ == "__main__":
    main()
