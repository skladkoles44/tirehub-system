import re
from typing import Any
from .price_normalization import to_number

TEXT_STOCK_PATTERNS = [
    (re.compile(r"^\s*больше\s+(\d+)\s*$", re.I), "gt"),
    (re.compile(r"^\s*>=\s*(\d+)\s*$", re.I), "gte"),
    (re.compile(r"^\s*(\d+)\+\s*$", re.I), "gte"),
]

def normalize_stock(v: Any):
    if v is None:
        return {"qty": None, "raw": None, "kind": None}

    n = to_number(v)
    if n is not None:
        return {"qty": int(n), "raw": str(v), "kind": "exact"}

    s = str(v).strip()
    sl = s.lower()

    if sl in ("в пути", "in transit"):
        return {"qty": None, "raw": s, "kind": "in_transit"}

    for rx, kind in TEXT_STOCK_PATTERNS:
        m = rx.match(s)
        if m:
            return {"qty": int(m.group(1)), "raw": s, "kind": kind}

    return {"qty": None, "raw": s, "kind": "text"}

def derive_availability(stock: dict):
    stock = stock or {}
    qty = stock.get("qty")
    kind = stock.get("kind")
    if kind in ("gt", "gte"):
        return "limited"
    if kind == "in_transit":
        return "backorder"
    if qty is not None:
        if qty > 0:
            return "in_stock"
        if qty == 0:
            return "out_of_stock"
    return "unknown"
