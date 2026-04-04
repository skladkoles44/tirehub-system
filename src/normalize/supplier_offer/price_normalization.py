import math
from decimal import Decimal, InvalidOperation
from typing import Any

def to_number(v: Any):
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        if not math.isfinite(v):
            return None
        return int(v) if v.is_integer() else v
    s = str(v).strip().replace("\xa0", "").replace(" ", "").replace(",", ".")
    if not s:
        return None
    try:
        x = Decimal(s)
    except (InvalidOperation, ValueError):
        return None
    if x == x.to_integral():
        return int(x)
    return float(x)

def to_cents(v: Any):
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v * 100
    s = str(v).strip().replace("\xa0", "").replace(" ", "").replace(",", ".")
    if not s:
        return None
    try:
        x = Decimal(s)
    except (InvalidOperation, ValueError):
        return None
    return int((x * Decimal("100")).quantize(Decimal("1")))

def pick_purchase_price(prices: dict):
    if not isinstance(prices, dict):
        return None
    for key in ("wholesale", "supplier_price", "cost", "price", "retail"):
        if key in prices:
            n = to_number(prices[key])
            if n is not None:
                return n
    return None
