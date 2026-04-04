import hashlib
import re
from copy import deepcopy
from decimal import Decimal, InvalidOperation
from typing import Any
from src.identity import enrich_from_name
from .price_normalization import to_cents, pick_purchase_price
from .stock_normalization import derive_availability
from .offer_identity_adapter import build_offer_identity

def _to_text(v: Any):
    if v is None:
        return None
    s = str(v).strip()
    return s or None

def _require_text(name: str, value: Any):
    v = _to_text(value)
    if not v:
        raise ValueError(f"{name} is required")
    return v

def _to_intish(v: Any):
    if v is None:
        return None
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

def _normalize_season(v: Any):
    t = (_to_text(v) or "").lower()
    if not t:
        return None
    if "зим" in t or t == "winter":
        return "winter"
    if "лет" in t or t == "summer":
        return "summer"
    if "всесез" in t or "all" in t:
        return "all_season"
    return t

def _normalize_load_index(v: Any):
    t = _to_text(v)
    if not t:
        return None
    digits = "".join(ch for ch in t if ch.isdigit())
    return digits or None

def _normalize_speed_index(v: Any):
    t = (_to_text(v) or "").upper()
    if not t:
        return None
    m = re.search(r"[A-Z]+", t)
    return m.group(0) if m else None

def derive_offer_key(supplier_id: str | None, identity_key: str | None, supplier_sku: str | None, raw_name: str | None):
    raw = "|".join([
        str(supplier_id or ""),
        str(identity_key or ""),
        str(supplier_sku or ""),
        str(raw_name or ""),
    ])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]

def build_canonical_supplier_offer(candidate: dict[str, Any]) -> dict[str, Any]:
    enriched = enrich_from_name(deepcopy(candidate))

    supplier_id = _require_text("supplier_id", enriched.get("supplier_id"))
    source_file = _to_text(enriched.get("source_file"))
    source_object_id = _require_text("source_object_id", enriched.get("source_object_id") or source_file)
    run_id = _require_text("run_id", enriched.get("run_id") or enriched.get("ingestion_id"))
    warehouse_key = _require_text("warehouse_key", enriched.get("warehouse_key"))
    warehouse_raw = _to_text(enriched.get("warehouse_raw")) or _to_text(enriched.get("warehouse"))
    supplier_sku = _to_text(enriched.get("supplier_sku")) or _to_text(enriched.get("sku"))
    raw_name = _to_text(enriched.get("raw_name")) or _to_text(enriched.get("name"))

    norm_width = _to_intish(enriched.get("width"))
    norm_height = _to_intish(enriched.get("height"))
    norm_diameter = _to_intish(enriched.get("diameter"))
    norm_season = _normalize_season(enriched.get("season"))
    norm_load_index = _normalize_load_index(enriched.get("load_index"))
    norm_speed_index = _normalize_speed_index(enriched.get("speed_index"))

    core_present = any([
        _to_text(enriched.get("brand")),
        _to_text(enriched.get("model")),
        norm_width is not None,
        norm_height is not None,
        norm_diameter is not None,
    ])
    if not any([supplier_sku, raw_name, core_present]):
        raise ValueError("at least one of supplier_sku, raw_name, or core identity fields is required")

    stock = enriched.get("stock") or {
        "qty": enriched.get("stock_qty_normalized"),
        "raw": enriched.get("stock_qty_raw"),
        "kind": None,
    }
    prices = enriched.get("prices") or {}
    price_value = pick_purchase_price(prices)

    identity = build_offer_identity({
        "supplier_id": supplier_id,
        "supplier_sku": supplier_sku,
        "brand": enriched.get("brand"),
        "model": enriched.get("model"),
        "width": norm_width,
        "height": norm_height,
        "diameter": norm_diameter,
        "load_index": norm_load_index,
        "speed_index": norm_speed_index,
        "raw_name": raw_name,
        "name": raw_name,
        "source_file": source_file,
        "row_index": enriched.get("row_index"),
        "row_id": enriched.get("row_id"),
    })
    offer_key = derive_offer_key(supplier_id, identity["identity_key"], supplier_sku, raw_name)

    return {
        "supplier_id": supplier_id,
        "source_type": _to_text(enriched.get("source_type")) or "file",
        "source_object_id": source_object_id,
        "run_id": run_id,
        "offer_key": offer_key,
        "warehouse_key": warehouse_key,
        "availability_status": derive_availability(stock),

        "supplier_sku": supplier_sku,
        "raw_name": raw_name,
        "item_type": _to_text(enriched.get("item_type")),
        "warehouse_raw": warehouse_raw,
        "stock_qty_raw": stock.get("raw"),
        "stock_qty_normalized": stock.get("qty"),
        "price_purchase_cents": to_cents(price_value),
        "currency": _to_text(enriched.get("currency")) or "RUB",
        "identity_key": identity["identity_key"],
        "quality_flags": enriched.get("quality_flags") or [],
        "is_reject": bool(enriched.get("is_reject", False)),
        "reject_reason": enriched.get("reject_reason"),

        "identity_method": identity["identity_method"],
        "identity_strength": identity["identity_strength"],
        "identity_raw": identity["identity_raw"],
        "variant_key": identity["variant_key"],

        "brand": enriched.get("brand"),
        "model": enriched.get("model"),
        "width": norm_width,
        "height": norm_height,
        "diameter": norm_diameter,
        "season": norm_season,
        "load_index": norm_load_index,
        "speed_index": norm_speed_index,

        "prices": prices,
        "stock": stock,
        "normalizer_version": enriched.get("normalizer_version") or "modernized_v2",
    }
