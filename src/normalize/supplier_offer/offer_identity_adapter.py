from typing import Any
from src.identity import build_identity_key

def build_offer_identity(candidate: dict[str, Any]) -> dict[str, Any]:
    key, method, strength, raw, variant = build_identity_key({
        "supplier_id": candidate.get("supplier_id"),
        "supplier_sku": candidate.get("supplier_sku") or candidate.get("sku"),
        "brand": candidate.get("brand"),
        "model": candidate.get("model"),
        "width": candidate.get("width"),
        "height": candidate.get("height"),
        "diameter": candidate.get("diameter"),
        "load_index": candidate.get("load_index"),
        "speed_index": candidate.get("speed_index"),
        "name": candidate.get("raw_name") or candidate.get("name"),
        "source_file": candidate.get("source_file"),
        "row_index": candidate.get("row_index"),
        "row_id": candidate.get("row_id"),
    })
    return {
        "identity_key": key,
        "identity_method": method,
        "identity_strength": strength,
        "identity_raw": raw,
        "variant_key": variant,
    }
