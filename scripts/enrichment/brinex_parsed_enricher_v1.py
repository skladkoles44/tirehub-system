#!/usr/bin/env python3
import json
import re
import sys
from typing import Any, Dict, Optional

def normalize_width_token(value: Optional[str]) -> Optional[str]:
    if value in (None, "", "null"):
        return None
    s = str(value).strip().replace(",", ".")
    try:
        n = float(s)
        return str(int(n)) if n.is_integer() else str(n).rstrip("0").rstrip(".")
    except Exception:
        return s


ASPECT_RATIO_WHITELIST = {"40","45","50","55","60","65","70","75","80","85","90"}
HEIGHT_FROM_NAME_RE = re.compile(r'(?<!\d)(\d+(?:\.\d+)?)\s*/\s*(\d{2})(?!\d)')

def extract_height_from_name(name: Optional[str], expected_width: Optional[str]) -> Optional[str]:
    if not name:
        return None
    expected_width_norm = normalize_width_token(expected_width)
    if expected_width_norm in (None, ""):
        return None
    m = HEIGHT_FROM_NAME_RE.search(str(name))
    if not m:
        return None
    width_token = normalize_width_token(m.group(1))
    if width_token != expected_width_norm:
        return None
    height = m.group(2)
    if height not in ASPECT_RATIO_WHITELIST:
        return None
    return height

def enrich_record(obj: Dict[str, Any]) -> Dict[str, Any]:
    parsed = obj.get("parsed")
    if not isinstance(parsed, dict):
        return obj
    current_height = parsed.get("height")
    if current_height not in (None, "", "null"):
        return obj
    width = parsed.get("width")
    diameter = parsed.get("diameter")
    if width in (None, "", "null"):
        return obj
    if diameter in (None, "", "null"):
        return obj
    derived = extract_height_from_name(parsed.get("name"), width)
    if derived is None:
        return obj
    new_obj = dict(obj)
    new_parsed = dict(parsed)
    new_parsed["height"] = derived
    new_obj["parsed"] = new_parsed
    qf = new_obj.get("quality_flags")
    if not isinstance(qf, list):
        qf = []
    if "height_derived_from_name" not in qf:
        qf = list(qf) + ["height_derived_from_name"]
    new_obj["quality_flags"] = qf
    return new_obj

def main() -> int:
    for raw in sys.stdin:
        line = raw.rstrip("\n")
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except Exception:
            sys.stdout.write(line + "\n")
            continue
        try:
            out = enrich_record(obj)
        except Exception:
            out = obj
        sys.stdout.write(json.dumps(out, ensure_ascii=False) + "\n")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
