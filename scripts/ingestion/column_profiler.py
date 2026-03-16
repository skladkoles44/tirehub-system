import re
from collections import Counter

SIZE_RE = re.compile(r"\d{3}/\d{2}R\d{2}")
FLOAT_RE = re.compile(r"^[+-]?\d+(?:[\.,]\d+)?$")
INT_RE = re.compile(r"^[+-]?\d+$")

def _cell_from_row(row, col, idx):
    if isinstance(row, dict):
        key = col.get("name") or col.get("header") or col.get("role") or f"col_{idx}"
        return row.get(key)
    if isinstance(row, (list, tuple)):
        return row[idx] if idx < len(row) else None
    return None

def _profile_key(col, idx):
    return col.get("name") or col.get("header") or col.get("role") or f"col_{idx}"

def profile_columns(rows, columns, sample_size=50):
    """
    rows: iterator of row dicts OR tuple/list rows
    columns: column metadata list
    """

    samples = []
    for i, r in enumerate(rows):
        samples.append(r)
        if i + 1 >= sample_size:
            break

    profiles = {}

    for idx, col in enumerate(columns):
        key = _profile_key(col, idx)

        raw_values = [_cell_from_row(r, col, idx) for r in samples]
        values = [str(v).strip() for v in raw_values if v not in (None, "")]

        if not values:
            profiles[key] = {
                "sample_count": 0,
                "patterns": {},
                "top_values": [],
                "column_index": idx,
                "role": col.get("role"),
            }
            continue

        patterns = Counter()

        for v in values:
            if SIZE_RE.match(v):
                patterns["tire_size"] += 1
            elif INT_RE.match(v):
                patterns["int"] += 1
            elif FLOAT_RE.match(v.replace(",", ".")):
                patterns["float"] += 1
            elif "/" in v and len(v) < 32:
                patterns["pattern_slash"] += 1
            else:
                patterns["text"] += 1

        profiles[key] = {
            "sample_count": len(values),
            "patterns": dict(patterns),
            "top_values": Counter(values).most_common(5),
            "column_index": idx,
            "role": col.get("role"),
        }

    return profiles
