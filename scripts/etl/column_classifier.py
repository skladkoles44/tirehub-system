from functools import lru_cache
from pathlib import Path
import yaml

ROLE_MAP_PATH = Path(__file__).resolve().parents[2] / "config" / "semantic_roles.yaml"


def _norm(x):
    s = "" if x is None else str(x)
    s = s.lower().replace("ё", "е").replace("_", " ").replace("-", " ")
    return " ".join(s.split())


@lru_cache(maxsize=1)
def _load_role_map():
    if not ROLE_MAP_PATH.exists():
        return {}

    data = yaml.safe_load(ROLE_MAP_PATH.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        return {}

    role_map = {}

    for role, words in data.items():
        if not isinstance(words, list):
            continue

        normalized = [_norm(w) for w in words if _norm(w)]
        if normalized:
            role_map[role] = normalized

    return role_map


def _classify(h):
    t = _norm(h)
    role_map = _load_role_map()

    for role, words in role_map.items():
        for w in words:
            if w in t:
                return role

    return "unknown"


def column_classifier(flat_headers):
    return [{"role": _classify(h), "header": h} for h in flat_headers]
