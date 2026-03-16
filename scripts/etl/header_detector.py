from functools import lru_cache

UNHASHABLE_CONTAINER_TYPES = (list, dict, set)
TEXT_TYPES = (str,)
NUMERIC_TYPES = (int, float)

def _cell_kind_uncached(v):
    if v is None:
        return "empty"
    if isinstance(v, TEXT_TYPES):
        s = v.strip()
        if not s:
            return "empty"
        return "text"
    if isinstance(v, NUMERIC_TYPES) and not isinstance(v, bool):
        return "num"
    return "other"


@lru_cache(maxsize=1024)
def _cell_kind_cached(v):
    return _cell_kind_uncached(v)


def _cell_kind(v):
    if isinstance(v, UNHASHABLE_CONTAINER_TYPES):
        return _cell_kind_uncached(v)
    try:
        hash(v)
    except TypeError:
        return _cell_kind_uncached(v)
    return _cell_kind_cached(v)


def _row_stats(row):
    nonempty = 0
    text = 0
    num = 0
    text_values = set()

    for v in row:
        kind = _cell_kind(v)
        if kind != "empty":
            nonempty += 1
        if kind == "text":
            text += 1
            text_values.add(str(v).strip())
        elif kind == "num":
            num += 1

    return {
        "nonempty": nonempty,
        "text": text,
        "num": num,
        "uniq_text": len(text_values),
    }


def _looks_like_index_row(row):
    vals = [v for v in row if _cell_kind(v) != "empty"]
    if len(vals) < 2:
        return False

    ints = []
    for v in vals:
        if isinstance(v, int):
            ints.append(v)
        elif isinstance(v, float) and v.is_integer():
            ints.append(int(v))
        else:
            return False

    return ints == list(range(1, len(ints) + 1))


def _looks_like_header(row):
    s = _row_stats(row)
    return s["nonempty"] >= 2 and s["text"] >= max(1, s["num"]) and s["uniq_text"] >= 2


def detect_header(table, scan_limit=20):
    rows = []
    for i, row in enumerate(table.iter_rows(values_only=True), 1):
        rows.append(row)
        if i >= scan_limit:
            break

    for i, row in enumerate(rows):
        if not _looks_like_header(row):
            continue

        nxt = rows[i + 1] if i + 1 < len(rows) else None
        skip_index_row = bool(nxt is not None and _looks_like_index_row(nxt))

        if skip_index_row:
            return {"header_idx": i, "header": row, "skip_index_row": True}

        if nxt is not None:
            ns = _row_stats(nxt)
            if ns["nonempty"] >= 2 and ns["num"] >= ns["text"]:
                return {"header_idx": i, "header": row, "skip_index_row": False}

        return {"header_idx": i, "header": row, "skip_index_row": False}

    for i, row in enumerate(rows):
        if any(v not in (None, "") for v in row):
            return {"header_idx": i, "header": row, "skip_index_row": False}

    return {"header_idx": 0, "header": [], "skip_index_row": False}


def header_detector(table, scan_limit=20):
    return detect_header(table, scan_limit=scan_limit)["header"]
