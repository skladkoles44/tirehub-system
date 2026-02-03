#!/usr/bin/env python3
import json
from pathlib import Path

def normalize(s: str) -> str:
    s = (s or "").strip().lower()
    s = " ".join(s.split())
    return s

def load_yaml_minimal(path: Path) -> dict:
    # Minimal YAML reader for this contract shape (keys, lists, scalars, dict nesting).
    # Assumes no fancy YAML features (anchors, multiline, etc.).
    # If PyYAML exists, prefer it.
    try:
        import yaml  # type: ignore
        return yaml.safe_load(path.read_text(encoding="utf-8"))
    except Exception:
        pass

    data = {}
    stack = [(0, data)]
    for raw in path.read_text(encoding="utf-8").splitlines():
        if not raw.strip() or raw.lstrip().startswith("#"):
            continue
        indent = len(raw) - len(raw.lstrip(" "))
        line = raw.strip()

        while stack and indent < stack[-1][0]:
            stack.pop()

        cur = stack[-1][1]
        if line.startswith("- "):
            # list item
            item = line[2:].strip()
            if not isinstance(cur, list):
                raise SystemExit("Minimal YAML parser hit list item where current node is not a list. Install pyyaml.")
            cur.append(item.strip('"').strip("'"))
            continue

        if ":" in line:
            k, v = line.split(":", 1)
            k = k.strip()
            v = v.strip()
            if v == "":
                # decide next container by peeking (default dict)
                nxt = {}
                cur[k] = nxt
                stack.append((indent + 2, nxt))
            else:
                # scalar / inline list
                if v.startswith("[") and v.endswith("]"):
                    inner = v[1:-1].strip()
                    if not inner:
                        cur[k] = []
                    else:
                        parts = [p.strip().strip('"').strip("'") for p in inner.split(",")]
                        cur[k] = parts
                else:
                    cur[k] = v.strip('"').strip("'")
        else:
            raise SystemExit("Minimal YAML parser cannot parse line. Install pyyaml.")
    return data

def main():
    ev_dir = Path("docs/ingestion/kolobox/evidence")
    contract_path = Path("docs/contracts/KOLOBOX_XLS_MAPPING_V1.yaml")
    if not ev_dir.exists():
        raise SystemExit(f"NOT_FOUND: {ev_dir}")
    if not contract_path.exists():
        raise SystemExit(f"NOT_FOUND: {contract_path}")

    contract = load_yaml_minimal(contract_path)
    rules = contract.get("dispatch", {}).get("rules", [])
    if not rules:
        raise SystemExit("Contract has no dispatch.rules")

    ok = True
    for ev_path in sorted(ev_dir.glob("*.evidence.json")):
        ev = json.loads(ev_path.read_text(encoding="utf-8"))
        filename = Path(ev.get("file", ev_path.name)).name
        cols = ev.get("shape", {}).get("cols")
        header = ev.get("header", {})
        row1 = [normalize(x) for x in header.get("row1_cells", [])]
        row2 = [normalize(x) for x in header.get("row2_cells", [])]
        row1_join = " | ".join([x for x in row1 if x])
        row2_join = " | ".join([x for x in row2 if x])

        matched = []
        for r in rules:
            m = r.get("match", {})
            if cols is not None and "cols_expected" in m and int(cols) != int(m["cols_expected"]):
                continue
            r1 = [normalize(x) for x in m.get("row1_contains_all", [])]
            r2 = [normalize(x) for x in m.get("row2_contains_all", [])]
            if r1 and not all(tok in row1_join for tok in r1):
                continue
            if r2 and not all(tok in row2_join for tok in r2):
                continue
            matched.append(r.get("layout", r.get("name", "UNKNOWN")))

        if len(matched) == 1:
            print(f"{filename}: OK -> {matched[0]}")
        else:
            ok = False
            print(f"{filename}: PROBLEM -> {matched if matched else 'NO MATCH'}")

    raise SystemExit(0 if ok else 2)

if __name__ == "__main__":
    main()
