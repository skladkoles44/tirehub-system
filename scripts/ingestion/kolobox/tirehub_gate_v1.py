#!/usr/bin/env python3
import argparse, json, sys
from pathlib import Path

EXIT_PASS = 0
EXIT_WARN = 10
EXIT_FAIL = 20

def die(msg: str, code: int = EXIT_FAIL):
    sys.stderr.write(msg + "\n")
    sys.exit(code)

def load_json(p: Path) -> dict:
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        die(f"JSON read/parse failed: {p} :: {e}", EXIT_FAIL)

def get_value(d: dict, path: str):
    cur = d
    for part in path.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return None
    return cur

def check_baseline(stats: dict, baseline_path: Path | None) -> list:
    reasons = []
    if baseline_path is None:
        reasons.append({"level": "WARN", "code": "baseline_missing", "detail": "baseline not configured"})
        return reasons
    if not baseline_path.exists():
        reasons.append({"level": "WARN", "code": "baseline_missing", "detail": f"baseline file not found: {baseline_path}"})
        return reasons

    baseline = load_json(baseline_path)

    if stats.get("parser_id") != baseline.get("parser_id"):
        reasons.append({"level": "WARN", "code": "baseline_mismatch", "detail": f"parser_id mismatch: stats={stats.get('parser_id')}, baseline={baseline.get('parser_id')}"})
        return reasons

    metrics = baseline.get("metrics", {})
    for metric_path, rule in metrics.items():
        expected = rule.get("expected")
        tolerance = rule.get("tolerance_abs", 0)
        actual = get_value(stats, metric_path)

        if actual is None:
            reasons.append({"level": "WARN", "code": "metric_missing", "detail": f"{metric_path} not in stats"})
            continue

        if isinstance(expected, str):
            if str(actual) != expected:
                reasons.append({"level": "WARN", "code": "metric_out_of_tolerance", "detail": f"{metric_path}: expected '{expected}', got '{actual}'"})
        elif isinstance(expected, (int, float)):
            try:
                if abs(float(actual) - float(expected)) > float(tolerance):
                    reasons.append({"level": "WARN", "code": "metric_out_of_tolerance", "detail": f"{metric_path}: expected {expected}±{tolerance}, got {actual}"})
            except Exception:
                reasons.append({"level": "WARN", "code": "metric_out_of_tolerance", "detail": f"{metric_path}: non-numeric actual={actual}, expected={expected}"})
        else:
            # неизвестный тип expected — считаем это baseline ошибкой
            reasons.append({"level": "WARN", "code": "baseline_invalid", "detail": f"{metric_path}: unsupported expected type {type(expected)}"})

    return reasons

def main():
    ap = argparse.ArgumentParser("tirehub-gate v1")
    ap.add_argument("--stats", required=True, help="path to stats.json from emitter")
    ap.add_argument("--out", required=False, help="path to verdict.json (will be overwritten)")
    ap.add_argument("--baseline", required=False, help="path to baseline.json file")
    args = ap.parse_args()

    stats_path = Path(args.stats)
    baseline_path = Path(args.baseline) if args.baseline else None

    if not stats_path.exists():
        die(f"stats.json not found: {stats_path}", EXIT_FAIL)

    stats = load_json(stats_path)

    # normalize minimal emitter stats (e.g. centrshin JSON emitters) to gate-required schema
    stats.setdefault("file_readable", True)
    stats.setdefault("format_ok", True)
    stats.setdefault("header_ok", True)
    stats.setdefault("structure_ok", True)
    stats.setdefault("required_columns_ok", True)

    # common counters (fallbacks)
    if "good_rows" not in stats:
        try:
            stats["good_rows"] = int(stats.get("good_rows") or stats.get("lines") or stats.get("emitted_items") or 0)
        except Exception:
            stats["good_rows"] = 0
    if "bad_rows" not in stats:
        try:
            stats["bad_rows"] = int(stats.get("bad_rows") or 0)
        except Exception:
            stats["bad_rows"] = 0

    # source_rows_read: how many source rows/items were read (best-effort)
    if "source_rows_read" not in stats:
        try:
            stats["source_rows_read"] = int(stats.get("seen_items") or stats.get("seen") or (stats["good_rows"] + stats["bad_rows"]))
        except Exception:
            stats["source_rows_read"] = stats["good_rows"] + stats["bad_rows"]

    # exploded_lines: how many NDJSON lines were produced after "explosion"/flattening
    if "exploded_lines" not in stats:
        try:
            stats["exploded_lines"] = int(stats.get("exploded_lines") or stats["good_rows"])
        except Exception:
            stats["exploded_lines"] = stats["good_rows"]

    # explosion_factor_exact: exploded_lines / source_rows_read (string or number ok; keep float)
    if "explosion_factor_exact" not in stats:
        denom = stats.get("source_rows_read") or 0
        try:
            denom = int(denom)
        except Exception:
            denom = 0
        if denom <= 0:
            stats["explosion_factor_exact"] = 0.0
        else:
            stats["explosion_factor_exact"] = float(stats.get("exploded_lines") or 0) / float(denom)

    # flags_counts: map of sanity flags -> counts
    if "flags_counts" not in stats or not isinstance(stats.get("flags_counts"), dict):
        stats["flags_counts"] = {}

    # has_bad_rows: derived
    if "has_bad_rows" not in stats:
        try:
            stats["has_bad_rows"] = int(stats.get("bad_rows") or 0) > 0
        except Exception:
            stats["has_bad_rows"] = False
    required = [
        "run_id","supplier_id","parser_id",
        "file_readable","structure_ok",
        "good_rows","bad_rows",
        "exploded_lines","explosion_factor_exact","source_rows_read",
        "flags_counts"
    ]
    for k in required:
        if k not in stats:
            die(f"stats.json missing field: {k}", EXIT_FAIL)

    reasons = []

    # FAIL rules
    if not bool(stats["file_readable"]):
        reasons.append({"level": "FAIL", "code": "file_not_readable", "detail": "file_readable=false"})
    if not bool(stats["structure_ok"]):
        reasons.append({"level": "FAIL", "code": "structure_not_ok", "detail": "structure_ok=false"})
    if int(stats["exploded_lines"]) == 0:
        reasons.append({"level": "FAIL", "code": "exploded_lines_zero", "detail": "exploded_lines == 0"})

    if any(r["level"] == "FAIL" for r in reasons):
        verdict = "FAIL"
        exit_code = EXIT_FAIL
    else:
        # WARN rules: baseline check
        reasons.extend(check_baseline(stats, baseline_path))

        # other WARN signals
        if int(stats["bad_rows"]) > 0:
            reasons.append({"level": "WARN", "code": "has_bad_rows", "detail": f"bad_rows={stats['bad_rows']}"})

        flags_counts = stats.get("flags_counts", {}) or {}
        if int(flags_counts.get("negative_price", 0)) > 0:
            reasons.append({"level": "WARN", "code": "has_negative_price", "detail": f"negative_price={flags_counts.get('negative_price')}"})

        if any(r["level"] == "WARN" for r in reasons):
            verdict = "WARN"
            exit_code = EXIT_WARN
        else:
            verdict = "PASS"
            exit_code = EXIT_PASS

    out = {
        "gate_version": "1.0.0",
        "run_id": stats["run_id"],
        "supplier_id": stats["supplier_id"],
        "parser_id": stats["parser_id"],
        "verdict": verdict,
        "reasons": reasons,
        "stats_ref": str(stats_path),
        "baseline_used": str(baseline_path) if baseline_path else None
    }

    payload = json.dumps(out, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"

    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(payload, encoding="utf-8")

    sys.stderr.write(payload)
    sys.exit(exit_code)

if __name__ == "__main__":
    main()
