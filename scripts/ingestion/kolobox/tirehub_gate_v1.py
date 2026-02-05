#!/usr/bin/env python3
import argparse, json, sys
from pathlib import Path
from decimal import Decimal

EXIT_PASS=0
EXIT_WARN=10
EXIT_FAIL=20

def die(msg:str, code:int=EXIT_FAIL):
    sys.stderr.write(msg+"\n")
    sys.exit(code)

def jload(p:Path)->dict:
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        die(f"stats.json read/parse failed: {p} :: {e}", EXIT_FAIL)

def main():
    ap=argparse.ArgumentParser("tirehub-gate v1")
    ap.add_argument("--stats", required=True, help="path to stats.json from emitter")
    ap.add_argument("--out", required=True, help="path to verdict.json (will be overwritten)")
    ap.add_argument("--fail-on-file-level-errors", action="store_true", default=True)
    args=ap.parse_args()

    stats_path=Path(args.stats)
    out_path=Path(args.out)
    if not stats_path.exists(): die(f"stats.json not found: {stats_path}", EXIT_FAIL)

    s=jload(stats_path)

    # required fields (minimal)
    for k in ["run_id","supplier_id","parser_id","file_readable","structure_ok","good_rows","bad_rows","exploded_lines","explosion_factor_exact","source_rows_read"]:
        if k not in s: die(f"stats.json missing field: {k}", EXIT_FAIL)

    file_readable=bool(s["file_readable"])
    structure_ok=bool(s["structure_ok"])
    good_rows=int(s["good_rows"])
    bad_rows=int(s["bad_rows"])
    exploded_lines=int(s["exploded_lines"])
    source_rows_read=int(s["source_rows_read"])
    try:
        explosion_factor_exact=Decimal(str(s["explosion_factor_exact"]))
    except Exception:
        die("stats.json invalid explosion_factor_exact", EXIT_FAIL)

    reasons=[]

    # FAIL rules (канон v1)
    if exploded_lines==0:
        reasons.append({"level":"FAIL","code":"exploded_lines_zero","detail":"exploded_lines == 0"})
    if not file_readable:
        reasons.append({"level":"FAIL","code":"file_not_readable","detail":"file_readable=false"})
    if not structure_ok:
        reasons.append({"level":"FAIL","code":"structure_not_ok","detail":"structure_ok=false"})
    if explosion_factor_exact > Decimal("50"):
        reasons.append({"level":"FAIL","code":"explosion_factor_too_high","detail":f"explosion_factor_exact={explosion_factor_exact}"})
    if exploded_lines > 5_000_000:
        reasons.append({"level":"FAIL","code":"exploded_lines_too_many","detail":f"exploded_lines={exploded_lines}"})
    if good_rows==0 and source_rows_read>0:
        # файл прочитан, но фактов нет — подозрительно
        reasons.append({"level":"FAIL","code":"no_good_facts","detail":"good_rows == 0 with readable/structured file"})

    # WARN rules (канон v1 базово, без baseline)
    if bad_rows>0:
        reasons.append({"level":"WARN","code":"has_bad_rows","detail":f"bad_rows={bad_rows}"})

    flags_counts=s.get("flags_counts",{}) or {}
    # high-priority warn: negative_price
    if int(flags_counts.get("negative_price",0))>0:
        reasons.append({"level":"WARN","code":"has_negative_price","detail":f"negative_price={flags_counts.get('negative_price')}"})

    # baseline_missing -> WARN (пока baseline не реализован)
    reasons.append({"level":"WARN","code":"baseline_missing","detail":"baseline not configured"})

    verdict="PASS"
    exit_code=EXIT_PASS
    if any(r["level"]=="FAIL" for r in reasons):
        verdict="FAIL"
        exit_code=EXIT_FAIL
    elif any(r["level"]=="WARN" for r in reasons):
        verdict="WARN"
        exit_code=EXIT_WARN

    out={
        "gate_version":"1.0.0",
        "run_id":s["run_id"],
        "supplier_id":s["supplier_id"],
        "parser_id":s["parser_id"],
        "verdict":verdict,
        "reasons":reasons,
        "stats_ref":str(stats_path),
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, ensure_ascii=False, sort_keys=True, separators=(",",":"))+"\n", encoding="utf-8")
    sys.stderr.write(json.dumps(out, ensure_ascii=False, sort_keys=True, separators=(",",":"))+"\n")
    sys.exit(exit_code)

if __name__=="__main__":
    main()
