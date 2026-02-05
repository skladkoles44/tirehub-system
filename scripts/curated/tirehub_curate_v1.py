#!/usr/bin/env python3
import argparse, json, sys, hashlib
from pathlib import Path
from datetime import datetime, timezone

EXIT_OK=0
EXIT_ARGS=1
EXIT_FAIL=2

def die(msg:str, code:int=EXIT_ARGS):
    sys.stderr.write(msg+"\n")
    sys.exit(code)

def jdump(obj:dict)->str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",",":"))

def load_json(p:Path)->dict:
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        die(f"JSON read/parse failed: {p} :: {e}", EXIT_FAIL)

def parse_rfc3339_z(s:str)->datetime:
    try:
        dt=datetime.fromisoformat(s.replace("Z","+00:00"))
        if dt.tzinfo is None:
            raise ValueError("tz missing")
        return dt.astimezone(timezone.utc)
    except Exception as e:
        die(f"Invalid RFC3339Z: {s} :: {e}", EXIT_ARGS)

def sha256_file(p:Path)->str:
    h=hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024*1024), b""):
            h.update(chunk)
    return h.hexdigest()

def main():
    ap=argparse.ArgumentParser("tirehub-curated v1 (WBP)")
    ap.add_argument("--segment", required=True, help="SSOT facts segment .ndjson")
    ap.add_argument("--manifest", required=True, help="SSOT manifest .json")
    ap.add_argument("--out-dir", required=True, help="curated_v1/out")
    ap.add_argument("--run-id", required=True)
    args=ap.parse_args()

    seg=Path(args.segment)
    man=Path(args.manifest)
    out_root=Path(args.out_dir)/args.run_id
    if not seg.exists(): die(f"segment not found: {seg}", EXIT_ARGS)
    if not man.exists(): die(f"manifest not found: {man}", EXIT_ARGS)

    manifest=load_json(man)
    effective_at=manifest.get("effective_at")
    if not isinstance(effective_at,str): die("manifest missing effective_at", EXIT_FAIL)
    eff_dt=parse_rfc3339_z(effective_at)

    out_root.mkdir(parents=True, exist_ok=True)
    curated_path=out_root/"curated.ndjson"
    stats_path=out_root/"curated.stats.json"
    stderr_path=out_root/"stderr.log"

    # WBP: atomic write via tmp + rename
    tmp_cur=out_root/"curated.ndjson.tmp"
    tmp_stats=out_root/"curated.stats.json.tmp"
    tmp_err=out_root/"stderr.log.tmp"

    total_in=0
    kept=0
    dropped_price_or_qty=0
    bad_json=0
    bad_contract=0

    # deterministic output ordering: as input (segment already deterministic)
    with seg.open("r",encoding="utf-8") as f_in, tmp_cur.open("w",encoding="utf-8",newline="\n") as f_out, tmp_err.open("w",encoding="utf-8",newline="\n") as f_err:
        for line in f_in:
            total_in += 1
            line=line.rstrip("\n")
            if not line:
                continue
            try:
                obj=json.loads(line)
            except Exception:
                bad_json += 1
                continue

            # minimal contract checks (curated is consumer; fail-soft per-line, count)
            try:
                parsed=obj.get("parsed") or {}
                price=parsed.get("price")
                qty=parsed.get("qty")
                if price is None or qty is None:
                    dropped_price_or_qty += 1
                    continue
                if not isinstance(price,int) or not isinstance(qty,int):
                    bad_contract += 1
                    continue
                if price <= 0 or qty <= 0:
                    dropped_price_or_qty += 1
                    continue
            except Exception:
                bad_contract += 1
                continue

            # curated record: keep full fact for now (WBP: no business loss)
            f_out.write(jdump(obj)+"\n")
            kept += 1

        # echo summary into stderr log (WBP trace)
        summary={
            "run_id": args.run_id,
            "curated_version": "1.0.0",
            "effective_at": eff_dt.isoformat().replace("+00:00","Z"),
            "segment_ref": str(seg),
            "manifest_ref": str(man),
            "counts":{
                "input_lines": total_in,
                "kept_lines": kept,
                "dropped_price_or_qty": dropped_price_or_qty,
                "bad_json": bad_json,
                "bad_contract": bad_contract
            }
        }
        f_err.write(jdump(summary)+"\n")

    stats={
        "run_id": args.run_id,
        "curated_version": "1.0.0",
        "effective_at": eff_dt.isoformat().replace("+00:00","Z"),
        "inputs":{
            "segment": str(seg),
            "manifest": str(man),
            "segment_sha256": sha256_file(seg),
            "manifest_sha256": sha256_file(man)
        },
        "outputs":{
            "curated": str(curated_path),
            "stderr_log": str(stderr_path)
        },
        "counts":{
            "input_lines": total_in,
            "kept_lines": kept,
            "dropped_price_or_qty": dropped_price_or_qty,
            "bad_json": bad_json,
            "bad_contract": bad_contract
        }
    }

    tmp_stats.write_text(jdump(stats)+"\n", encoding="utf-8")
    tmp_cur.replace(curated_path)
    tmp_stats.replace(stats_path)
    tmp_err.replace(stderr_path)

    sys.stderr.write(jdump(stats)+"\n")
    sys.exit(EXIT_OK)

if __name__=="__main__":
    main()
