#!/usr/bin/env python3
import argparse, json, sys, hashlib
from pathlib import Path
from datetime import datetime, timezone

CURATED_VERSION="1.1.0"
EXIT_OK=0
EXIT_FAIL=1

REQUIRED_GOOD_FIELDS=[
  "supplier_id","parser_id","mapping_version","mapping_hash","ndjson_contract_version",
  "emitter_version","run_id","effective_at","sku_candidate_key","raw","parsed","quality_flags","_meta"
]

def die(msg:str,code:int=EXIT_FAIL):
  sys.stderr.write(msg+"\n")
  sys.exit(code)

def jdump(obj:dict)->str:
  return json.dumps(obj,ensure_ascii=False,sort_keys=True,separators=(",",":"))

def sha256_file(p:Path)->str:
  h=hashlib.sha256()
  with p.open("rb") as f:
    for chunk in iter(lambda:f.read(1024*1024), b""):
      h.update(chunk)
  return h.hexdigest()

def load_json(p:Path)->dict:
  try:
    return json.loads(p.read_text(encoding="utf-8"))
  except Exception as e:
    die(f"JSON read/parse failed: {p} :: {e}")

def now_rfc3339z()->str:
  return datetime.now(timezone.utc).isoformat().replace("+00:00","Z")

def validate_good(obj:dict)->bool:
  for k in REQUIRED_GOOD_FIELDS:
    if k not in obj: return False
  if not isinstance(obj.get("parsed"), dict): return False
  if not isinstance(obj.get("raw"), dict): return False
  if not isinstance(obj.get("quality_flags"), list): return False
  if not isinstance(obj.get("_meta"), dict): return False
  return True

def drop_reason(price, qty)->str:
  reasons=[]
  # WBP v2: не дропаем факт из-за отсутствия цены (qty>0 уже отфильтрован в emitter)
  pass  # price_missing -> quality only
  # WBP v2: не дропаем факт из-за цены<=0 (если такое встретится)
  pass  # price_nonpositive -> quality only
  if qty is None: reasons.append("qty_missing")
  elif isinstance(qty,(int,float)) and qty <= 0: reasons.append("qty_nonpositive")
  return "_and_".join(reasons) if reasons else "unknown"

def main():
  ap=argparse.ArgumentParser("tirehub-curated v1.1")
  ap.add_argument("--manifest",required=True,help="path to SSOT manifest json")
  ap.add_argument("--out-dir",required=False,default="curated_v1/out",help="base output dir")
  ap.add_argument("--max-dropped-samples",type=int,default=50,help="cap for dropped_samples.ndjson (0 disables)")
  args=ap.parse_args()

  manifest_path=Path(args.manifest)
  if not manifest_path.exists(): die(f"manifest not found: {manifest_path}")

  manifest=load_json(manifest_path)
  run_id=manifest.get("run_id")
  paths=(manifest.get("paths") or {})
  seg_path=paths.get("segment")
  if not run_id or not seg_path: die("manifest missing run_id or paths.segment")

  segment_path=Path(seg_path)
  if not segment_path.exists(): die(f"segment not found: {segment_path}")

  out_base=Path(args.out_dir)/run_id
  out_base.mkdir(parents=True,exist_ok=True)
  curated_path=out_base/"curated.ndjson"
  dropped_path=out_base/"dropped_samples.ndjson"
  stderr_path=out_base/"stderr.log"
  stats_path=out_base/"curated.stats.json"

  curated_f=curated_path.open("w",encoding="utf-8",newline="\n")
  dropped_f=dropped_path.open("w",encoding="utf-8",newline="\n")
  stderr_f=stderr_path.open("w",encoding="utf-8",newline="\n")

  input_lines=0
  kept_lines=0
  dropped_price_or_qty=0
  bad_json=0
  bad_contract=0
  drop_counts={}
  dropped_samples_written=0
  max_samples=max(0,int(args.max_dropped_samples))

  effective_at=None

  with segment_path.open("r",encoding="utf-8") as f:
    for line_no, line in enumerate(f, start=1):
      line=line.rstrip("\n")
      if not line: continue
      input_lines += 1
      try:
        obj=json.loads(line)
      except Exception:
        bad_json += 1
        continue

      if not validate_good(obj):
        bad_contract += 1
        continue

      if effective_at is None:
        effective_at = obj.get("effective_at")

      parsed=obj.get("parsed") or {}
      price=parsed.get("price")
      qty=parsed.get("qty")

      # eligibility: qty>0 (price may be missing; that is quality-only)
      ok = (isinstance(qty,int) and qty>0)
      if ok:
        curated_f.write(jdump(obj)+"\n")
        kept_lines += 1
        continue

      dropped_price_or_qty += 1
      r=drop_reason(price,qty)
      drop_counts[r]=int(drop_counts.get(r,0))+1

      if max_samples>0 and dropped_samples_written < max_samples:
        raw=obj.get("raw") or {}
        sample={
          "run_id": obj.get("run_id"),
          "seg_line_no": line_no,
          "_meta": {"source_row_number": (obj.get("_meta") or {}).get("source_row_number")},
          "sku_candidate_key": obj.get("sku_candidate_key"),
          "supplier_warehouse_name": raw.get("supplier_warehouse_name"),
          "parsed": {"price": price, "qty": qty},
          "quality_flags": obj.get("quality_flags") or [],
          "drop_reason": r,
          "raw_snapshot": {
            "price_raw": raw.get("price_raw"),
            "qty_raw": raw.get("qty_raw"),
          },
        }
        dropped_f.write(jdump(sample)+"\n")
        dropped_samples_written += 1

  curated_f.close(); dropped_f.close(); stderr_f.close()

  manifest_sha=sha256_file(manifest_path)
  segment_sha=sha256_file(segment_path)

  out={
    "run_id": run_id,
    "curated_version": CURATED_VERSION,
    "effective_at": effective_at,
    "counts": {
      "input_lines": input_lines,
      "kept_lines": kept_lines,
      "dropped_price_or_qty": dropped_price_or_qty,
      "bad_json": bad_json,
      "bad_contract": bad_contract,
      "dropped_samples_written": dropped_samples_written,
    },
    "drop_counts": drop_counts,
    "inputs": {
      "manifest": str(manifest_path),
      "manifest_sha256": manifest_sha,
      "segment": str(segment_path),
      "segment_sha256": segment_sha,
    },
    "outputs": {
      "curated": str(curated_path),
      "dropped_samples": str(dropped_path),
      "stderr_log": str(stderr_path),
    },
    "ingested_at": now_rfc3339z(),
  }

  stats_path.write_text(jdump(out)+"\n",encoding="utf-8")
  stderr_path.write_text(jdump(out)+"\n",encoding="utf-8")
  sys.stderr.write(jdump(out)+"\n")
  sys.exit(EXIT_OK)

if __name__=="__main__":
  main()
