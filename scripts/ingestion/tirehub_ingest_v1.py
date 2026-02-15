#!/usr/bin/env python3
import argparse, json, sys, os, hashlib
from pathlib import Path
from datetime import datetime, timezone

EXIT_OK = 0
EXIT_ARGS = 1
EXIT_FAIL = 20

REQUIRED_GOOD_FIELDS = [
  "supplier_id","parser_id","mapping_version","mapping_hash","ndjson_contract_version",
  "emitter_version","run_id","effective_at","sku_candidate_key","raw","parsed","quality_flags","_meta"
]

def die(msg: str, code: int = EXIT_FAIL):
  sys.stderr.write(msg + "\n")
  sys.exit(code)

def json_load(path: Path) -> dict:
  try:
    return json.loads(path.read_text(encoding="utf-8"))
  except Exception as e:
    die(f"JSON read/parse failed: {path} :: {e}", EXIT_FAIL)

def sha256_bytes(b: bytes) -> str:
  return hashlib.sha256(b).hexdigest()

def sha256_file(path: Path) -> str:
  return sha256_bytes(path.read_bytes())


def accepted_marker_path(ssot_root: Path, supplier_id: str, input_sha256: str) -> Path:
  supplier = (supplier_id or "").strip().lower()
  sha = (input_sha256 or "").strip().lower()
  if len(sha) < 2:
    die(f"INPUT_SHA256_INVALID: {sha}", EXIT_FAIL)
  return ssot_root / "accepted_runs" / supplier / sha[:2] / f"{sha}.json"

def sha256_lf_normalized(path: Path) -> str:
  return sha256_bytes(path.read_bytes().replace(b"\r\n", b"\n"))

def ensure_dir(p: Path):
  p.mkdir(parents=True, exist_ok=True)

def atomic_write_text(path: Path, text: str):
  ensure_dir(path.parent)
  tmp = path.with_suffix(path.suffix + ".tmp")
  tmp.write_text(text, encoding="utf-8", newline="\n")
  tmp.replace(path)

def compact(obj: dict) -> str:
  return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",",":")) + "\n"

def acquire_lock(lock_path: Path):
  ensure_dir(lock_path.parent)
  try:
    fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    os.close(fd)
  except FileExistsError:
    die(f"LOCKED: {lock_path}", EXIT_ARGS)
  except Exception as e:
    die(f"LOCK_CREATE_FAILED: {lock_path} :: {e}", EXIT_FAIL)

def release_lock(lock_path: Path):
  try:
    if lock_path.exists():
      lock_path.unlink()
  except Exception:
    pass

def parse_effective_month(effective_at: str) -> str:
  # RFC3339Z -> YYYY-MM
  try:
    dt = datetime.fromisoformat(effective_at.replace("Z","+00:00"))
    if dt.tzinfo is None:
      raise ValueError("tzinfo missing")
    dt = dt.astimezone(timezone.utc)
    return f"{dt.year:04d}-{dt.month:02d}"
  except Exception as e:
    die(f"Invalid effective_at format (need RFC3339Z): {effective_at} :: {e}", EXIT_FAIL)

def validate_good_line(obj: dict, expected: dict):
  for k in REQUIRED_GOOD_FIELDS:
    if k not in obj:
      die(f"GOOD_CONTRACT_MISSING_FIELD: {k}", EXIT_FAIL)

  if not isinstance(obj["quality_flags"], list):
    die("GOOD_CONTRACT_BAD_TYPE: quality_flags must be list", EXIT_FAIL)
  if not isinstance(obj["_meta"], dict):
    die("GOOD_CONTRACT_BAD_TYPE: _meta must be object", EXIT_FAIL)
  if "source_row_number" not in obj["_meta"] or not isinstance(obj["_meta"]["source_row_number"], int):
    die("GOOD_CONTRACT_BAD_META: _meta.source_row_number int required", EXIT_FAIL)

  if obj["supplier_id"] != expected["supplier_id"]:
    die(f"SUPPLIER_ID_MISMATCH: {obj['supplier_id']} != {expected['supplier_id']}", EXIT_FAIL)
  if obj["parser_id"] != expected["parser_id"]:
    die(f"PARSER_ID_MISMATCH: {obj['parser_id']} != {expected['parser_id']}", EXIT_FAIL)
  if obj["run_id"] != expected["run_id"]:
    die(f"RUN_ID_MISMATCH: {obj['run_id']} != {expected['run_id']}", EXIT_FAIL)
  if obj["effective_at"] != expected["effective_at"]:
    die(f"EFFECTIVE_AT_MISMATCH: {obj['effective_at']} != {expected['effective_at']}", EXIT_FAIL)
  if obj["mapping_hash"] != expected["mapping_hash"]:
    die(f"MAPPING_HASH_MISMATCH: {obj['mapping_hash']} != {expected['mapping_hash']}", EXIT_FAIL)
  if str(obj["mapping_version"]) != str(expected["mapping_version"]):
    die(f"MAPPING_VERSION_MISMATCH: {obj['mapping_version']} != {expected['mapping_version']}", EXIT_FAIL)

  parsed = obj["parsed"]
  if not isinstance(parsed, dict):
    die("GOOD_CONTRACT_BAD_TYPE: parsed must be object", EXIT_FAIL)

  price = parsed.get("price", None)
  qty = parsed.get("qty", None)
  if price is not None and not isinstance(price, int):
    die("GOOD_CONTRACT_BAD_TYPE: parsed.price must be int or null", EXIT_FAIL)
  if qty is not None and not isinstance(qty, int):
    die("GOOD_CONTRACT_BAD_TYPE: parsed.qty must be int or null", EXIT_FAIL)

def main():
  ap = argparse.ArgumentParser("tirehub-ingest v1 (SSOT segmented by run_id)")
  ap.add_argument("--good", required=True, help="path to out/<run_id>/good.ndjson")
  ap.add_argument("--stats", required=True, help="path to out/<run_id>/stats.json")
  ap.add_argument("--verdict", required=True, help="path to out/<run_id>/verdict.json (gate)")
  ap.add_argument("--mapping", required=False, help="path to mapping.yaml (optional but recommended)")
  ap.add_argument("--ssot-root", required=False, default="/home/etl/apps/tirehub/ssot", help="SSOT root dir")
  args = ap.parse_args()

  good_path = Path(args.good)
  stats_path = Path(args.stats)
  verdict_path = Path(args.verdict)
  mapping_path = Path(args.mapping) if args.mapping else None
  ssot_root = Path(args.ssot_root)

  for p in [good_path, stats_path, verdict_path]:
    if not p.exists():
      die(f"NOT_FOUND: {p}", EXIT_ARGS)

  verdict = json_load(verdict_path)
  v = verdict.get("verdict")
  if v not in ["PASS","WARN"]:
    die(f"INGEST_BLOCKED_BY_GATE: verdict={v}", EXIT_FAIL)

  stats = json_load(stats_path)
  for k in ["run_id","supplier_id","parser_id","effective_at","mapping_hash","mapping_version","good_rows","file_readable","structure_ok"]:
    if k not in stats:
      die(f"STATS_MISSING_FIELD: {k}", EXIT_FAIL)

  expected = {
    "run_id": stats["run_id"],
    "supplier_id": stats["supplier_id"],
    "parser_id": stats["parser_id"],
    "effective_at": stats["effective_at"],
    "mapping_hash": stats["mapping_hash"],
    "mapping_version": str(stats["mapping_version"]),
  }

  # mapping_hash cross-check (recommended)
  if mapping_path is not None:
    if not mapping_path.exists():
      die(f"MAPPING_NOT_FOUND: {mapping_path}", EXIT_ARGS)
    mh = sha256_lf_normalized(mapping_path)
    if mh != expected["mapping_hash"]:
      die(f"MAPPING_HASH_MISMATCH_FILE: {mh} != {expected['mapping_hash']}", EXIT_FAIL)

  # idempotency marker (supplier + input sha256)
  input_sha256 = sha256_file(source_path)
  marker_path = accepted_marker_path(ssot_root, expected["supplier_id"], input_sha256)
  if marker_path.exists():
    out = {
      "status": "already_ingested",
      "run_id": expected["run_id"],
      "supplier_id": expected["supplier_id"],
      "input_sha256": input_sha256,
      "marker": str(marker_path),
    }
    sys.stderr.write(compact(out))
    sys.exit(EXIT_OK)

  locks_dir = ssot_root / "locks"
  lock_path = locks_dir / f"{expected['supplier_id']}.lock"
  acquire_lock(lock_path)

  tmp_dir = ssot_root / "tmp"
  facts_root = ssot_root / "facts"
  manifests_dir = ssot_root / "manifests"

  tmp_path = tmp_dir / f"{expected['run_id']}.ndjson.tmp"

  ingested_at = datetime.now(timezone.utc).isoformat().replace("+00:00","Z")
  source_rows = 0
  written_lines = 0

  try:
    ensure_dir(tmp_dir)
    # validate + copy to tmp with LF newlines
    with good_path.open("r", encoding="utf-8", errors="strict") as fin, tmp_path.open("w", encoding="utf-8", newline="\n") as fout:
      for line in fin:
        line = line.strip()
        if not line:
          continue
        try:
          obj = json.loads(line)
        except Exception as e:
          die(f"NDJSON_PARSE_FAIL: {e}", EXIT_FAIL)
        validate_good_line(obj, expected)
        source_rows += 1
        fout.write(json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",",":")) + "\n")
        written_lines += 1

    # sanity vs stats.good_rows (warn-level mismatch treated as FAIL here: ingestion must be strict)
    if int(stats["good_rows"]) != written_lines:
      die(f"GOOD_ROWS_MISMATCH: stats={stats['good_rows']} actual={written_lines}", EXIT_FAIL)

    month = parse_effective_month(expected["effective_at"])
    seg_dir = facts_root / expected["supplier_id"] / expected["parser_id"] / month
    ensure_dir(seg_dir)
    final_seg_path = seg_dir / f"{expected['run_id']}.ndjson"

    # atomic commit: tmp -> final
    tmp_path.replace(final_seg_path)

    # manifest (after segment)
    manifest_path = manifests_dir / f"{expected['run_id']}.json"
    manifest = {
      "ingestion_version": "1.0.0",
      "run_id": expected["run_id"],
      "supplier_id": expected["supplier_id"],
      "parser_id": expected["parser_id"],
      "effective_at": expected["effective_at"],
      "mapping_version": expected["mapping_version"],
      "mapping_hash": expected["mapping_hash"],
      "paths": {
        "segment": str(final_seg_path),
        "stats": str(stats_path),
        "verdict": str(verdict_path),
        "mapping": str(mapping_path) if mapping_path else None,
        "accepted_marker": str(marker_path),
      },
      "sha256": {
        "segment": sha256_file(final_seg_path),
        "good_input": sha256_file(good_path),
        "stats": sha256_file(stats_path),
        "verdict": sha256_file(verdict_path),
        "mapping_lf": sha256_lf_normalized(mapping_path) if mapping_path else None,
      },
      "counts": {
        "lines_written": written_lines,
      },
      "ingested_at": ingested_at,
    }
    atomic_write_text(manifest_path, compact(manifest))

    # marker (strictly after manifest)
    ensure_dir(marker_path.parent)
    marker = {"status":"accepted","run_id": expected["run_id"],"supplier_id": expected["supplier_id"],"input_sha256": input_sha256,"ingested_at": ingested_at,"manifest_ref": str(manifest_path)}
    atomic_write_text(marker_path, compact(marker))

    out = {
      "status": "ingested",
      "run_id": expected["run_id"],
      "segment": str(final_seg_path),
      "marker": str(marker_path),
      "manifest": str(manifest_path),
    }
    sys.stderr.write(compact(out))
    sys.exit(EXIT_OK)

  finally:
    # cleanup tmp (if still exists) + lock
    try:
      if tmp_path.exists():
        tmp_path.unlink()
    except Exception:
      pass
    release_lock(lock_path)

if __name__ == "__main__":
  main()
