#!/usr/bin/env python3
from __future__ import annotations
import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

EXIT_OK = 0
EXIT_ARGS = 1
EXIT_FAIL = 20

def die(msg: str, code: int = EXIT_FAIL) -> None:
    sys.stderr.write(msg + "\n")
    raise SystemExit(code)

def jload(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        die(f"JSON_READ_FAIL: {path} :: {e}")

def jdump(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=2) + "\n"

def run_cmd(cmd: list[str], env: dict[str, str] | None = None) -> dict[str, Any]:
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env)
    return {"exit_code": proc.returncode, "output": proc.stdout}

def verdict_path_for(stats_path: Path) -> Path:
    name = stats_path.name
    if name.endswith(".stats.json"):
        return stats_path.with_name(name[:-11] + ".verdict.json")
    return stats_path.with_name(stats_path.stem + ".verdict.json")

def e2e_log_path_for(stats_path: Path) -> Path:
    name = stats_path.name
    if name.endswith(".stats.json"):
        return stats_path.with_name(name[:-11] + ".e2e.log")
    return stats_path.with_name(stats_path.stem + ".e2e.log")

def find_kolobox_baseline(repo_root: Path, item: dict[str, Any]) -> Path:
    layout = str(item.get("layout") or "")
    supplier = str(item.get("supplier_id") or "")
    if supplier != "kolobox":
        die(f"BASELINE_NOT_IMPLEMENTED: supplier={supplier}")
    return repo_root / "rulesets" / "gate_baselines" / "kolobox_xls_v1.baseline.json"

def main() -> int:
    ap = argparse.ArgumentParser(description="Batch E2E runner v1: report.json -> gate -> ingest -> curated")
    ap.add_argument("--report", required=True, help="path to report.json from run_inbox_batch_v1.py")
    ap.add_argument("--ssot-root", default=os.environ.get("SSOT_ROOT", ""), help="SSOT root dir")
    ap.add_argument("--curated-root", default=os.environ.get("CURATED_ROOT", ""), help="Curated root dir")
    ap.add_argument("--output", default="", help="output e2e report path (default: alongside report)")
    args = ap.parse_args()

    report_path = Path(args.report)
    if not report_path.exists():
        die(f"REPORT_NOT_FOUND: {report_path}", EXIT_ARGS)
    if not (args.ssot_root or "").strip():
        die("SSOT_ROOT_MISSING: pass --ssot-root or export SSOT_ROOT", EXIT_ARGS)
    if not (args.curated_root or "").strip():
        die("CURATED_ROOT_MISSING: pass --curated-root or export CURATED_ROOT", EXIT_ARGS)

    repo_root = Path(__file__).resolve().parents[2]
    gate_script = repo_root / "scripts" / "ingestion" / "kolobox" / "tirehub_gate_v1.py"
    ingest_script = repo_root / "scripts" / "ingestion" / "tirehub_ingest_v1.py"
    curate_script = repo_root / "scripts" / "curated" / "tirehub_curate_v1.py"

    for p in (gate_script, ingest_script, curate_script):
        if not p.exists():
            die(f"SCRIPT_NOT_FOUND: {p}", EXIT_ARGS)

    report = jload(report_path)
    items = report.get("items") or []
    if not isinstance(items, list):
        die("REPORT_INVALID: items must be list", EXIT_FAIL)

    out_path = Path(args.output) if (args.output or "").strip() else report_path.with_name(report_path.stem + ".e2e.json")
    env = os.environ.copy()
    env["SSOT_ROOT"] = args.ssot_root
    env["CURATED_ROOT"] = args.curated_root

    e2e: dict[str, Any] = {
        "runner": "run_batch_e2e_v1",
        "report": str(report_path),
        "ssot_root": args.ssot_root,
        "curated_root": args.curated_root,
        "items_total": len(items),
        "items": [],
    }

    for item in items:
        rec: dict[str, Any] = {
            "supplier_id": item.get("supplier_id"),
            "layout": item.get("layout"),
            "file": item.get("file"),
            "mapping": item.get("mapping"),
            "emitter_exit_code": item.get("exit_code"),
            "status": "init",
        }

        if int(item.get("exit_code") or 0) != 0:
            rec["status"] = "skipped_emitter_failed"
            e2e["items"].append(rec)
            continue

        supplier = str(item.get("supplier_id") or "")
        if supplier != "kolobox":
            rec["status"] = "skipped_supplier_not_supported_v1"
            e2e["items"].append(rec)
            continue

        stats_path = Path(str(item.get("out_stats") or ""))
        good_path = Path(str(item.get("out_ndjson") or ""))
        mapping_path = Path(str(item.get("mapping") or ""))
        if not stats_path.exists():
            rec["status"] = "failed_stats_missing"
            rec["error"] = str(stats_path)
            e2e["items"].append(rec)
            continue
        if not good_path.exists():
            rec["status"] = "failed_good_missing"
            rec["error"] = str(good_path)
            e2e["items"].append(rec)
            continue
        if not mapping_path.exists():
            rec["status"] = "failed_mapping_missing"
            rec["error"] = str(mapping_path)
            e2e["items"].append(rec)
            continue

        stats = jload(stats_path)
        run_id = str(stats.get("run_id") or "")
        rec["run_id"] = run_id
        verdict_path = verdict_path_for(stats_path)
        rec["verdict_path"] = str(verdict_path)
        rec["e2e_log"] = str(e2e_log_path_for(stats_path))
        baseline_path = find_kolobox_baseline(repo_root, item)
        rec["baseline_path"] = str(baseline_path)

        gate_cmd = [
            sys.executable,
            str(gate_script),
            "--stats", str(stats_path),
            "--baseline", str(baseline_path),
            "--out", str(verdict_path),
        ]
        gate_res = run_cmd(gate_cmd, env=env)
        rec["gate_exit_code"] = gate_res["exit_code"]
        rec["gate_output"] = gate_res["output"]
        if not verdict_path.exists():
            rec["status"] = "failed_gate_no_verdict"
            Path(rec["e2e_log"]).write_text(gate_res["output"], encoding="utf-8")
            e2e["items"].append(rec)
            continue

        verdict = jload(verdict_path)
        rec["gate_verdict"] = verdict.get("verdict")
        if verdict.get("verdict") not in ("PASS", "WARN"):
            rec["status"] = "blocked_by_gate"
            Path(rec["e2e_log"]).write_text(gate_res["output"], encoding="utf-8")
            e2e["items"].append(rec)
            continue

        ingest_cmd = [
            sys.executable,
            str(ingest_script),
            "--ssot-root", args.ssot_root,
            "--good", str(good_path),
            "--stats", str(stats_path),
            "--verdict", str(verdict_path),
            "--mapping", str(mapping_path),
        ]
        ingest_res = run_cmd(ingest_cmd, env=env)
        rec["ingest_exit_code"] = ingest_res["exit_code"]
        rec["ingest_output"] = ingest_res["output"]
        manifest_path = Path(args.ssot_root) / "manifests" / f"{run_id}.json"
        rec["manifest_path"] = str(manifest_path)
        rec["resolved_manifest_run_id"] = manifest_path.stem

        ingest_payload = None
        try:
            ingest_payload = json.loads((ingest_res.get("output") or "").strip())
        except Exception:
            ingest_payload = None

        ingest_status = str((ingest_payload or {}).get("status") or "")
        rec["ingest_status"] = ingest_status

        if ingest_res["exit_code"] != 0:
            rec["status"] = "failed_ingest"
            Path(rec["e2e_log"]).write_text(gate_res["output"] + "\n" + ingest_res["output"], encoding="utf-8")
            e2e["items"].append(rec)
            continue

        if ingest_status == "already_ingested":
            rec["marker_path"] = (ingest_payload or {}).get("marker")
            marker_path = Path(str(rec["marker_path"] or ""))
            marker_payload = None
            if marker_path.exists():
                try:
                    marker_payload = jload(marker_path)
                except Exception:
                    marker_payload = None
            marker_manifest_ref = str((marker_payload or {}).get("manifest_ref") or "")
            if marker_manifest_ref:
                manifest_path = Path(marker_manifest_ref)
                rec["manifest_path"] = str(manifest_path)
                rec["resolved_manifest_run_id"] = manifest_path.stem
            if not marker_manifest_ref or not manifest_path.exists():
                rec["status"] = "already_ingested_no_manifest"
                Path(rec["e2e_log"]).write_text(gate_res["output"] + "\n" + ingest_res["output"], encoding="utf-8")
                e2e["items"].append(rec)
                continue
            ingest_status = "ingested"
            rec["ingest_status"] = ingest_status

        if ingest_status != "ingested" or not manifest_path.exists():
            rec["status"] = "failed_ingest"
            Path(rec["e2e_log"]).write_text(gate_res["output"] + "\n" + ingest_res["output"], encoding="utf-8")
            e2e["items"].append(rec)
            continue

        curate_cmd = [
            sys.executable,
            str(curate_script),
            "--manifest", str(manifest_path),
            "--out-dir", args.curated_root,
        ]
        curate_res = run_cmd(curate_cmd, env=env)
        rec["curated_exit_code"] = curate_res["exit_code"]
        rec["curated_output"] = curate_res["output"]

        curated_payload = None
        try:
            curated_payload = json.loads((curate_res.get("output") or "").strip())
        except Exception:
            curated_payload = None

        curated_dir = ""
        if isinstance(curated_payload, dict):
            outputs = curated_payload.get("outputs") or {}
            curated_path = str((outputs or {}).get("curated") or "")
            if curated_path:
                curated_dir = str(Path(curated_path).parent)
        if not curated_dir:
            curated_dir = str(Path(args.curated_root) / manifest_path.stem)
        rec["curated_dir"] = curated_dir

        if curate_res["exit_code"] != 0:
            rec["status"] = "failed_curated"
            Path(rec["e2e_log"]).write_text(gate_res["output"] + "\n" + ingest_res["output"] + "\n" + curate_res["output"], encoding="utf-8")
            e2e["items"].append(rec)
            continue

        rec["status"] = "ok"
        Path(rec["e2e_log"]).write_text(gate_res["output"] + "\n" + ingest_res["output"] + "\n" + curate_res["output"], encoding="utf-8")
        e2e["items"].append(rec)

    counts: dict[str, int] = {}
    for rec in e2e["items"]:
        st = str(rec.get("status") or "unknown")
        counts[st] = counts.get(st, 0) + 1
    e2e["counts"] = counts
    out_path.write_text(jdump(e2e), encoding="utf-8")
    print(f"E2E_REPORT={out_path}")
    print(jdump(e2e).rstrip())
    return EXIT_OK

if __name__ == "__main__":
    raise SystemExit(main())
