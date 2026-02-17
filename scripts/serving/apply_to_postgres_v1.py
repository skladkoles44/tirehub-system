#!/usr/bin/env python3
"""
apply_to_postgres_v1.py

Cosmetic upgrades:
- --log-file (append logs to file; stderr still used unless --quiet)
- retry/backoff on transient DB errors during batch flush (deadlock/serialization/lock timeout)
- supplier_id validation (safe charset)
- unified logging (warn/progress go through logger)
- still MVP-simple: no insert-vs-update split, no map-change cross-run checks

Notes:
- stats["master_base_inserted"] counts ONLY newly inserted base anchors (ON CONFLICT DO NOTHING -> rowcount=0 if existed).
- If base_sku == internal_candidate (no variation tokens in key), variation attrs are NOT stored (treated as base row).
"""

import argparse
import json
import os
import sys
from pathlib import Path
# ensure repo root is on sys.path when running as a script
_REPO_ROOT = str(Path(__file__).resolve().parents[2])
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)
import time
from datetime import datetime, timezone
from typing import Optional, Any, Dict, List, Callable
import re
import logging

from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError, OperationalError, DBAPIError

try:
    from scripts.serving.sku_normalize_v1 import build_skus, norm_int, norm_season, norm_brand  # type: ignore
except Exception:
    # Fallback минималка для PHONE: достаточно для MVP B-ключей
    import re as _re
    from dataclasses import dataclass

    def norm_int(x):
        if x is None:
            return None
        try:
            s = str(x).strip()
            if not s:
                return None
            return int(float(s.replace(",", ".")))
        except Exception:
            return None

    def norm_brand(x: str) -> str:
        s = (x or "").strip().upper()
        # keep only [A-Z0-9], collapse everything else
        s = _re.sub(r"[^A-Z0-9]+", "", s)
        return s

    def norm_season(x: str) -> str:
        s = (x or "").strip().lower()
        if s in {"лето", "летняя", "summer"}:
            return "SUMMER"
        if s in {"зима", "зимняя", "winter"}:
            return "WINTER"
        if s in {"всесезон", "всесезонная", "allseason", "all season", "all-season"}:
            return "ALLSEASON"
        return (x or "").strip().upper()

    @dataclass(frozen=True)
    class _Skus:
        base_sku: str
        internal_sku: str

    def build_skus(*, brand, width, height, diameter, season,
                  load_index=None, speed_index=None, runflat=False, studded=False,
                  use_variations=True):
        if not brand or not width or not height or not diameter or not season:
            raise ValueError("missing required fields for base_sku")
        base = f"{brand}-{width}-{height}-{diameter}-{season}"
        cand = base
        if use_variations:
            parts = []
            if load_index and speed_index:
                parts.append(f"{load_index}{speed_index}")
            elif load_index:
                parts.append(f"{load_index}")
            elif speed_index:
                parts.append(f"{speed_index}")
            if studded:
                parts.append("STUDDED")
            if runflat:
                parts.append("RUNFLAT")
            if parts:
                cand = base + "-" + "-".join(parts)
        return _Skus(base_sku=base, internal_sku=cand)


ALLOWED_CURRENCIES = {"RUB", "USD", "EUR"}
_WS_RE = re.compile(r"\s+")
_SUPPLIER_ID_RE = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def norm_opt_text(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None


def norm_model(x: Any) -> Optional[str]:
    s = norm_opt_text(x)
    if not s:
        return None
    s = _WS_RE.sub(" ", s)
    return s.upper()


def norm_upper_opt(x: Any) -> Optional[str]:
    s = norm_opt_text(x)
    return s.upper() if s else None


def to_bool_opt(x: Any) -> Optional[bool]:
    """
    None -> None
    bool -> as-is
    numbers -> bool(x)  (0.0 False, 0.1 True)
    strings -> explicit tokens, else True (RF/RUNFLAT etc.)
    """
    if x is None:
        return None
    if isinstance(x, bool):
        return x
    if isinstance(x, (int, float)):
        return bool(x)
    if isinstance(x, str):
        s = x.strip().lower()
        if s in {"", "0", "false", "no", "n", "нет", "не", "off"}:
            return False
        if s in {"1", "true", "yes", "y", "да", "on"}:
            return True
        return True
    return bool(x)


def norm_currency(x: Any, ln: int, warn_fn: Callable[[str], None]) -> str:
    s = (str(x).strip().upper() if x is not None else "RUB")
    if not s:
        return "RUB"
    if s not in ALLOWED_CURRENCIES:
        warn_fn(f"WARN line {ln}: currency={s} not allowed -> fallback RUB")
        return "RUB"
    return s


def to_int_opt(x: Any) -> Optional[int]:
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    try:
        return int(float(s.replace(",", ".")))
    except Exception:
        return None


def to_float_opt(x: Any) -> Optional[float]:
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    try:
        return float(s.replace(",", "."))
    except Exception:
        return None


def norm_updated_at(x: Any, ln: int, warn_fn: Callable[[str], None]) -> str:
    """
    Ensure updated_at is a valid ISO8601 string accepted by Postgres timestamptz.
    If invalid -> fallback to now() (UTC).
    """
    if x is None:
        return utc_now_iso()
    s = str(x).strip()
    if not s:
        return utc_now_iso()
    if s.endswith("Z"):
        s2 = s[:-1] + "+00:00"
    else:
        s2 = s
    try:
        datetime.fromisoformat(s2)
        return s2
    except Exception:
        warn_fn(f"WARN line {ln}: invalid updated_at={s!r} -> fallback now()")
        return utc_now_iso()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Apply normalized offers NDJSON into Postgres serving tables (MVP).")
    p.add_argument("--input_ndjson", required=True, help="Path to NDJSON with normalized offers")
    p.add_argument("--db_url", required=False, help="PostgreSQL URL (fallback: env DB_URL)")
    p.add_argument("--supplier_id", required=True, help="Supplier id for this update batch")
    p.add_argument("--dry-run", action="store_true", help="Print actions only, do not write to DB")
    p.add_argument("--commit-every", type=int, default=500, help="Commit every N rows (default 500)")
    p.add_argument("--quiet", action="store_true", help="Suppress WARN/progress logs (still prints final stats)")
    p.add_argument("--lock-timeout", default="5min", help="Postgres lock_timeout for advisory lock (e.g. 5min, 30s)")
    p.add_argument("--progress-every", type=int, default=0, help="Progress log every N rows (0=auto)")
    p.add_argument("--debug", action="store_true", help="Enable extra internal checks (slower)")
    p.add_argument("--vacuum", action="store_true", help="VACUUM ANALYZE tables at end (use for big runs)")
    p.add_argument("--log-file", default="", help="Append logs to this file (optional)")
    p.add_argument("--retry", type=int, default=2, help="Retry batch on transient DB errors (default 2)")
    p.add_argument("--retry-base-sleep", type=float, default=0.25, help="Base sleep seconds for backoff (default 0.25)")
    return p.parse_args()


def setup_logger(quiet: bool, log_file: str) -> logging.Logger:
    logger = logging.getLogger("apply_to_postgres_v1")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = False

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    if not quiet:
        sh = logging.StreamHandler(stream=sys.stderr)
        sh.setLevel(logging.INFO)
        sh.setFormatter(fmt)
        logger.addHandler(sh)

    if log_file:
        fh = logging.FileHandler(log_file, mode="a", encoding="utf-8")
        fh.setLevel(logging.INFO)
        fh.setFormatter(fmt)
        logger.addHandler(fh)

    return logger


def is_transient_db_error(exc: BaseException) -> bool:
    orig = getattr(exc, "orig", None)
    pgcode = getattr(orig, "pgcode", None)
    if pgcode in {"40P01", "40001", "55P03", "57014", "53300"}:
        return True
    if isinstance(exc, OperationalError):
        return True
    return False


LOCK_TIMEOUT_SQL = None  # patched: SET lock_timeout cannot use bind params
LOCK_SQL = text("SELECT pg_advisory_lock(hashtext(:k))")
UNLOCK_SQL = text("SELECT pg_advisory_unlock(hashtext(:k))")

MASTER_BASE_INSERT = text(
    """
    INSERT INTO master_products (
      internal_sku, base_sku, brand, model, width, height, diameter, season,
      load_index, speed_index, runflat, studded,
      source_supplier_id, first_seen_at, last_updated_at, is_manual
    ) VALUES (
      :internal_sku, :base_sku, :brand, :model, :width, :height, :diameter, :season,
      NULL, NULL, NULL, NULL,
      :source_supplier_id, now(), now(), false
    )
    ON CONFLICT (internal_sku) DO NOTHING
    """
)

MASTER_CANDIDATE_UPSERT = text(
    """
    INSERT INTO master_products (
      internal_sku, base_sku, brand, model, width, height, diameter, season,
      load_index, speed_index, runflat, studded,
      source_supplier_id, first_seen_at, last_updated_at, is_manual
    ) VALUES (
      :internal_sku, :base_sku, :brand, :model, :width, :height, :diameter, :season,
      :load_index, :speed_index, :runflat, :studded,
      :source_supplier_id, now(), now(), false
    )
    ON CONFLICT (internal_sku) DO UPDATE SET
      base_sku        = EXCLUDED.base_sku,
      brand           = EXCLUDED.brand,
      model           = EXCLUDED.model,
      width           = EXCLUDED.width,
      height          = EXCLUDED.height,
      diameter        = EXCLUDED.diameter,
      season          = EXCLUDED.season,
      load_index      = EXCLUDED.load_index,
      speed_index     = EXCLUDED.speed_index,
      runflat         = EXCLUDED.runflat,
      studded         = EXCLUDED.studded,
      last_updated_at = now()
    WHERE
      master_products.base_sku    IS DISTINCT FROM EXCLUDED.base_sku OR
      master_products.brand       IS DISTINCT FROM EXCLUDED.brand OR
      master_products.model       IS DISTINCT FROM EXCLUDED.model OR
      master_products.width       IS DISTINCT FROM EXCLUDED.width OR
      master_products.height      IS DISTINCT FROM EXCLUDED.height OR
      master_products.diameter    IS DISTINCT FROM EXCLUDED.diameter OR
      master_products.season      IS DISTINCT FROM EXCLUDED.season OR
      master_products.load_index  IS DISTINCT FROM EXCLUDED.load_index OR
      master_products.speed_index IS DISTINCT FROM EXCLUDED.speed_index OR
      master_products.runflat     IS DISTINCT FROM EXCLUDED.runflat OR
      master_products.studded     IS DISTINCT FROM EXCLUDED.studded
    """
)

MAP_UPSERT = text(
    """
    INSERT INTO supplier_sku_map (supplier_id, supplier_sku, internal_sku, first_seen_at)
    VALUES (:supplier_id, :supplier_sku, :internal_sku, now())
    ON CONFLICT (supplier_id, supplier_sku) DO UPDATE SET
      internal_sku = EXCLUDED.internal_sku
    WHERE supplier_sku_map.internal_sku IS DISTINCT FROM EXCLUDED.internal_sku
    """
)

OFFER_UPSERT = text(
    """
    INSERT INTO supplier_offers_latest (
      supplier_id, supplier_sku, internal_sku,
      qty, price_purchase, currency, updated_at,
      last_applied_at
    ) VALUES (
      :supplier_id, :supplier_sku, :internal_sku,
      :qty, :price_purchase, :currency, :updated_at,
      now()
    )
    ON CONFLICT (supplier_id, supplier_sku) DO UPDATE SET
      internal_sku    = EXCLUDED.internal_sku,
      qty             = COALESCE(EXCLUDED.qty, supplier_offers_latest.qty),
      price_purchase  = COALESCE(EXCLUDED.price_purchase, supplier_offers_latest.price_purchase),
      currency        = COALESCE(EXCLUDED.currency, supplier_offers_latest.currency),
      updated_at      = COALESCE(EXCLUDED.updated_at, supplier_offers_latest.updated_at),
      last_applied_at = now()
    WHERE
      (
        supplier_offers_latest.updated_at IS NULL OR
        EXCLUDED.updated_at IS NULL OR
        EXCLUDED.updated_at >= supplier_offers_latest.updated_at
      )
      AND (
        supplier_offers_latest.internal_sku   IS DISTINCT FROM EXCLUDED.internal_sku OR
        supplier_offers_latest.qty            IS DISTINCT FROM COALESCE(EXCLUDED.qty, supplier_offers_latest.qty) OR
        supplier_offers_latest.price_purchase IS DISTINCT FROM COALESCE(EXCLUDED.price_purchase, supplier_offers_latest.price_purchase) OR
        supplier_offers_latest.currency       IS DISTINCT FROM COALESCE(EXCLUDED.currency, supplier_offers_latest.currency) OR
        supplier_offers_latest.updated_at     IS DISTINCT FROM COALESCE(EXCLUDED.updated_at, supplier_offers_latest.updated_at)
      )
    """
)


def main() -> int:
    print("Starting apply_to_postgres_v1 ...")
    sys.stdout.flush()

    args = parse_args()
    logger = setup_logger(args.quiet, args.log_file)

    supplier_id = args.supplier_id.strip()
    if not supplier_id:
        logger.error("ERROR: --supplier_id is empty")
        return 2
    if not _SUPPLIER_ID_RE.match(supplier_id):
        logger.error("ERROR: --supplier_id has invalid chars (allowed: A-Za-z0-9_.- , len<=64)")
        return 2

    def warn(msg: str) -> None:
        logger.warning(msg)

    def prog(msg: str) -> None:
        logger.info(msg)

    db_url = args.db_url or os.environ.get("DB_URL")
    if not db_url:
        logger.error("ERROR: --db_url not provided and env DB_URL is empty")
        return 2

    in_path = args.input_ndjson
    if not os.path.isfile(in_path):
        logger.error("ERROR: input file not found: %s", in_path)
        return 2

    commit_every = max(1, int(args.commit_every))

    if args.progress_every and int(args.progress_every) > 0:
        progress_every = int(args.progress_every)
    else:
        progress_every = min(1000, commit_every * 2)
        progress_every = max(100, progress_every)

    retry_n = max(0, int(args.retry))
    base_sleep = max(0.0, float(args.retry_base_sleep))

    stats = {
        "processed": 0,
        "skipped": 0,
        "errors": 0,
        "dup_supplier_sku": 0,
        "batches_committed": 0,
        "master_base_inserted": 0,
        "master_candidate_affected": 0,
        "map_affected": 0,
        "offer_affected": 0,
        "integrity_errors": 0,
        "integrity_error_rows": 0,
        "retries": 0,
        "retry_batches": 0,
    }

    engine = None
    conn = None

    seen_supplier_sku: Dict[str, str] = {}

    base_rows: List[Dict[str, Any]] = []
    cand_rows: List[Dict[str, Any]] = []
    map_rows: List[Dict[str, Any]] = []
    offer_rows: List[Dict[str, Any]] = []

    def flush_batch() -> None:
        nonlocal conn
        if args.dry_run:
            base_rows.clear()
            cand_rows.clear()
            map_rows.clear()
            offer_rows.clear()
            return
        if conn is None:
            raise RuntimeError("no database connection (conn is None)")
        if not (base_rows or cand_rows or map_rows or offer_rows):
            return

        batch_rows = len(offer_rows)

        attempt = 0
        while True:
            tx = conn.begin()
            try:
                if base_rows:
                    rc = conn.execute(MASTER_BASE_INSERT, base_rows).rowcount or 0
                    stats["master_base_inserted"] += int(rc)
                if cand_rows:
                    rc = conn.execute(MASTER_CANDIDATE_UPSERT, cand_rows).rowcount or 0
                    stats["master_candidate_affected"] += int(rc)
                if map_rows:
                    rc = conn.execute(MAP_UPSERT, map_rows).rowcount or 0
                    stats["map_affected"] += int(rc)
                if offer_rows:
                    rc = conn.execute(OFFER_UPSERT, offer_rows).rowcount or 0
                    stats["offer_affected"] += int(rc)

                tx.commit()
                stats["batches_committed"] += 1
                return

            except IntegrityError as ie:
                stats["errors"] += 1
                stats["integrity_errors"] += 1
                stats["integrity_error_rows"] += batch_rows
                try:
                    tx.rollback()
                except Exception:
                    pass
                warn(f"WARN: batch IntegrityError -> rollback batch (rows={batch_rows}) ({ie})")
                return

            except KeyboardInterrupt:
                stats["errors"] += 1
                try:
                    tx.rollback()
                except Exception:
                    pass
                warn("WARN: KeyboardInterrupt during batch -> rollback batch")
                raise

            except (OperationalError, DBAPIError) as e:
                try:
                    tx.rollback()
                except Exception:
                    pass

                transient = is_transient_db_error(e)
                if transient and attempt < retry_n:
                    attempt += 1
                    stats["retries"] += 1
                    if attempt == 1:
                        stats["retry_batches"] += 1
                    sleep_s = base_sleep * (2 ** (attempt - 1))
                    warn(f"WARN: transient DB error -> retry {attempt}/{retry_n} after {sleep_s:.2f}s (rows={batch_rows}) ({e})")
                    time.sleep(sleep_s)
                    continue

                stats["errors"] += 1
                warn(f"WARN: batch db error -> rollback batch (rows={batch_rows}) ({e})")
                return

            except Exception as e:
                stats["errors"] += 1
                try:
                    tx.rollback()
                except Exception:
                    pass
                warn(f"WARN: batch db error -> rollback batch (rows={batch_rows}) ({e})")
                return

            finally:
                base_rows.clear()
                cand_rows.clear()
                map_rows.clear()
                offer_rows.clear()

    try:
        if not args.dry_run:
            engine = create_engine(db_url, future=True, pool_pre_ping=True)
            conn = engine.connect()

            try:
                timeout_val = str(args.lock_timeout)
                # PostgreSQL does not accept bind params in SET; use literal after strict validation
                if not re.match(r"^\d+(ms|s|min|h)$", timeout_val):
                    raise ValueError(f"invalid --lock-timeout value '{timeout_val}'")
                conn.execute(text(f"SET lock_timeout = '{timeout_val}'"))
                conn.commit()  # end autobegin txn after SET lock_timeout
            except Exception as e:
                logger.error("ERROR: invalid --lock-timeout value %r: %s", args.lock_timeout, e)
                return 2

            conn.execute(LOCK_SQL, {"k": supplier_id})
            conn.commit()  # end autobegin txn after advisory lock

        with open(in_path, "r", encoding="utf-8") as f:
            for ln, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue

                try:
                    obj = json.loads(line)
                except Exception as e:
                    stats["errors"] += 1
                    warn(f"WARN line {ln}: json parse failed: {e}")
                    continue

                stats["processed"] += 1
                if stats["processed"] % progress_every == 0:
                    prog(f"PROGRESS: processed={stats['processed']} skipped={stats['skipped']} errors={stats['errors']}")

                supplier_sku = (obj.get("supplier_sku") or obj.get("sku") or "").strip()
                if not supplier_sku:
                    stats["skipped"] += 1
                    warn(f"WARN line {ln}: missing supplier_sku -> skip")
                    continue

                brand_raw = obj.get("brand")
                season_raw = obj.get("season")
                width = obj.get("width")
                height = obj.get("height")
                diameter = obj.get("diameter")

                load_index_raw = obj.get("load_index")
                speed_index_raw = obj.get("speed_index")
                runflat_raw = obj.get("runflat")
                studded_raw = obj.get("studded")
                model_raw = obj.get("model")

                qty_raw = obj.get("qty")
                price_raw = obj.get("price_purchase")

                currency = norm_currency(obj.get("currency"), ln, warn)
                updated_at = norm_updated_at(obj.get("updated_at"), ln, warn)

                brand_n = norm_brand(str(brand_raw)) if brand_raw is not None else ""
                season_n = norm_season(str(season_raw)) if season_raw is not None else ""
                w = norm_int(width)
                h = norm_int(height)
                d = norm_int(diameter)

                runflat_b = to_bool_opt(runflat_raw)
                studded_b = to_bool_opt(studded_raw)

                load_index_n = norm_upper_opt(load_index_raw)
                speed_index_n = norm_upper_opt(speed_index_raw)

                try:
                    skus = build_skus(
                        brand=brand_n,
                        width=w,
                        height=h,
                        diameter=d,
                        season=season_n,
                        load_index=load_index_n,
                        speed_index=speed_index_n,
                        runflat=bool(runflat_b) if runflat_b is not None else False,
                        studded=bool(studded_b) if studded_b is not None else False,
                        use_variations=True,
                    )
                except Exception as e:
                    stats["skipped"] += 1
                    warn(f"WARN line {ln}: missing/invalid fields for base_sku -> skip ({e})")
                    continue

                base_sku = skus.base_sku
                internal_candidate = skus.internal_sku

                prev = seen_supplier_sku.get(supplier_sku)
                if prev is not None and prev != internal_candidate:
                    stats["dup_supplier_sku"] += 1
                    warn(
                        f"WARN line {ln}: duplicate supplier_sku={supplier_sku} within file: "
                        f"prev_internal={prev} new_internal={internal_candidate} -> last-wins"
                    )
                seen_supplier_sku[supplier_sku] = internal_candidate

                qty = to_int_opt(qty_raw)
                price_purchase = to_float_opt(price_raw)

                is_variation = (internal_candidate != base_sku)

                base_param = {
                    "internal_sku": base_sku,
                    "base_sku": base_sku,
                    "brand": brand_n,
                    "model": norm_model(model_raw),
                    "width": w,
                    "height": h,
                    "diameter": d,
                    "season": season_n,
                    "source_supplier_id": supplier_id,
                }

                cand_param = {
                    "internal_sku": internal_candidate,
                    "base_sku": base_sku,
                    "brand": brand_n,
                    "model": norm_model(model_raw),
                    "width": w,
                    "height": h,
                    "diameter": d,
                    "season": season_n,
                    "load_index": (load_index_n if is_variation else None),
                    "speed_index": (speed_index_n if is_variation else None),
                    "runflat": (runflat_b if is_variation else None),
                    "studded": (studded_b if is_variation else None),
                    "source_supplier_id": supplier_id,
                }

                map_param = {
                    "supplier_id": supplier_id,
                    "supplier_sku": supplier_sku,
                    "internal_sku": internal_candidate,
                }

                offer_param = {
                    "supplier_id": supplier_id,
                    "supplier_sku": supplier_sku,
                    "internal_sku": internal_candidate,
                    "qty": qty,
                    "price_purchase": price_purchase,
                    "currency": currency,
                    "updated_at": updated_at,
                }

                if args.dry_run:
                    if not args.quiet:
                        print(
                            f"DRYRUN line {ln}: supplier={supplier_id} supplier_sku={supplier_sku} "
                            f"base_sku={base_sku} chosen_internal={internal_candidate} "
                            f"qty={qty} price_purchase={price_purchase} currency={currency} updated_at={updated_at}"
                        )
                    continue

                base_rows.append(base_param)
                cand_rows.append(cand_param)
                map_rows.append(map_param)
                offer_rows.append(offer_param)

                if len(offer_rows) >= commit_every:
                    if args.debug:
                        if not (len(base_rows) == len(cand_rows) == len(map_rows) == len(offer_rows)):
                            raise RuntimeError(
                                f"buffer desync: base={len(base_rows)} cand={len(cand_rows)} map={len(map_rows)} offer={len(offer_rows)}"
                            )
                    else:
                        if not (len(base_rows) == len(offer_rows) and len(cand_rows) == len(offer_rows) and len(map_rows) == len(offer_rows)):
                            raise RuntimeError(
                                f"buffer desync: base={len(base_rows)} cand={len(cand_rows)} map={len(map_rows)} offer={len(offer_rows)}"
                            )
                    flush_batch()

        flush_batch()

        if args.vacuum and (not args.dry_run) and conn is not None and stats["processed"] >= 5000:
            try:
                conn.execute(text("VACUUM ANALYZE master_products"))
                conn.execute(text("VACUUM ANALYZE supplier_offers_latest"))
            except Exception as e:
                warn(f"WARN: VACUUM failed: {e}")

    except KeyboardInterrupt:
        warn("WARN: KeyboardInterrupt -> exiting cleanly")
        return 130

    except Exception as e:
        stats["errors"] += 1
        logger.error("ERROR: %s", e)
        return 1

    finally:
        try:
            if conn is not None:
                try:
                    conn.execute(UNLOCK_SQL, {"k": supplier_id})
                except Exception:
                    pass
        except Exception:
            pass
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass
        try:
            if engine is not None:
                engine.dispose()
        except Exception:
            pass

    print(json.dumps(stats, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
