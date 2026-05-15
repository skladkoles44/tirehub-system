#!/usr/bin/env python3
"""
4tochki ETL v3.2 — parallel catalog, WSDL cache persistent.
"""

import sys, os, json, time, logging, signal, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras
from zeep import Client, Settings
from zeep.transports import Transport
from zeep.helpers import serialize_object
from zeep.cache import SqliteCache
from requests import Session
from tenacity import retry, stop_after_attempt, wait_exponential

DSN = os.getenv("DB_CONN", "dbname=canonical user=canonical")
CHUNK_SIZE = int(os.getenv("FTO_CHUNK_SIZE", "150"))
PAGE_SIZE = 50
REQUEST_TIMEOUT = int(os.getenv("FTO_TIMEOUT", "90"))
BATCH_SIZE = 1000

_shutdown = False
logger = None

CATALOG_METHODS = {
    "GetFindTyre": {
        "filters": [{"season_list": {"string": [s]}} for s in ["w", "s", "a"]],
        "container": ["price_rest_list", "TyrePriceRest"],
        "type": "tyre"
    },
    "GetFindDisk": {
        "filters": [{}],
        "container": ["price_rest_list", "DiskPriceRest"],
        "type": "disk"
    },
    "GetFindCamera": {
        "filters": [{}],
        "container": ["ResultItems", "GetFindCameraContainer"],
        "type": "camera"
    },
    "GetFindWheel": {
        "filters": [{}],
        "container": ["price_rest_list", "Wheel"],
        "type": "wheel"
    },
    "GetFastener": {
        "filters": [{}],
        "container": ["items", "GetFastenerContainer"],
        "type": "fastener"
    },
    "GetOil": {
        "filters": [{}],
        "container": ["price_rest_list", "GetOilResultItem"],
        "type": "oil"
    },
}

def setup_logging(mode: str):
    log_path = f"/var/log/4tochki_etl_{mode}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="""{"time": "%(asctime)s", "level": "%(levelname)s", "message": "%(message)s"}""",
        handlers=[logging.FileHandler(log_path), logging.StreamHandler()]
    )
    return logging.getLogger(__name__)

def _log(msg, **extra):
    if extra:
        logger.info(f"{msg} | {json.dumps(extra, default=str)}")
    else:
        logger.info(msg)

def _warn(msg, **extra):
    if extra:
        logger.warning(f"{msg} | {json.dumps(extra, default=str)}")
    else:
        logger.warning(msg)

def _err(msg, **extra):
    if extra:
        logger.error(f"{msg} | {json.dumps(extra, default=str)}")
    else:
        logger.error(msg)

def safe_float(v, default=0.0):
    try: return float(v)
    except: return default

def get_client():
    session = Session()
    session.headers.update({"Connection": "keep-alive"})
    settings = Settings(strict=True, xml_huge_tree=False)
    os.makedirs("/var/cache", exist_ok=True)
    return Client(
        "https://api-b2b.4tochki.ru/WCF/ClientService.svc?wsdl",
        transport=Transport(
            session=session,
            timeout=REQUEST_TIMEOUT,
            cache=SqliteCache(path="/var/cache/4tochki_wsdl.db", timeout=3600)
        ),
        settings=settings
    )

def _load_env(path: str = "/opt/canonical-core/.env.4tochki"):
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip())

def _handle_sigterm(signum, frame):
    global _shutdown
    _shutdown = True
    if logger:
        _warn("SIGTERM received, finishing current chunk...")

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1.5, min=2, max=20))
def fetch_page(client, method, login, password, filt, page, page_size):
    return client.service[method](login, password, filt, page, page_size)

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1.5, min=2, max=20))
def fetch_stock_chunk(client, login, password, chunk):
    filter_dict = {
        "code_list": {"string": chunk},
        "wrh_list": {"int": []},
        "include_paid_delivery": False,
        "user_address_id": 64302,
        "searchCodeByOccurence": False
    }
    return client.service.GetGoodsPriceRestByCode(login, password, filter_dict)

def _fetch_method(client, login, password, method_name, cfg):
    items = []
    product_type = cfg["type"]
    for filt in cfg["filters"]:
        if _shutdown:
            break
        page = 0
        while True:
            if _shutdown:
                break
            try:
                result = fetch_page(client, method_name, login, password, filt, page, PAGE_SIZE)
                data = serialize_object(result)
            except Exception as e:
                _err(f"{method_name} page={page}: {e}")
                break

            container = data
            for key in cfg["container"]:
                container = container.get(key, {}) if isinstance(container, dict) else {}
            page_items = container if isinstance(container, list) else []

            for item in page_items:
                if isinstance(item, dict) and item.get("code"):
                    items.append({
                        "code": item["code"],
                        "type": product_type,
                        "brand": item.get("marka") or item.get("brand", ""),
                        "model": item.get("model", ""),
                        "name": item.get("name", ""),
                        "season": item.get("season"),
                        "raw_json": json.dumps(item, ensure_ascii=False, default=str)
                    })

            total_pages = data.get("totalPages", 1) if isinstance(data, dict) else 1
            if total_pages <= 0 or page >= total_pages:
                break
            page += 1
            time.sleep(0.2)
    return items

def fetch_catalog(client, login, password) -> Dict[str, dict]:
    catalog = {}
    lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(_fetch_method, client, login, password, method_name, cfg): method_name
            for method_name, cfg in CATALOG_METHODS.items()
        }

        for future in as_completed(futures):
            method_name = futures[future]
            try:
                items = future.result()
                with lock:
                    for item in items:
                        code = item["code"]
                        if code not in catalog:
                            catalog[code] = item
                _log(f"{method_name} completed", count=len(items))
            except Exception as e:
                _err(f"{method_name} FAILED: {e}")

    return catalog

def fetch_stock(client, login, password, codes: List[str]) -> Tuple[List[tuple], List[int]]:
    global _shutdown
    chunks = [codes[i:i + CHUNK_SIZE] for i in range(0, len(codes), CHUNK_SIZE)]
    offers = []
    failed_chunks = []

    for idx, chunk in enumerate(chunks):
        if _shutdown:
            _log(f"Shutdown after chunk {idx+1}/{len(chunks)}", chunk=idx+1, total=len(chunks))
            break

        chunk_offers = _process_chunk(client, login, password, chunk)
        if chunk_offers is None:
            failed_chunks.append(idx)
        else:
            offers.extend(chunk_offers)

        if (idx + 1) % 20 == 0 or idx == 0 or idx == len(chunks) - 1:
            _log(f"Chunk {idx+1}/{len(chunks)} OK", chunk=idx+1, total=len(chunks))

        time.sleep(0.3)

    if not _shutdown and failed_chunks:
        _warn(f"Retrying {len(failed_chunks)} failed chunks", failed_count=len(failed_chunks))
        time.sleep(5)
        for idx in failed_chunks:
            if _shutdown:
                break
            _log(f"Retry chunk {idx+1}/{len(chunks)}", chunk=idx+1, total=len(chunks))
            chunk_offers = _process_chunk(client, login, password, chunks[idx])
            if chunk_offers is None:
                _err(f"Chunk {idx+1}/{len(chunks)} FAILED after retry", chunk=idx+1)
            else:
                offers.extend(chunk_offers)
                _log(f"Chunk {idx+1}/{len(chunks)} RECOVERED", chunk=idx+1)
            time.sleep(1)

    return offers, failed_chunks

def _process_chunk(client, login, password, chunk) -> Optional[List[tuple]]:
    try:
        result = fetch_stock_chunk(client, login, password, chunk)
        data = serialize_object(result)
    except Exception as e:
        _err(f"Chunk FAIL: {e}")
        return None

    now_utc = datetime.now(timezone.utc).isoformat()
    chunk_offers = []

    items = data.get("price_rest_list", {}).get("price_rest", [])
    if not isinstance(items, list):
        items = []
    for item in items:
        if not isinstance(item, dict):
            continue
        code = item.get("code")
        if not code:
            continue
        whpr = item.get("whpr", {})
        if not isinstance(whpr, dict):
            continue
        for wh in whpr.get("wh_price_rest", []):
            if not isinstance(wh, dict):
                continue
            wrh = wh.get("wrh")
            if wrh is None:
                continue
            chunk_offers.append((
                code, int(wrh),
                safe_float(wh.get("price")),
                safe_float(wh.get("price_rozn")),
                int(wh.get("rest", 0)),
                json.dumps(wh, ensure_ascii=False, default=str),
                now_utc,
                True
            ))

    return chunk_offers

def update_products(cur, catalog: Dict[str, dict]):
    if not catalog:
        return
    now = datetime.now(timezone.utc).isoformat()
    rows = [(code, v["type"], v["brand"], v["model"], v["name"], v["season"],
             v["raw_json"], True, now)
            for code, v in catalog.items()]
    psycopg2.extras.execute_values(cur, """
        INSERT INTO _4tochki_products
            (code, type, brand, model, name, season, raw_json, is_active, updated_at)
        VALUES %s
        ON CONFLICT (code) DO UPDATE SET
            type = EXCLUDED.type,
            brand = EXCLUDED.brand,
            model = EXCLUDED.model,
            name = EXCLUDED.name,
            season = EXCLUDED.season,
            raw_json = EXCLUDED.raw_json,
            is_active = EXCLUDED.is_active,
            updated_at = EXCLUDED.updated_at
    """, rows, page_size=BATCH_SIZE)
    _log(f"Products updated", count=len(rows))

def update_offers(cur, offers: List[tuple]):
    if not offers:
        return
    psycopg2.extras.execute_values(cur, """
        INSERT INTO _4tochki_offers
        (product_code, warehouse_code, price, price_rozn, rest, whpr_json, updated_at, is_active)
        VALUES %s
        ON CONFLICT (product_code, warehouse_code) DO UPDATE SET
            price = EXCLUDED.price, price_rozn = EXCLUDED.price_rozn,
            rest = EXCLUDED.rest, whpr_json = EXCLUDED.whpr_json,
            updated_at = EXCLUDED.updated_at, is_active = EXCLUDED.is_active
    """, offers, page_size=BATCH_SIZE)
    _log(f"Offers updated", count=len(offers))

def main():
    global logger, _shutdown

    _load_env()

    if len(sys.argv) < 2 or sys.argv[1] not in ("stock", "full"):
        print("Usage: python3 4tochki_etl.py stock|full", file=sys.stderr)
        sys.exit(1)

    mode = sys.argv[1]
    logger = setup_logging(mode)

    signal.signal(signal.SIGTERM, _handle_sigterm)

    t0 = time.time()
    _log(f"START", mode=mode)

    login = os.environ.get("FTO_LOGIN")
    password = os.environ.get("FTO_PASSWORD")
    if not login or not password:
        _err("MISSING_CREDS")
        sys.exit(1)

    conn = psycopg2.connect(DSN)
    conn.autocommit = False
    cur = conn.cursor()
    conn_closed = False
    failed_chunks = []

    try:
        client = get_client()

        if mode == "full":
            _log("[0] Healthcheck")
            client.service.GetWarehouses(login, password)
            _log("Healthcheck OK")

            _log("[1] Fetching catalog (parallel)")
            catalog = fetch_catalog(client, login, password)
            _log(f"Catalog", count=len(catalog))

            if _shutdown:
                _log("Exited on SIGTERM after catalog")
                conn.rollback()
                return

            _log("[2] Updating products")
            update_products(cur, catalog)
            conn.commit()

            _log("[3] Fetching stock")
            all_codes = list(catalog.keys())
        else:
            _log("[1] Getting codes from DB")
            cur.execute("SELECT code FROM _4tochki_products WHERE is_active = true ORDER BY code")
            all_codes = []
            while True:
                rows = cur.fetchmany(5000)
                if not rows:
                    break
                all_codes.extend(row[0] for row in rows)
            _log(f"Codes", count=len(all_codes))

            _log("[2] Fetching stock")

        offers, failed_chunks = fetch_stock(client, login, password, all_codes)
        _log(f"Offers collected", count=len(offers))

        if not _shutdown:
            _log("[3] Updating offers")
            update_offers(cur, offers)

            _log("[4] Deactivating stale offers")
            cur.execute("""
                UPDATE _4tochki_offers
                SET is_active = false
                WHERE updated_at < NOW() - INTERVAL '3 hours'
                  AND is_active = true
            """)
            _log(f"Stale offers deactivated", deactivated=cur.rowcount)

            if mode == "full":
                _log("[4a] Deactivating removed products")
                cur.execute("""
                    UPDATE _4tochki_products
                    SET is_active = false
                    WHERE updated_at < NOW() - INTERVAL '3 hours'
                      AND is_active = true
                """)
                _log(f"Removed products deactivated", deactivated=cur.rowcount)

            conn.commit()

        if mode == "full" and not _shutdown:
            _log("[5] VACUUM ANALYZE")
            cur.close()
            conn.close()
            conn_closed = True
            vac_conn = psycopg2.connect(DSN)
            vac_conn.autocommit = True
            vac_cur = vac_conn.cursor()
            vac_cur.execute("VACUUM ANALYZE _4tochki_products")
            vac_cur.execute("VACUUM ANALYZE _4tochki_offers")
            vac_cur.close()
            vac_conn.close()
            _log("VACUUM done")

    except Exception as e:
        if not conn_closed:
            conn.rollback()
        _err(f"FATAL: {e}", error=str(e))
        sys.exit(1)
    finally:
        if not conn_closed:
            cur.close()
            conn.close()

    elapsed = time.time() - t0
    _log(f"DONE", mode=mode, elapsed=int(elapsed))

if __name__ == "__main__":
    main()
