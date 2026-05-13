#!/usr/bin/env python3
"""
4tochki ETL v2.4 — фикс логирования (без extra в формате).
"""

import sys, os, json, time, logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

import psycopg2
import psycopg2.extras
from zeep import Client, Settings
from zeep.transports import Transport
from zeep.helpers import serialize_object
from tenacity import retry, stop_after_attempt, wait_exponential

DSN = os.getenv("DB_CONN", "dbname=canonical user=canonical")
CHUNK_SIZE = int(os.getenv("FTO_CHUNK_SIZE", "50"))
PAGE_SIZE = 50
REQUEST_TIMEOUT = int(os.getenv("FTO_TIMEOUT", "90"))
BATCH_SIZE = 1000

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

logging.basicConfig(
    level=logging.INFO,
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "message": "%(message)s"}',
    handlers=[logging.FileHandler('/var/log/4tochki_etl.log'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

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
    settings = Settings(strict=True, xml_huge_tree=False)
    return Client(
        "https://api-b2b.4tochki.ru/WCF/ClientService.svc?wsdl",
        transport=Transport(timeout=REQUEST_TIMEOUT),
        settings=settings
    )

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

def fetch_catalog(client, login, password) -> Dict[str, dict]:
    catalog = {}
    for method_name, cfg in CATALOG_METHODS.items():
        product_type = cfg["type"]
        for filt in cfg["filters"]:
            page = 0
            while True:
                try:
                    result = fetch_page(client, method_name, login, password, filt, page, PAGE_SIZE)
                    data = serialize_object(result)
                except Exception as e:
                    _err(f"{method_name} page={page}: {e}", method=method_name, page=page)
                    break

                container = data
                for key in cfg["container"]:
                    if isinstance(container, dict):
                        container = container.get(key, {})
                    else:
                        container = {}
                items = container if isinstance(container, list) else []

                for item in items:
                    if not isinstance(item, dict):
                        continue
                    code = item.get("code")
                    if code and code not in catalog:
                        catalog[code] = {
                            "type": product_type,
                            "brand": item.get("marka") or item.get("brand", ""),
                            "model": item.get("model", ""),
                            "name": item.get("name", ""),
                            "season": item.get("season"),
                            "raw_json": json.dumps(item, ensure_ascii=False, default=str)
                        }

                total_pages = data.get("totalPages", 1) if isinstance(data, dict) else 1
                page += 1
                if page >= total_pages:
                    break
                time.sleep(0.3)

        count = sum(1 for v in catalog.values() if v["type"] == product_type)
        _log(f"{method_name}: {count} {product_type}s", method=method_name, count=count)
    return catalog

def fetch_stock(client, login, password, codes: List[str]) -> List[tuple]:
    chunks = [codes[i:i + CHUNK_SIZE] for i in range(0, len(codes), CHUNK_SIZE)]
    offers = []
    failed_chunks = []

    for idx, chunk in enumerate(chunks):
        chunk_offers = _process_chunk(client, login, password, chunk)
        if chunk_offers is None:
            failed_chunks.append(idx)
        else:
            offers.extend(chunk_offers)

        if (idx + 1) % 50 == 0 or idx == 0 or idx == len(chunks) - 1:
            _log(f"Chunk {idx+1}/{len(chunks)} OK", chunk=idx+1, total=len(chunks))

        time.sleep(0.3)

    if failed_chunks:
        _warn(f"Retrying {len(failed_chunks)} failed chunks", failed_count=len(failed_chunks))
        time.sleep(5)
        for idx in failed_chunks:
            _log(f"Retry chunk {idx+1}/{len(chunks)}", chunk=idx+1, total=len(chunks))
            chunk_offers = _process_chunk(client, login, password, chunks[idx])
            if chunk_offers is None:
                _err(f"Chunk {idx+1}/{len(chunks)} FAILED after retry", chunk=idx+1)
            else:
                offers.extend(chunk_offers)
                _log(f"Chunk {idx+1}/{len(chunks)} RECOVERED", chunk=idx+1)
            time.sleep(1)

    return offers

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
    for item in items:
        if not isinstance(item, dict):
            continue
        code = item.get("code")
        if not code:
            continue
        for wh in item.get("whpr", {}).get("wh_price_rest", []):
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
                now_utc
            ))

    return chunk_offers

def update_products(cur, catalog: Dict[str, dict]):
    if not catalog:
        return
    rows = [(code, v["type"], v["brand"], v["model"], v["name"], v["season"], v["raw_json"])
            for code, v in catalog.items()]
    psycopg2.extras.execute_values(cur, """
        INSERT INTO _4tochki_products (code, type, brand, model, name, season, raw_json)
        VALUES %s
        ON CONFLICT (code) DO UPDATE SET
            type = EXCLUDED.type, brand = EXCLUDED.brand,
            model = EXCLUDED.model, name = EXCLUDED.name,
            season = EXCLUDED.season, raw_json = EXCLUDED.raw_json,
            updated_at = NOW()
    """, rows, page_size=BATCH_SIZE)
    _log(f"Products updated", count=len(rows))

def update_offers(cur, offers: List[tuple]):
    if not offers:
        return
    psycopg2.extras.execute_values(cur, """
        INSERT INTO _4tochki_offers
        (product_code, warehouse_code, price, price_rozn, rest, whpr_json, updated_at)
        VALUES %s
        ON CONFLICT (product_code, warehouse_code) DO UPDATE SET
            price = EXCLUDED.price, price_rozn = EXCLUDED.price_rozn,
            rest = EXCLUDED.rest, whpr_json = EXCLUDED.whpr_json,
            updated_at = EXCLUDED.updated_at
    """, offers, page_size=BATCH_SIZE)
    _log(f"Offers updated", count=len(offers))

def main():
    if len(sys.argv) < 2 or sys.argv[1] not in ("stock", "full"):
        _err("Usage: python3 4tochki_etl.py stock|full")
        sys.exit(1)

    mode = sys.argv[1]
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

    try:
        client = get_client()

        if mode == "full":
            _log("[1] Fetching catalog")
            catalog = fetch_catalog(client, login, password)
            _log(f"Catalog", count=len(catalog))

            _log("[2] Updating products")
            update_products(cur, catalog)
            conn.commit()

            _log("[3] Fetching stock")
            all_codes = list(catalog.keys())
        else:
            _log("[1] Getting codes from DB")
            cur.execute("SELECT code FROM _4tochki_products ORDER BY code")
            all_codes = [row[0] for row in cur.fetchall()]
            _log(f"Codes", count=len(all_codes))

            _log("[2] Fetching stock")

        offers = fetch_stock(client, login, password, all_codes)
        _log(f"Offers collected", count=len(offers))

        _log("[3] Updating offers")
        update_offers(cur, offers)
        conn.commit()

        if mode == "full":
            _log("[4] VACUUM ANALYZE")
            cur.execute("VACUUM ANALYZE _4tochki_products")
            cur.execute("VACUUM ANALYZE _4tochki_offers")
            conn.commit()
            _log("VACUUM done")

    except Exception as e:
        conn.rollback()
        _err(f"FATAL: {e}", error=str(e))
        sys.exit(1)
    finally:
        cur.close()
        conn.close()

    elapsed = time.time() - t0
    _log(f"DONE", mode=mode, elapsed=int(elapsed))

if __name__ == "__main__":
    main()
