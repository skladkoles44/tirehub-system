#!/usr/bin/env python3
"""
ETL Шинсервис v6.3 — production-ready (финальная стабильная версия)

Режимы:
  stock - обновление остатков (каждые 30 минут)
  full  - полное обновление каталога + цен + остатков + складов (раз в сутки)
"""

import sys
import json
import logging
import traceback
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, List, Tuple, Set
from uuid import uuid4

import psycopg2
import requests
from psycopg2.extras import execute_values
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# ====================== КОНФИГУРАЦИЯ ======================
SHINSERVICE_UUID_TYRES = os.getenv("SHINSERVICE_UUID_TYRES", "019dbb42-9e14-b7d0-a829-b64101ead29f")
SHINSERVICE_UUID_DISKS = os.getenv("SHINSERVICE_UUID_DISKS", "019dbb40-9828-be33-9728-e5d7db368ca6")
BASE_URL = "https://duplo-api.shinservice.ru/api/v1/exporter"
DB_CONN = os.getenv("DB_CONN", "dbname=canonical user=canonical host=/var/run/postgresql")

BATCH_SIZE = int(os.getenv("SHINSERVICE_BATCH_SIZE", "2000"))
CHUNK_SIZE = int(os.getenv("SHINSERVICE_CHUNK_SIZE", "10000"))
REQUEST_TIMEOUT = int(os.getenv("SHINSERVICE_TIMEOUT", "90"))
MAX_WORKERS = int(os.getenv("SHINSERVICE_MAX_WORKERS", "2"))
REQUEST_DELAY = float(os.getenv("SHINSERVICE_REQUEST_DELAY", "0.15"))
MAX_STOCK_RECORDS = int(os.getenv("SHINSERVICE_MAX_STOCK_RECORDS", "1000000"))

# ====================== DDL ДЛЯ ДОПОЛНИТЕЛЬНЫХ ТАБЛИЦ ======================
DDL_RUNS_TABLE = """
CREATE TABLE IF NOT EXISTS _shinservice_etl_runs (
    run_id TEXT PRIMARY KEY,
    mode TEXT,
    status TEXT,
    records_processed INT DEFAULT 0,
    records_failed INT DEFAULT 0,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP
)
"""

DDL_ERRORS_TABLE = """
CREATE TABLE IF NOT EXISTS _shinservice_etl_errors (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT,
    sku TEXT,
    error TEXT,
    raw_json JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""


def ensure_tables(conn):
    """Создаёт дополнительные таблицы при первом запуске"""
    with conn.cursor() as cur:
        cur.execute(DDL_RUNS_TABLE)
        cur.execute(DDL_ERRORS_TABLE)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_errors_run_id ON _shinservice_etl_errors(run_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_errors_created_at ON _shinservice_etl_errors(created_at)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_runs_mode ON _shinservice_etl_runs(mode)")
        conn.commit()


def update_run_status_start(conn, run_id: str, mode: str):
    """Регистрирует начало запуска"""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO _shinservice_etl_runs (run_id, mode, status, started_at)
            VALUES (%s, %s, 'start', NOW())
            ON CONFLICT (run_id) DO UPDATE SET
                mode = EXCLUDED.mode,
                status = 'start',
                started_at = NOW(),
                finished_at = NULL
        """, (run_id, mode))
        conn.commit()


def update_run_status_finish(conn, run_id: str, mode: str, status: str, 
                              records_processed: int = 0, records_failed: int = 0):
    """Записывает финальный статус запуска"""
    with conn.cursor() as cur:
        # run_id — PRIMARY KEY, mode не обязателен в WHERE
        cur.execute("""
            UPDATE _shinservice_etl_runs 
            SET status = %s,
                records_processed = %s,
                records_failed = %s,
                finished_at = NOW()
            WHERE run_id = %s
        """, (status, records_processed, records_failed, run_id))
        conn.commit()


def log_error(conn, run_id: str, sku: str, error_msg: str, raw_data: dict):
    """Сохраняет ошибку в dead-letter queue"""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO _shinservice_etl_errors (run_id, sku, error, raw_json)
            VALUES (%s, %s, %s, %s)
        """, (run_id, sku, error_msg[:1000], json.dumps(raw_data, ensure_ascii=False)))
        conn.commit()


# ====================== ПОТОКОБЕЗОПАСНЫЙ СЧЁТЧИК ======================
class SafeCounter:
    def __init__(self):
        self._value = 0
        self._lock = None
        try:
            import threading
            self._lock = threading.Lock()
        except ImportError:
            pass
    
    def increment(self) -> int:
        if self._lock:
            with self._lock:
                self._value += 1
                return self._value
        self._value += 1
        return self._value

_request_counter = SafeCounter()


# ====================== НАСТРОЙКА ЛОГИРОВАНИЯ ======================
class SafeJsonFormatter(logging.Formatter):
    def __init__(self, run_id: str):
        super().__init__()
        self.run_id = run_id
    
    def format(self, record):
        extra = getattr(record, "extra", None)
        if extra is None or not isinstance(extra, dict):
            extra = {}
        
        log_entry = {
            "time": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
            "funcName": record.funcName,
            "lineno": record.lineno,
            "run_id": self.run_id,
        }
        
        for key, value in extra.items():
            try:
                log_entry[key] = value
            except TypeError:
                log_entry[key] = str(value)
        
        if record.exc_info:
            log_entry["traceback"] = "".join(traceback.format_exception(*record.exc_info))
        
        return json.dumps(log_entry, ensure_ascii=False, default=str)


# Глобальный логгер (инициализируется в setup_logging)
_logger = None


def get_logger():
    return _logger


def setup_logging(run_id: str):
    global _logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    for h in root_logger.handlers[:]:
        root_logger.removeHandler(h)
    
    formatter = SafeJsonFormatter(run_id)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    file_handler = logging.FileHandler('/var/log/shinservice_etl.log')
    file_handler.setFormatter(formatter)
    
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)
    _logger = logging.getLogger(__name__)


# ====================== HTTP СЕССИЯ ======================
session = requests.Session()
session.headers.update({"User-Agent": "Canonical-Core-ETL/6.3 (Shinservice)"})


# ====================== FETCH ======================
@retry(stop=stop_after_attempt(4), wait=wait_exponential(multiplier=1.5, min=3, max=30),
       retry=retry_if_exception_type((requests.RequestException, ConnectionError)))
def fetch_data(uuid: str, export_type: str, request_name: str) -> Tuple[List[Dict], str]:
    start = time.perf_counter()
    url = f"{BASE_URL}/{uuid}/download?type={export_type}&format=json"
    req_num = _request_counter.increment()
    logger = get_logger()

    try:
        resp = session.get(url, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        
        items = []
        source = "unknown"
        
        if export_type == "stock":
            if isinstance(data, list):
                items = data
                source = "stock_array"
            elif isinstance(data, dict):
                if data.get("status") == "OK":
                    items = (data.get("data") or data).get("items", [])
                    source = "stock_wrapper"
                if not items:
                    nested = data.get("data") or data
                    items = nested.get("items") or nested.get("stock") or nested.get("stock_list") or []
                    source = "stock_nested"
        
        if not items and isinstance(data, dict):
            for key in ("tyre", "disk"):
                chunk = data.get(key) or []
                if isinstance(chunk, list):
                    items.extend(chunk)
                    source = key
            if not items and "items" in data:
                items = data.get("items") or []
                source = "items"
        
        duration = time.perf_counter() - start
        if logger:
            logger.info("Fetch completed", extra={
                "request_id": req_num, "request": request_name, "count": len(items),
                "duration": round(duration, 2), "source": source, "status": "success"
            })
        return items, source
    
    except Exception as e:
        duration = time.perf_counter() - start
        if logger:
            logger.error("Fetch failed", extra={
                "request_id": req_num, "request": request_name, "duration": round(duration, 2),
                "error": str(e), "status": "error"
            }, exc_info=True)
        raise


def submit_requests(executor, requests_config):
    logger = get_logger()
    futures = []
    for i, (uuid, export_type, request_name) in enumerate(requests_config):
        if i > 0:
            if logger:
                logger.info("Delaying request", extra={"delay": REQUEST_DELAY, "request": request_name})
            time.sleep(REQUEST_DELAY)
        futures.append(executor.submit(fetch_data, uuid, export_type, request_name))
    return futures


# ====================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ======================
def _extract_shop_ids_from_stock(items: List[Dict]) -> Set[int]:
    shop_ids = set()
    logger = get_logger()
    for item in items:
        store_id = item.get("store_id")
        if store_id is not None:
            try:
                shop_ids.add(int(store_id))
            except (ValueError, TypeError):
                if logger:
                    logger.warning(f"Invalid store_id: {store_id}")
    return shop_ids


def _extract_shop_ids_from_catalog(items: List[Dict]) -> Set[int]:
    shop_ids = set()
    # logger не используется в этой функции, но для единообразия получаем
    _ = get_logger()
    for item in items:
        for k in item:
            if k.startswith("amount_shopId_"):
                parts = k.replace("amount_shopId_", "")
                if parts.isdigit():
                    shop_ids.add(int(parts))
    return shop_ids


def _update_shops_table(conn, shop_ids: Set[int]):
    if not shop_ids:
        return

    with conn.cursor() as cur:
        shop_data = [(sid, f"Shop {sid}", datetime.now()) for sid in shop_ids]
        execute_values(cur, """
            INSERT INTO _shinservice_shops (shop_id, title, updated_at)
            VALUES %s
            ON CONFLICT (shop_id) DO UPDATE SET updated_at = NOW()
        """, shop_data)
        conn.commit()

    logger = get_logger()
    if logger:
        logger.info("Shops updated", extra={"count": len(shop_ids), "status": "success"})


def update_shops_from_stock(conn, items: List[Dict]):
    shop_ids = _extract_shop_ids_from_stock(items)
    _update_shops_table(conn, shop_ids)


def update_shops_from_catalog(conn, items: List[Dict]):
    shop_ids = _extract_shop_ids_from_catalog(items)
    _update_shops_table(conn, shop_ids)


def batch_insert(conn, table: str, columns: List[str], values: List[tuple], conflict: str = None) -> int:
    if not values:
        return 0

    with conn.cursor() as cur:
        sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s"
        if conflict:
            sql += f" ON CONFLICT {conflict}"
        execute_values(cur, sql, values, page_size=BATCH_SIZE)
        conn.commit()

    return len(values)


def safe_int(value, default=0):
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return default


# ====================== UPDATE ФУНКЦИИ ======================
def update_products(conn, items: List[Dict], run_id: str) -> Tuple[int, int]:
    columns = [
        "sku", "title", "brand", "model", "gtin", "season", "diameter",
        "width", "profile", "load_index", "speed_index",
        "pins", "runflat", "extra_load", "photo_url", "raw_json", "updated_at"
    ]

    values = []
    failed = 0
    logger = get_logger()
    for item in items:
        sku = item.get("sku")
        if not sku:
            failed += 1
            continue
        
        try:
            values.append((
                sku,
                item.get("title"),
                item.get("brand"),
                item.get("model"),
                item.get("gtin"),
                item.get("season"),
                item.get("diameter"),
                item.get("width"),
                item.get("profile"),
                item.get("load_index"),
                item.get("speed_index"),
                bool(item.get("pins")),
                bool(item.get("runflat")),
                bool(item.get("extra_load")),
                item.get("photo_url"),
                json.dumps(item, ensure_ascii=False),
                datetime.now()
            ))
        except Exception as e:
            failed += 1
            log_error(conn, run_id, sku, str(e), item)
            if logger:
                logger.warning(f"Failed to process sku={sku}: {e}")

    if not values:
        if logger:
            logger.info("No products to update")
        return 0, failed

    conflict = "(sku) DO UPDATE SET " + ", ".join(f"{c}=EXCLUDED.{c}" for c in columns[:-1]) + ", updated_at=NOW()"

    total = 0
    for i in range(0, len(values), CHUNK_SIZE):
        chunk = values[i:i + CHUNK_SIZE]
        total += batch_insert(conn, "_shinservice_products", columns, chunk, conflict)
        if logger:
            logger.info("Products chunk", extra={"chunk": i // CHUNK_SIZE + 1, "rows": len(chunk), "status": "success"})

    if logger:
        logger.info("Products total updated", extra={"total": total, "failed": failed, "table": "products", "status": "success"})
    return total, failed


def update_offers_prices(conn, items: List[Dict], run_id: str) -> Tuple[int, int]:
    """
    Обновляет цены. Внимание: shop_id = NULL, так как цены общие для всех складов.
    Для привязки к складу используется отдельная таблица остатков _shinservice_offers с shop_id.
    """
    columns = ["sku", "shop_id", "price", "price_retail", "price_msrp", "raw_json", "updated_at"]

    values = []
    failed = 0
    logger = get_logger()
    for item in items:
        sku = item.get("sku")
        if not sku:
            failed += 1
            continue
        
        try:
            # shop_id = NULL для общих цен (не привязаны к складу)
            values.append((
                sku, None,
                item.get("price"),
                item.get("price_retail"),
                item.get("price_msrp"),
                json.dumps(item, ensure_ascii=False),
                datetime.now()
            ))
        except Exception as e:
            failed += 1
            log_error(conn, run_id, sku, str(e), item)
            if logger:
                logger.warning(f"Failed to process price sku={sku}: {e}")

    if not values:
        if logger:
            logger.info("No prices to update")
        return 0, failed

    conflict = ("(sku, shop_id) DO UPDATE SET price=EXCLUDED.price, price_retail=EXCLUDED.price_retail, price_msrp=EXCLUDED.price_msrp, raw_json=EXCLUDED.raw_json, updated_at=NOW()")

    total = 0
    for i in range(0, len(values), CHUNK_SIZE):
        chunk = values[i:i + CHUNK_SIZE]
        total += batch_insert(conn, "_shinservice_offers", columns, chunk, conflict)
        if logger:
            logger.info("Prices chunk", extra={"chunk": i // CHUNK_SIZE + 1, "rows": len(chunk), "status": "success"})

    if logger:
        logger.info("Prices total updated", extra={"total": total, "failed": failed, "table": "offers", "status": "success"})
    return total, failed


def update_offers_stock(conn, items: List[Dict], run_id: str) -> Tuple[int, int]:
    """Обновляет остатки с привязкой к складу (shop_id)"""
    columns = ["sku", "shop_id", "stock", "raw_json", "updated_at"]
    
    values = []
    failed = 0
    total_processed = 0
    logger = get_logger()
    
    for item in items:
        sku = item.get("sku")
        if not sku:
            failed += 1
            continue
        
        try:
            store_id = item.get("store_id")
            stock_total = item.get("stock_total") or item.get("amount_total") or item.get("rest", 0)
            
            # Пропускаем записи без store_id (не создаём мусор с shop_id=0)
            if store_id is None:
                failed += 1
                log_error(conn, run_id, sku, "Missing store_id", item)
                if logger:
                    logger.warning(f"Skipping stock record: missing store_id for sku={sku}")
                continue
            
            store_id_int = safe_int(store_id)
            if store_id_int == 0:
                failed += 1
                log_error(conn, run_id, sku, "Invalid store_id (converted to 0)", item)
                if logger:
                    logger.warning(f"Skipping stock record: invalid store_id for sku={sku}: {store_id}")
                continue
                
            values.append((
                sku,
                store_id_int,
                safe_int(stock_total, 0),
                json.dumps(item, ensure_ascii=False),
                datetime.now()
            ))
            total_processed += 1
        except Exception as e:
            failed += 1
            log_error(conn, run_id, sku, str(e), item)
            if logger:
                logger.warning(f"Failed to process stock sku={sku}: {e}")
            continue
        
        if len(values) >= MAX_STOCK_RECORDS:
            if logger:
                logger.warning(f"Reached MAX_STOCK_RECORDS limit ({MAX_STOCK_RECORDS}), stopping")
            break
    
    if not values:
        if logger:
            logger.info("No stock to update")
        return 0, failed
    
    conflict = ("(sku, shop_id) DO UPDATE SET "
                "stock=EXCLUDED.stock, raw_json=EXCLUDED.raw_json, updated_at=NOW()")
    
    total = 0
    for i in range(0, len(values), CHUNK_SIZE):
        chunk = values[i:i + CHUNK_SIZE]
        total += batch_insert(conn, "_shinservice_offers", columns, chunk, conflict)
        if logger:
            logger.info("Stock chunk", extra={"chunk": i // CHUNK_SIZE + 1, "rows": len(chunk), "status": "success"})
    
    unique_skus = {v[0] for v in values}
    if logger:
        logger.info("Stock total updated", extra={
            "total": total, 
            "unique_skus": len(unique_skus), 
            "total_processed": total_processed,
            "failed": failed,
            "status": "success"
        })
    return total, failed


def vacuum_analyze(conn):
    logger = get_logger()
    if logger:
        logger.info("Starting VACUUM ANALYZE")
    conn.commit()
    with conn.cursor() as cur:
        cur.execute("VACUUM ANALYZE _shinservice_products")
        cur.execute("VACUUM ANALYZE _shinservice_offers")
        cur.execute("VACUUM ANALYZE _shinservice_shops")
        conn.commit()
    if logger:
        logger.info("VACUUM ANALYZE completed", extra={"status": "success"})


def analyze_offers(conn):
    logger = get_logger()
    if logger:
        logger.info("Running ANALYZE _shinservice_offers")
    conn.commit()
    with conn.cursor() as cur:
        cur.execute("ANALYZE _shinservice_offers")
        conn.commit()
    if logger:
        logger.info("ANALYZE completed", extra={"status": "success"})


# ====================== MAIN ======================
def main():
    # Генерируем run_id для этого запуска
    run_id = str(uuid4())[:8]
    setup_logging(run_id)
    logger = get_logger()
    
    start_total = time.perf_counter()
    mode = sys.argv[1] if len(sys.argv) > 1 else "stock"

    if mode not in ("stock", "full"):
        logger.error("Invalid mode", extra={"mode": mode, "status": "error"})
        sys.exit(1)

    conn = psycopg2.connect(DB_CONN)
    
    # Создаём дополнительные таблицы (один раз)
    ensure_tables(conn)
    
    # Регистрируем начало запуска
    update_run_status_start(conn, run_id, mode)
    records_processed = 0
    records_failed = 0

    try:
        logger.info("Update started", extra={"mode": mode.upper(), "status": "start"})

        if mode == "full":
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                # Каталог
                logger.info("Loading catalog data")
                catalog_futures = submit_requests(executor, [
                    (SHINSERVICE_UUID_TYRES, "catalog", "tyres_catalog"),
                    (SHINSERVICE_UUID_DISKS, "catalog", "disks_catalog")
                ])
                all_catalog = []
                for future in as_completed(catalog_futures):
                    items, _ = future.result()
                    all_catalog.extend(items)
                
                logger.info("Pausing before price requests", extra={"delay": REQUEST_DELAY})
                time.sleep(REQUEST_DELAY)
                
                # Цены
                logger.info("Loading price data")
                price_futures = submit_requests(executor, [
                    (SHINSERVICE_UUID_TYRES, "price", "tyres_price"),
                    (SHINSERVICE_UUID_DISKS, "price", "disks_price")
                ])
                all_prices = []
                for future in as_completed(price_futures):
                    items, _ = future.result()
                    all_prices.extend(items)
                
                # Остатки (для полного обновления)
                logger.info("Loading stock data")
                stock_futures = submit_requests(executor, [
                    (SHINSERVICE_UUID_TYRES, "stock", "tyres_stock"),
                    (SHINSERVICE_UUID_DISKS, "stock", "disks_stock")
                ])
                all_stock = []
                for future in as_completed(stock_futures):
                    items, _ = future.result()
                    all_stock.extend(items)

            # Обновление
            update_shops_from_catalog(conn, all_catalog)
            prod_processed, prod_failed = update_products(conn, all_catalog, run_id)
            price_processed, price_failed = update_offers_prices(conn, all_prices, run_id)
            stock_processed, stock_failed = update_offers_stock(conn, all_stock, run_id)
            
            records_processed += prod_processed + price_processed + stock_processed
            records_failed += prod_failed + price_failed + stock_failed
            
            vacuum_analyze(conn)
            
            update_run_status_finish(conn, run_id, mode, 'success', records_processed, records_failed)
            logger.info("Full update completed", extra={"mode": "full", "status": "success", 
                                                        "records_processed": records_processed,
                                                        "records_failed": records_failed})

        else:  # stock
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                logger.info("Loading stock data")
                stock_futures = submit_requests(executor, [
                    (SHINSERVICE_UUID_TYRES, "stock", "tyres_stock"),
                    (SHINSERVICE_UUID_DISKS, "stock", "disks_stock")
                ])
                all_stock = []
                for future in as_completed(stock_futures):
                    items, _ = future.result()
                    all_stock.extend(items)

            update_shops_from_stock(conn, all_stock)
            stock_processed, stock_failed = update_offers_stock(conn, all_stock, run_id)
            records_processed += stock_processed
            records_failed += stock_failed
            analyze_offers(conn)
            
            update_run_status_finish(conn, run_id, mode, 'success', records_processed, records_failed)
            logger.info("Stock update completed", extra={"mode": "stock", "status": "success",
                                                         "records_processed": records_processed,
                                                         "records_failed": records_failed})

        elapsed = time.perf_counter() - start_total
        logger.info("Update finished", extra={"mode": mode.upper(), "duration": round(elapsed, 2), 
                                              "status": "success", "records_processed": records_processed,
                                              "records_failed": records_failed})

    except Exception as e:
        update_run_status_finish(conn, run_id, mode, 'failed', records_processed, records_failed)
        logger.error("Update failed", extra={"mode": mode.upper(), "error": str(e), "status": "error"}, exc_info=True)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
