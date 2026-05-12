#!/usr/bin/env python3
"""
ETL Шинсервис v4.3 — production-ready (полное логирование с traceback)

Режимы:
  stock - обновление остатков (каждые 30 минут)
  full  - полное обновление каталога, цен и складов (раз в сутки)

Логирование в JSON для удобного парсинга (ELK, Grafana, grep)
"""

import sys
import json
import logging
import traceback
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, List, Tuple

import psycopg2
import requests
from psycopg2.extras import execute_values
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# ====================== КОНФИГУРАЦИЯ ======================
SHINSERVICE_UUID_TYRES = os.getenv("SHINSERVICE_UUID_TYRES", "019dbb42-9e14-b7d0-a829-b64101ead29f")
SHINSERVICE_UUID_DISKS = os.getenv("SHINSERVICE_UUID_DISKS", "019dbb40-9828-be33-9728-e5d7db368ca6")
BASE_URL = "https://duplo-api.shinservice.ru/api/v1/exporter"
DB_CONN = os.getenv("DB_CONN", "dbname=canonical user=canonical host=localhost")

BATCH_SIZE = int(os.getenv("SHINSERVICE_BATCH_SIZE", "2000"))
CHUNK_SIZE = int(os.getenv("SHINSERVICE_CHUNK_SIZE", "10000"))
REQUEST_TIMEOUT = int(os.getenv("SHINSERVICE_TIMEOUT", "90"))
MAX_WORKERS = int(os.getenv("SHINSERVICE_MAX_WORKERS", "2"))
REQUEST_DELAY = float(os.getenv("SHINSERVICE_REQUEST_DELAY", "0.15"))

# ====================== ПОТОКОБЕЗОПАСНЫЙ СЧЁТЧИК ======================
class SafeCounter:
    """Потокобезопасный счётчик для ID запросов (синглтон)"""
    _instance = None
    _value = 0
    _lock = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            try:
                import threading
                cls._lock = threading.Lock()
            except ImportError:
                pass
        return cls._instance
    
    def __call__(self) -> int:
        if self._lock:
            with self._lock:
                self._value += 1
                return self._value
        self._value += 1
        return self._value

_request_counter = SafeCounter()

# ====================== НАСТРОЙКА ЛОГИРОВАНИЯ ======================
class JsonFormatter(logging.Formatter):
    """JSON-форматер с поддержкой traceback"""
    def format(self, record):
        log_entry = {
            "time": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
            "funcName": record.funcName,
            "lineno": record.lineno,
            "extra": getattr(record, "extra", {})
        }
        
        # Добавляем traceback при ошибках
        if record.exc_info:
            log_entry["traceback"] = "".join(traceback.format_exception(*record.exc_info))
        
        return json.dumps(log_entry, ensure_ascii=False)

# Настройка root logger без basicConfig
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

# Удаляем существующие handlers, если есть
for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)

# Консольный handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(JsonFormatter())

# Файловый handler
file_handler = logging.FileHandler('/var/log/shinservice_etl.log')
file_handler.setFormatter(JsonFormatter())

root_logger.addHandler(console_handler)
root_logger.addHandler(file_handler)

logger = logging.getLogger(__name__)

# ====================== HTTP СЕССИЯ ======================
session = requests.Session()
session.headers.update({"User-Agent": "Canonical-Core-ETL/4.3 (Shinservice)"})


# ====================== FETCH C RETRY И МОНИТОРИНГОМ ======================
@retry(stop=stop_after_attempt(4), wait=wait_exponential(multiplier=1.5, min=3, max=30),
       retry=retry_if_exception_type((requests.RequestException, ConnectionError)))
def fetch_data(uuid: str, export_type: str, request_name: str) -> Tuple[List[Dict], str]:
    """Загружает данные по UUID и типу выгрузки с повторными попытками"""
    start = time.perf_counter()
    url = f"{BASE_URL}/{uuid}/download?type={export_type}&format=json"
    req_num = _request_counter()

    try:
        resp = session.get(url, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        
        items = []
        source = "unknown"
        if isinstance(data, dict):
            for key in ("tyre", "disk", "items"):
                chunk = data.get(key) or []
                if isinstance(chunk, list):
                    items.extend(chunk)
                    source = key
        
        duration = time.perf_counter() - start
        logger.info(f"Fetch completed", extra={
            "request_id": req_num,
            "request": request_name,
            "count": len(items),
            "duration": round(duration, 2),
            "source": source,
            "status": "success"
        })
        return items, source
    
    except Exception as e:
        duration = time.perf_counter() - start
        logger.error(f"Fetch failed", extra={
            "request_id": req_num,
            "request": request_name,
            "duration": round(duration, 2),
            "error": str(e),
            "status": "error"
        }, exc_info=True)
        raise


def submit_requests(executor, requests_config):
    """Отправка запросов с задержками (первый без задержки, остальные с REQUEST_DELAY)"""
    futures = []
    for i, (uuid, export_type, request_name) in enumerate(requests_config):
        if i > 0:
            logger.info(f"Delaying request", extra={"delay": REQUEST_DELAY, "request": request_name})
            time.sleep(REQUEST_DELAY)
        futures.append(executor.submit(fetch_data, uuid, export_type, request_name))
    return futures


# ====================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ======================
def update_shops(conn, items: List[Dict]):
    """Обновляет справочник складов из amount_shopId_* полей"""
    shop_ids = {
        int(k.replace("amount_shopId_", ""))
        for item in items
        for k in item
        if k.startswith("amount_shopId_") and k.replace("amount_shopId_", "").isdigit()
    }

    if not shop_ids:
        return

    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO _shinservice_shops (shop_id, name, updated_at)
            VALUES %s
            ON CONFLICT (shop_id) DO UPDATE SET updated_at = NOW()
        """, [(sid, f"Shop {sid}", datetime.now()) for sid in shop_ids])
        conn.commit()

    logger.info(f"Shops updated", extra={"count": len(shop_ids), "status": "success"})


def batch_insert(conn, table: str, columns: List[str], values: List[tuple], conflict: str = None) -> int:
    """Пакетная вставка с execute_values"""
    if not values:
        return 0

    with conn.cursor() as cur:
        sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s"
        if conflict:
            sql += f" ON CONFLICT {conflict}"
        execute_values(cur, sql, values, page_size=BATCH_SIZE)
        conn.commit()

    return len(values)


def update_products(conn, items: List[Dict]) -> int:
    """Обновляет каталог товаров"""
    columns = [
        "sku", "title", "brand", "model", "gtin", "season", "diameter",
        "width", "profile", "load_index", "speed_index",
        "pins", "runflat", "extra_load", "photo_url", "raw_json", "updated_at"
    ]

    values = []
    for item in items:
        sku = item.get("sku")
        if not sku:
            continue
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

    if not values:
        logger.info("No products to update")
        return 0

    conflict = "(sku) DO UPDATE SET " + ", ".join(f"{c}=EXCLUDED.{c}" for c in columns[:-1]) + ", updated_at=NOW()"

    total = 0
    for i in range(0, len(values), CHUNK_SIZE):
        chunk = values[i:i + CHUNK_SIZE]
        count = batch_insert(conn, "_shinservice_products", columns, chunk, conflict)
        total += count
        logger.info(f"Products chunk", extra={"chunk": i // CHUNK_SIZE + 1, "rows": len(chunk), "status": "success"})

    logger.info(f"Products total updated", extra={"total": total, "table": "products", "status": "success"})
    return total


def update_offers_prices(conn, items: List[Dict]) -> int:
    """Обновляет цены"""
    columns = ["sku", "shop_id", "price", "price_retail", "price_msrp", "raw_json", "updated_at"]

    values = []
    for item in items:
        sku = item.get("sku")
        if not sku:
            continue
        values.append((
            sku, 0,
            item.get("price"),
            item.get("price_retail"),
            item.get("price_msrp"),
            json.dumps(item, ensure_ascii=False),
            datetime.now()
        ))

    if not values:
        logger.info("No prices to update")
        return 0

    conflict = ("(sku, shop_id) DO UPDATE SET "
                "price=EXCLUDED.price, price_retail=EXCLUDED.price_retail, "
                "price_msrp=EXCLUDED.price_msrp, raw_json=EXCLUDED.raw_json, updated_at=NOW()")

    total = 0
    for i in range(0, len(values), CHUNK_SIZE):
        chunk = values[i:i + CHUNK_SIZE]
        count = batch_insert(conn, "_shinservice_offers", columns, chunk, conflict)
        total += count
        logger.info(f"Prices chunk", extra={"chunk": i // CHUNK_SIZE + 1, "rows": len(chunk), "status": "success"})

    logger.info(f"Prices total updated", extra={"total": total, "table": "offers", "status": "success"})
    return total


def update_offers_stock(conn, items: List[Dict]) -> int:
    """Обновляет остатки"""
    columns = ["sku", "shop_id", "stock", "raw_json", "updated_at"]

    all_values = []
    for item in items:
        sku = item.get("sku")
        if not sku:
            continue
        for key, val in item.items():
            if key.startswith("amount_shopId_") and isinstance(val, (int, float)):
                shop_id_str = key.replace("amount_shopId_", "")
                if shop_id_str.isdigit():
                    all_values.append((
                        sku,
                        int(shop_id_str),
                        int(val),
                        json.dumps(item, ensure_ascii=False),
                        datetime.now()
                    ))

    if not all_values:
        logger.info("No stock to update")
        return 0

    conflict = ("(sku, shop_id) DO UPDATE SET "
                "stock=EXCLUDED.stock, raw_json=EXCLUDED.raw_json, updated_at=NOW()")

    total = 0
    for i in range(0, len(all_values), CHUNK_SIZE):
        chunk = all_values[i:i + CHUNK_SIZE]
        count = batch_insert(conn, "_shinservice_offers", columns, chunk, conflict)
        total += count
        logger.info(f"Stock chunk", extra={"chunk": i // CHUNK_SIZE + 1, "rows": len(chunk), "status": "success"})

    unique_skus = {v[0] for v in all_values}
    logger.info(f"Stock total updated", extra={"total": total, "unique_skus": len(unique_skus), "table": "offers", "status": "success"})
    return total


def vacuum_analyze(conn):
    """Оптимизация таблиц после full обновления"""
    logger.info("Starting VACUUM ANALYZE")
    with conn.cursor() as cur:
        cur.execute("VACUUM ANALYZE _shinservice_products")
        cur.execute("VACUUM ANALYZE _shinservice_offers")
        cur.execute("VACUUM ANALYZE _shinservice_shops")
        conn.commit()
    logger.info("VACUUM ANALYZE completed", extra={"status": "success"})


# ====================== MAIN ======================
def main():
    start_total = time.perf_counter()
    mode = sys.argv[1] if len(sys.argv) > 1 else "stock"

    if mode not in ("stock", "full"):
        logger.error(f"Invalid mode", extra={"mode": mode, "status": "error"})
        sys.exit(1)

    conn = psycopg2.connect(DB_CONN)

    try:
        logger.info(f"Update started", extra={"mode": mode.upper(), "status": "start"})

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
                
                # Пауза
                logger.info(f"Pausing before price requests", extra={"delay": REQUEST_DELAY})
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

            # Обновление
            update_shops(conn, all_catalog)
            update_products(conn, all_catalog)
            update_offers_prices(conn, all_prices)
            vacuum_analyze(conn)
            
            logger.info(f"Full update completed", extra={"mode": "full", "status": "success"})

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

            update_shops(conn, all_stock)
            update_offers_stock(conn, all_stock)
            
            logger.info(f"Stock update completed", extra={"mode": "stock", "status": "success"})

        elapsed = time.perf_counter() - start_total
        logger.info(f"Update finished", extra={"mode": mode.upper(), "duration": round(elapsed, 2), "status": "success"})

    except Exception as e:
        logger.error(f"Update failed", extra={"mode": mode.upper(), "error": str(e), "status": "error"}, exc_info=True)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
