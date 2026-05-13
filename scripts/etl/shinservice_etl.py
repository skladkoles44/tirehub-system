#!/usr/bin/env python3
"""
ETL Шинсервис v4.9 — production-ready (финальная стабильная версия)

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
DB_CONN = os.getenv("DB_CONN", "dbname=canonical user=canonical host=/var/run/postgresql")

BATCH_SIZE = int(os.getenv("SHINSERVICE_BATCH_SIZE", "2000"))
CHUNK_SIZE = int(os.getenv("SHINSERVICE_CHUNK_SIZE", "10000"))
REQUEST_TIMEOUT = int(os.getenv("SHINSERVICE_TIMEOUT", "90"))
MAX_WORKERS = int(os.getenv("SHINSERVICE_MAX_WORKERS", "2"))
REQUEST_DELAY = float(os.getenv("SHINSERVICE_REQUEST_DELAY", "0.15"))
MAX_STOCK_RECORDS = int(os.getenv("SHINSERVICE_MAX_STOCK_RECORDS", "1000000"))

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
        
        if record.exc_info:
            log_entry["traceback"] = "".join(traceback.format_exception(*record.exc_info))
        
        return json.dumps(log_entry, ensure_ascii=False)

root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(JsonFormatter())

file_handler = logging.FileHandler('/var/log/shinservice_etl.log')
file_handler.setFormatter(JsonFormatter())

root_logger.addHandler(console_handler)
root_logger.addHandler(file_handler)

logger = logging.getLogger(__name__)

# ====================== HTTP СЕССИЯ ======================
session = requests.Session()
session.headers.update({"User-Agent": "Canonical-Core-ETL/4.9 (Shinservice)"})


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
        
        # Для stock API поддерживаем максимальное количество форматов
        if export_type == "stock":
            if isinstance(data, list):
                items = data
                source = "stock_array"
            elif isinstance(data, dict):
                # Обёртка с полем status OK
                if data.get("status") == "OK":
                    items = (data.get("data") or data).get("items", [])
                    source = "stock_wrapper"
                # Максимальный fallback
                if not items:
                    nested = data.get("data") or data
                    items = nested.get("items") or nested.get("stock") or nested.get("stock_list") or []
                    source = "stock_nested"
        
        # Если items всё ещё пуст, пробуем другие форматы
        if not items and isinstance(data, dict):
            # Прямые массивы в ключах tyre/disk
            for key in ("tyre", "disk"):
                chunk = data.get(key) or []
                if isinstance(chunk, list):
                    items.extend(chunk)
                    source = key
            # Если нет tyre/disk, пробуем items
            if not items and "items" in data:
                chunk = data.get("items") or []
                if isinstance(chunk, list):
                    items.extend(chunk)
                    source = "items"
        
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
            logger.info("Delaying request", extra={"delay": REQUEST_DELAY, "request": request_name})
            time.sleep(REQUEST_DELAY)
        futures.append(executor.submit(fetch_data, uuid, export_type, request_name))
    return futures


# ====================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ======================
def update_shops_from_stock(conn, items: List[Dict]):
    """Обновляет справочник складов из stock API (поле store_id)"""
    shop_ids = set()
    for item in items:
        store_id = item.get("store_id")
        if store_id is not None:
            try:
                shop_ids.add(int(store_id))
            except (ValueError, TypeError):
                logger.warning(f"Invalid store_id: {store_id}")
    
    if not shop_ids:
        return

    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO _shinservice_shops (shop_id, title, updated_at)
            VALUES %s
            ON CONFLICT (shop_id) DO UPDATE SET updated_at = NOW()
        """, [(sid, f"Shop {sid}", datetime.now()) for sid in shop_ids])
        conn.commit()

    logger.info(f"Shops updated from stock", extra={"count": len(shop_ids), "status": "success"})


def update_shops_from_catalog(conn, items: List[Dict]):
    """Обновляет справочник складов из amount_shopId_* полей (catalog/price)"""
    shop_ids = set()
    for item in items:
        for k in item:
            if k.startswith("amount_shopId_"):
                parts = k.replace("amount_shopId_", "")
                if parts.isdigit():
                    shop_ids.add(int(parts))
    
    if not shop_ids:
        return

    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO _shinservice_shops (shop_id, title, updated_at)
            VALUES %s
            ON CONFLICT (shop_id) DO UPDATE SET updated_at = NOW()
        """, [(sid, f"Shop {sid}", datetime.now()) for sid in shop_ids])
        conn.commit()

    logger.info(f"Shops updated from catalog", extra={"count": len(shop_ids), "status": "success"})


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


def safe_int(value, default=0):
    """Безопасное преобразование в int"""
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return default


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
        total += batch_insert(conn, "_shinservice_offers", columns, chunk, conflict)
        logger.info(f"Prices chunk", extra={"chunk": i // CHUNK_SIZE + 1, "rows": len(chunk), "status": "success"})

    logger.info(f"Prices total updated", extra={"total": total, "table": "offers", "status": "success"})
    return total


def update_offers_stock(conn, items: List[Dict]) -> int:
    """Обновляет остатки из stock API (формат: массив офферов)"""
    columns = ["sku", "shop_id", "stock", "raw_json", "updated_at"]
    
    values = []
    total_processed = 0
    for item in items:
        sku = item.get("sku")
        store_id = item.get("store_id")
        # Поддержка разных названий полей с остатками
        stock_total = item.get("stock_total") or item.get("amount_total") or item.get("rest", 0)
        # Безопасное преобразование в int
        stock_total = safe_int(stock_total, 0)
        
        if sku and store_id is not None:
            try:
                store_id_int = safe_int(store_id)
                if store_id_int == 0:
                    logger.warning(f"Invalid store_id for sku={sku}: {store_id}")
                    continue
                values.append((
                    sku,
                    store_id_int,
                    stock_total,
                    json.dumps(item, ensure_ascii=False),
                    datetime.now()
                ))
                total_processed += 1
            except (ValueError, TypeError) as e:
                logger.warning(f"Invalid stock data for sku={sku}: {e}")
                continue
        
        # Защита от переполнения памяти
        if len(values) >= MAX_STOCK_RECORDS:
            logger.warning(f"Reached MAX_STOCK_RECORDS limit ({MAX_STOCK_RECORDS}), stopping")
            break
    
    if not values:
        logger.info("No stock to update")
        return 0
    
    conflict = ("(sku, shop_id) DO UPDATE SET "
                "stock=EXCLUDED.stock, raw_json=EXCLUDED.raw_json, updated_at=NOW()")
    
    total = batch_insert(conn, "_shinservice_offers", columns, values, conflict)
    unique_skus = {v[0] for v in values}
    logger.info(f"Stock total updated", extra={
        "total": total, 
        "unique_skus": len(unique_skus), 
        "total_processed": total_processed,
        "status": "success"
    })
    return total


def vacuum_analyze(conn):
    """Оптимизация таблиц"""
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

            # Обновление
            update_shops_from_catalog(conn, all_catalog)
            update_products(conn, all_catalog)
            update_offers_prices(conn, all_prices)
            vacuum_analyze(conn)
            
            logger.info("Full update completed", extra={"mode": "full", "status": "success"})

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

            # Обновление
            update_shops_from_stock(conn, all_stock)
            update_offers_stock(conn, all_stock)
            
            logger.info("Stock update completed", extra={"mode": "stock", "status": "success"})

        elapsed = time.perf_counter() - start_total
        logger.info("Update finished", extra={"mode": mode.upper(), "duration": round(elapsed, 2), "status": "success"})

    except Exception as e:
        logger.error("Update failed", extra={"mode": mode.upper(), "error": str(e), "status": "error"}, exc_info=True)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
