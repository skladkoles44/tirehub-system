#!/usr/bin/env python3
"""
ETL Шинсервис v12.17 — Production Ready
"""

import sys, json, os, hashlib, logging, traceback, time
from uuid import uuid4
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager

import psycopg2
import requests
from psycopg2.extras import execute_values
from psycopg2.pool import ThreadedConnectionPool
from psycopg2 import errors as psycopg2_errors
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# ====================== CONFIG ======================
SHINSERVICE_UUID_TYRES = os.getenv("SHINSERVICE_UUID_TYRES", "019dbb42-9e14-b7d0-a829-b64101ead29f")
SHINSERVICE_UUID_DISKS = os.getenv("SHINSERVICE_UUID_DISKS", "019dbb40-9828-be33-9728-e5d7db368ca6")
BASE_URL = "https://duplo-api.shinservice.ru/api/v1/exporter"
DB_CONN = os.getenv("DB_CONN", "dbname=canonical user=canonical host=/var/run/postgresql")
TOKEN = os.getenv("SHINSERVICE_TOKEN")

REQUEST_TIMEOUT = int(os.getenv("SHINSERVICE_TIMEOUT", "90"))
MAX_WORKERS = int(os.getenv("SHINSERVICE_MAX_WORKERS", "3"))
EXECUTE_VALUES_PAGE_SIZE = 2500
LOCK_TIMEOUT_MINUTES = int(os.getenv("SHINSERVICE_LOCK_TIMEOUT", "60"))

# ====================== VALIDATION ======================
if not TOKEN:
    print("❌ Ошибка: SHINSERVICE_TOKEN не установлен")
    sys.exit(1)

if not DB_CONN:
    print("❌ Ошибка: DB_CONN не установлен")
    sys.exit(1)

# ====================== GLOBAL SESSION & POOL ======================
session = requests.Session()
session.headers.update({
    "Authorization": f"Bearer {TOKEN}",
    "Accept-Encoding": "gzip, deflate",
    "User-Agent": "CanonicalCore-ETL/12.17"
})

db_pool = ThreadedConnectionPool(
    minconn=2,
    maxconn=MAX_WORKERS + 4,
    dsn=DB_CONN
)

import atexit
atexit.register(session.close)
atexit.register(db_pool.closeall)

# ====================== LOGGING ======================
def setup_logging():
    """Настраивает корневой логгер. Вызывается один раз при старте."""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    for h in logger.handlers[:]:
        logger.removeHandler(h)

    class JsonFormatter(logging.Formatter):
        def format(self, record):
            log_entry = {
                "time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "level": record.levelname,
                "message": record.getMessage(),
                "run_id": getattr(record, "run_id", None),
                "mode": getattr(record, "mode", None),
                "module": record.module,
            }
            if record.exc_info:
                log_entry["traceback"] = "".join(traceback.format_exception(*record.exc_info))
            return json.dumps(log_entry, ensure_ascii=False, default=str)

    formatter = JsonFormatter()
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    fileh = logging.FileHandler("/var/log/shinservice_etl.log", encoding="utf-8")
    fileh.setFormatter(formatter)
    logger.addHandler(console)
    logger.addHandler(fileh)
    return logger

# ====================== CONTEXT MANAGER ======================
@contextmanager
def get_db_conn():
    """Безопасное получение соединения из пула с восстановлением autocommit"""
    conn = db_pool.getconn()
    autocommit_original = conn.autocommit
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.rollback()
        conn.autocommit = autocommit_original
        db_pool.putconn(conn)

# ====================== DB: миграции ======================
def migrate_schema(logger):
    """
    Выполняет миграции схемы БД.
    Вызывается только при RUN_MIGRATIONS=true.
    """
    logger.info("migrate_schema: starting")
    with get_db_conn() as conn:
        autocommit_original = conn.autocommit
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS _shinservice_etl_runs (
                        run_id TEXT PRIMARY KEY,
                        mode TEXT,
                        status TEXT,
                        records_processed INT DEFAULT 0,
                        records_failed INT DEFAULT 0,
                        started_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                        finished_at TIMESTAMPTZ,
                        catalog_worker_duration_ms NUMERIC(10,2),
                        price_worker_duration_ms NUMERIC(10,2),
                        stock_worker_duration_ms NUMERIC(10,2),
                        catalog_rows INT,
                        price_rows INT,
                        stock_rows INT
                    )
                """)
                cur.execute("ALTER TABLE _shinservice_etl_runs ADD COLUMN IF NOT EXISTS lock_owner INT")
                cur.execute("ALTER TABLE _shinservice_etl_runs ADD COLUMN IF NOT EXISTS locked_at TIMESTAMPTZ")

                cur.execute("""
                    CREATE TABLE IF NOT EXISTS _shinservice_raw (
                        id BIGSERIAL PRIMARY KEY,
                        endpoint_type TEXT NOT NULL CHECK (endpoint_type IN ('catalog', 'price', 'stock')),
                        data JSONB NOT NULL,
                        data_hash TEXT NOT NULL,
                        run_id TEXT,
                        loaded_at TIMESTAMPTZ DEFAULT NOW(),
                        sku TEXT GENERATED ALWAYS AS (data->>'sku') STORED,
                        item_type TEXT GENERATED ALWAYS AS (data->>'type') STORED
                    )
                """)
                
                # ADD CONSTRAINT IF NOT EXISTS не поддерживается — проверяем вручную
                cur.execute("""
                    DO $$
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM pg_constraint 
                            WHERE conname = 'uq_raw_hash' 
                              AND conrelid = '_shinservice_raw'::regclass
                        ) THEN
                            ALTER TABLE _shinservice_raw ADD CONSTRAINT uq_raw_hash UNIQUE (data_hash);
                        END IF;
                    END $$;
                """)

                cur.execute("CREATE INDEX IF NOT EXISTS idx_raw_endpoint ON _shinservice_raw(endpoint_type)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_raw_sku ON _shinservice_raw(sku)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_raw_type ON _shinservice_raw(item_type)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_raw_loaded ON _shinservice_raw(loaded_at DESC)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_raw_gin ON _shinservice_raw USING GIN(data)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_raw_gin_path ON _shinservice_raw USING GIN(data jsonb_path_ops)")

                cur.execute("""
                    CREATE TABLE IF NOT EXISTS _shinservice_etl_errors (
                        id BIGSERIAL PRIMARY KEY,
                        run_id TEXT,
                        endpoint_type TEXT,
                        sku TEXT,
                        error TEXT,
                        raw_json JSONB,
                        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                    )
                """)
        finally:
            conn.autocommit = autocommit_original
    logger.info("migrate_schema: completed")

# ====================== DB: блокировка ======================
ADVISORY_LOCK_ID = 0x12F6E0

def acquire_lock(conn, run_id, mode, logger):
    """Захват эксклюзивной блокировки через pg_advisory_lock"""
    with conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s)", (ADVISORY_LOCK_ID,))
        if not cur.fetchone()[0]:
            logger.warning("Другой процесс ETL уже выполняется, пропускаем")
            return False

        cur.execute(
            "DELETE FROM _shinservice_etl_runs WHERE status = 'running' AND locked_at < NOW() - (%s || ' minutes')::INTERVAL",
            (str(LOCK_TIMEOUT_MINUTES),)
        )

        cur.execute("""
            INSERT INTO _shinservice_etl_runs (run_id, mode, status, started_at, lock_owner, locked_at)
            VALUES (%s, %s, 'running', NOW(), pg_backend_pid(), NOW())
        """, (run_id, mode))
        conn.commit()
        return True

def release_lock(conn):
    """Освобождение advisory lock и очистка lock_owner"""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE _shinservice_etl_runs
            SET lock_owner = NULL, locked_at = NULL
            WHERE lock_owner = pg_backend_pid()
        """)
        cur.execute("SELECT pg_advisory_unlock(%s)", (ADVISORY_LOCK_ID,))
        conn.commit()

def update_run_status(conn, run_id, status, metrics=None):
    """Обновление статуса и метрик"""
    with conn.cursor() as cur:
        if status == 'success' and metrics:
            cur.execute("""
                UPDATE _shinservice_etl_runs 
                SET status = 'success', finished_at = NOW(),
                    catalog_worker_duration_ms = %s,
                    price_worker_duration_ms = %s,
                    stock_worker_duration_ms = %s,
                    catalog_rows = %s, price_rows = %s, stock_rows = %s
                WHERE run_id = %s
            """, (
                metrics.get('catalog', {}).get('worker_duration_ms', 0),
                metrics.get('price', {}).get('worker_duration_ms', 0),
                metrics.get('stock', {}).get('worker_duration_ms', 0),
                metrics.get('catalog', {}).get('rows', 0),
                metrics.get('price', {}).get('rows', 0),
                metrics.get('stock', {}).get('rows', 0),
                run_id
            ))
        else:
            cur.execute("""
                UPDATE _shinservice_etl_runs 
                SET status = %s, finished_at = NOW()
                WHERE run_id = %s
            """, (status, run_id))
        conn.commit()

# ====================== FETCH + SAVE ======================
@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1.5, min=2, max=60),
       retry=retry_if_exception_type((requests.RequestException, psycopg2_errors.OperationalError)))
def fetch_and_save(uuid, endpoint_type, run_id, logger):
    with get_db_conn() as conn:
        start_time = time.perf_counter()
        url = f"{BASE_URL}/{uuid}/download?type={endpoint_type}&format=json"

        resp = session.get(url, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        raw = resp.json()

        if not raw:
            logger.warning("Empty response from API", extra={"endpoint_type": endpoint_type, "run_id": run_id})
            return endpoint_type, 0, 0, 0, 0.0

        if isinstance(raw, dict):
            for k in ("items", "tyre", "disk", "stock", "data"):
                if k in raw and isinstance(raw[k], list):
                    items = raw[k]
                    break
            else:
                items = [raw]
        else:
            items = raw if isinstance(raw, list) else [raw]

        values = []
        error_values = []
        failed = 0

        for item in items:
            try:
                data_json = json.dumps(item, sort_keys=True, ensure_ascii=False)
                data_hash = hashlib.sha256(data_json.encode('utf-8')).hexdigest()
                values.append((endpoint_type, json.dumps(item, ensure_ascii=False), data_hash, run_id))
            except Exception as e:
                failed += 1
                sku = item.get("sku") if isinstance(item, dict) else None
                error_values.append((run_id, endpoint_type, sku, str(e)[:500],
                                    json.dumps(item, ensure_ascii=False) if isinstance(item, dict) else None))

        if error_values:
            with conn.cursor() as cur:
                execute_values(cur, """
                    INSERT INTO _shinservice_etl_errors (run_id, endpoint_type, sku, error, raw_json)
                    VALUES %s
                """, error_values, page_size=EXECUTE_VALUES_PAGE_SIZE)
                conn.commit()

        saved = 0
        duplicates = 0
        if values:
            with conn.cursor() as cur:
                result = execute_values(
                    cur,
                    """
                    INSERT INTO _shinservice_raw (endpoint_type, data, data_hash, run_id)
                    VALUES %s ON CONFLICT (data_hash) DO NOTHING
                    RETURNING id
                    """,
                    values,
                    page_size=EXECUTE_VALUES_PAGE_SIZE,
                    fetch=True
                )
                saved = sum(len(batch) for batch in result) if result else 0
                duplicates = len(values) - saved
                conn.commit()

        worker_duration_ms = round((time.perf_counter() - start_time) * 1000, 2)

        logger.info("Fetch completed", extra={
            "endpoint_type": endpoint_type,
            "items_total": len(items),
            "saved_new": saved,
            "duplicates_skipped": duplicates,
            "failed": failed,
            "worker_duration_ms": worker_duration_ms,
            "run_id": run_id
        })

        return endpoint_type, saved, failed, len(items), worker_duration_ms

# ====================== MAIN ======================
def main():
    run_id = str(uuid4())[:8]
    mode = sys.argv[1] if len(sys.argv) > 1 else "stock"

    logger = logging.getLogger(__name__)

    with get_db_conn() as conn:
        if not acquire_lock(conn, run_id, mode, logger):
            return

        try:
            logger.info("ETL run started", extra={"mode": mode, "run_id": run_id, "workers": MAX_WORKERS})
            start_total = time.perf_counter()
            endpoint_metrics = {}
            total_ok = total_fail = 0

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = []
                uuids = [SHINSERVICE_UUID_TYRES, SHINSERVICE_UUID_DISKS]
                endpoints = ["catalog", "price", "stock"] if mode == "full" else ["stock"]

                for uuid in uuids:
                    for ep in endpoints:
                        futures.append(executor.submit(fetch_and_save, uuid, ep, run_id, logger))

                for future in as_completed(futures):
                    ep, saved, failed, total_items, duration = future.result()
                    total_ok += saved
                    total_fail += failed

                    metrics = endpoint_metrics.setdefault(ep, {'worker_duration_ms': 0, 'rows': 0})
                    metrics['worker_duration_ms'] += duration
                    metrics['rows'] += total_items

            total_duration_ms = round((time.perf_counter() - start_total) * 1000, 2)
            update_run_status(conn, run_id, 'success', endpoint_metrics)

            logger.info("ETL run completed successfully", extra={
                "run_id": run_id, "mode": mode,
                "total_saved": total_ok, "total_failed": total_fail,
                "total_duration_ms": total_duration_ms,
                "metrics": endpoint_metrics
            })

        except Exception:
            update_run_status(conn, run_id, 'failed')
            logger.error("ETL run failed", extra={"run_id": run_id}, exc_info=True)
            raise
        finally:
            release_lock(conn)

if __name__ == "__main__":
    setup_logging()
    logger = logging.getLogger("migration")
    if os.getenv("RUN_MIGRATIONS", "false").lower() == "true":
        logger.info("Running migrations", extra={"run_id": "migration", "mode": "init"})
        migrate_schema(logger)
    else:
        logger.info("Migrations skipped (RUN_MIGRATIONS not set)", extra={"run_id": "migration", "mode": "init"})
    main()
