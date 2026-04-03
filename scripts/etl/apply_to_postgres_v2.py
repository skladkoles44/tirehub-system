#!/usr/bin/env python3
"""
apply_to_postgres_v2.py — загрузка good.ndjson в PostgreSQL
"""

import sys
import json
import psycopg2
from psycopg2.extras import execute_values
from pathlib import Path

DB_URL = "postgresql://user:password@localhost:5432/tirehub"
BATCH_SIZE = 1000


def price_to_cents(price):
    if price is None:
        return 0
    return int(round(float(price) * 100))


def load_good_to_db(good_path, conn):
    items = []
    offers = []
    suppliers = set()
    
    with open(good_path) as f:
        for line in f:
            row = json.loads(line)
            
            supplier = row.get("supplier", "unknown")
            suppliers.add(supplier)
            
            items.append((
                row["identity_key"],
                row.get("item_type", "tire_passenger"),
                row.get("brand"),
                row.get("model"),
                row.get("width"),
                row.get("height"),
                row.get("diameter"),
                row.get("season"),
                row.get("load_index"),
                row.get("speed_index"),
                json.dumps(row.get("attributes", {})),
                row.get("normalizer_version"),
                row.get("identity_version")
            ))
            
            offers.append((
                row["identity_key"],
                supplier,
                row.get("supplier_sku", ""),
                price_to_cents(row.get("price")),
                "RUB",
                row.get("stock_qty")
            ))
    
    with conn.cursor() as cur:
        # suppliers
        supplier_list = [(s,) for s in suppliers]
        execute_values(cur, """
            INSERT INTO suppliers (name) VALUES %s
            ON CONFLICT (name) DO NOTHING
        """, supplier_list)
        
        # supplier mapping
        cur.execute("SELECT id, name FROM suppliers WHERE name = ANY(%s)", (list(suppliers),))
        supplier_map = {name: sid for sid, name in cur.fetchall()}
        
        # items
        execute_values(cur, """
            INSERT INTO items (
                identity_key, item_type, brand, model, width, height, diameter,
                season, load_index, speed_index, attributes, normalizer_version, identity_version
            ) VALUES %s
            ON CONFLICT (identity_key) DO NOTHING
        """, items)
        
        # offers batch
        offer_batch = []
        for item_key, supplier_name, sku, price_cents, currency, stock in offers:
            supplier_id = supplier_map.get(supplier_name)
            if supplier_id:
                offer_batch.append((item_key, supplier_id, sku, price_cents, currency, stock))
        
        total = len(offer_batch)
        for i in range(0, total, BATCH_SIZE):
            batch = offer_batch[i:i+BATCH_SIZE]
            execute_values(cur, """
                INSERT INTO offers (item_identity_key, supplier_id, supplier_sku, price_cents, currency, stock_qty, updated_at)
                VALUES %s
                ON CONFLICT (item_identity_key, supplier_id, supplier_sku) DO UPDATE SET
                    price_cents = EXCLUDED.price_cents,
                    stock_qty = EXCLUDED.stock_qty,
                    updated_at = NOW()
            """, batch)
            conn.commit()


def main():
    if len(sys.argv) < 2:
        print("usage: apply_to_postgres_v2.py <good.ndjson>")
        sys.exit(1)
    
    good_path = Path(sys.argv[1])
    if not good_path.exists():
        print(f"❌ {good_path} not found")
        sys.exit(1)
    
    conn = psycopg2.connect(DB_URL)
    load_good_to_db(good_path, conn)
    conn.close()
    print(f"✅ Loaded {good_path} to PostgreSQL")


if __name__ == "__main__":
    main()
