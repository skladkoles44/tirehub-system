-- Таблица товаров
CREATE TABLE IF NOT EXISTS _shinservice_products (
    sku TEXT PRIMARY KEY,
    title TEXT,
    brand TEXT,
    model TEXT,
    gtin TEXT,
    season TEXT,
    diameter TEXT,
    width INTEGER,
    profile INTEGER,
    load_index TEXT,
    speed_index TEXT,
    pins BOOLEAN,
    runflat BOOLEAN,
    extra_load BOOLEAN,
    photo_url TEXT,
    raw_json JSONB,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица остатков и цен
CREATE TABLE IF NOT EXISTS _shinservice_offers (
    id BIGSERIAL PRIMARY KEY,
    sku TEXT NOT NULL,
    shop_id INTEGER NOT NULL,
    price INTEGER,
    price_retail INTEGER,
    price_msrp INTEGER,
    stock INTEGER,
    raw_json JSONB,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(sku, shop_id)
);

-- Таблица складов
CREATE TABLE IF NOT EXISTS _shinservice_shops (
    shop_id INTEGER PRIMARY KEY,
    title TEXT,
    address TEXT,
    has_pickup BOOLEAN,
    has_delivery BOOLEAN,
    raw_json JSONB,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индексы
CREATE INDEX IF NOT EXISTS idx_offers_sku ON _shinservice_offers(sku);
CREATE INDEX IF NOT EXISTS idx_offers_shop_id ON _shinservice_offers(shop_id);
