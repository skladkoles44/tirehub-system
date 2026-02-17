-- Serving schema v1 (MVP aggregator offers)
-- Idempotent: safe to run multiple times

CREATE TABLE IF NOT EXISTS master_products (
  internal_sku        TEXT PRIMARY KEY,
  base_sku            TEXT NOT NULL,
  brand               TEXT NOT NULL,
  model               TEXT,
  width               INT  NOT NULL,
  height              INT  NOT NULL,
  diameter            INT  NOT NULL,
  season              TEXT NOT NULL,
  load_index          TEXT,
  speed_index         TEXT,
  runflat             BOOLEAN,
  studded             BOOLEAN,

  source_supplier_id  TEXT,
  first_seen_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  is_manual           BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_master_products_base_sku
  ON master_products USING btree (base_sku);

CREATE TABLE IF NOT EXISTS supplier_sku_map (
  supplier_id   TEXT NOT NULL,
  supplier_sku  TEXT NOT NULL,
  internal_sku  TEXT NOT NULL REFERENCES master_products(internal_sku),
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (supplier_id, supplier_sku)
);

CREATE TABLE IF NOT EXISTS supplier_offers_latest (
  supplier_id     TEXT NOT NULL,
  supplier_sku    TEXT NOT NULL,
  internal_sku    TEXT NOT NULL REFERENCES master_products(internal_sku),

  qty             INT,
  price_purchase  NUMERIC(14, 4),
  currency        TEXT,
  updated_at      TIMESTAMPTZ,

  last_applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  PRIMARY KEY (supplier_id, supplier_sku),
  FOREIGN KEY (supplier_id, supplier_sku)
    REFERENCES supplier_sku_map(supplier_id, supplier_sku)
);

CREATE INDEX IF NOT EXISTS idx_offers_latest_internal_sku
  ON supplier_offers_latest USING btree (internal_sku);

CREATE INDEX IF NOT EXISTS idx_offers_latest_qty_gt_zero
  ON supplier_offers_latest (internal_sku) WHERE qty > 0;

CREATE INDEX IF NOT EXISTS idx_offers_latest_price
  ON supplier_offers_latest (price_purchase);

CREATE TABLE IF NOT EXISTS schema_migrations (
  version     TEXT PRIMARY KEY,
  applied_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  applied_by  TEXT
);

INSERT INTO schema_migrations (version, applied_by)
VALUES ('serving_v1', current_user)
ON CONFLICT (version) DO UPDATE SET
  applied_at = now(),
  applied_by = current_user;
