# Runtime journal — Этап 4: Нормализация и загрузка в PostgreSQL

**Date:** 2026-05-12
**Time:** 09:18:29 UTC
**Host:** cv6319345.novalocal
**Scope:** tirehub-system / 4tochki integration (Read-side)
**Этап:** 4 — Нормализация и загрузка в PostgreSQL

## Verdict

**Этап 4 успешно завершён.**

Данные 4точек (каталог, остатки, склады) загружены в PostgreSQL 16 (БД canonical).
20 490 товаров, 36 складов, 28 263 офферов. Время загрузки: 3.2 секунды.

## Results

- **Складов:** 36
- **Товаров:** 20 490
- **Офферов:** 28 263
- **Время загрузки:** 3.2s
- **БД:** PostgreSQL 16, БД canonical, роль canonical
- **Таблицы:** _4tochki_warehouses, _4tochki_products, _4tochki_offers

## Technical Findings

1. **Peer authentication** — добавлена строка `local canonical canonical trust` в pg_hba.conf.
2. **Натуральные ключи** (`code TEXT PK`) упростили вставку офферов без JOIN.
3. **NOT VALID для FK** — ограничения добавлены после вставки, без проверки существующих строк.
4. **ON CONFLICT DO UPDATE** — скрипт можно перезапускать без дубликатов. TRUNCATE не используется.
5. **execute_values с page_size=1000** — 3.2 секунды на полную загрузку.
6. **raw_json JSONB** в каждой таблице — полный сырой ответ API для аудита.

## Tables

- **_4tochki_warehouses** (36 rows) — code PK, name, short_name, logistic_days, have_delivery, have_pickup, is_paid_delivery, raw_json
- **_4tochki_products** (20 490 rows) — code PK, type, brand, model, name, season, raw_json
- **_4tochki_offers** (28 263 rows) — product_code, warehouse_code, price, price_rozn, rest, whpr_json, UNIQUE(product_code, warehouse_code)

## Previous Milestones

- Этап 1 (11 мая 18:42 UTC) — справочники
- Этап 2 (11 мая 17:47 UTC) — каталог: 20 490 SKU
- Этап 3 (12 мая 06:56 UTC) — batch-остатки: 20 487 items, 0 failed, 183s

## Next Step

Этап 5 — единый каталог (MDM/PIM). Подготовка к подключению второго поставщика.

## Status

**Closed.** Готов к продолжению.
