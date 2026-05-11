# Runtime journal — Этап 2: Полный каталог 4точки

**Date:** 2026-05-11
**Time:** 22:09:10 MSK
**Host:** cv6319345.novalocal
**Scope:** tirehub-system / 4tochki integration (Read-side)
**Этап:** 2 — Полный каталог товаров (GetFindTyre + GetFindDisk)

## Verdict

**Этап 2 успешно завершён.**

Полный каталог поставщика 4точки собран, версионирован и зафиксирован.
Подготовлена база для Этапа 3 (Batch остатки и цены).

## Results

- **Всего уникальных SKU:** 20 490
- **Товары:** Шины (зима + лето + всесезон) + Диски
- **pageSize:** 50 (pageSize=2000 не работал — API возвращал по 1 товару)
- **Страниц всего:** ~410
- **Время сбора:** ~3 минуты
- **Снепшот:** var/probes/4tochki/catalog/2026-05-11T16-53-27Z/

## Technical Findings

1. **Различия в структуре ответов:**
   - GetFindTyre → price_rest_list.TyrePriceRest[]
   - GetFindDisk → price_rest_list.DiskPriceRest[]
   - GetGoodsPriceRestByCode → price_rest_list.price_rest[]

2. **Один товар = полный набор складов**
   Каждый элемент уже содержит whpr.wh_price_rest[] — массив остатков по всем складам. Джойнить не требуется.

3. **Пагинация** работает корректно через totalPages.

4. **pageSize=2000 не работает** — API возвращал по 1 товару на страницу. pageSize=50 стабильно.

## Files Created

var/probes/4tochki/catalog/2026-05-11T16-53-27Z/
├── tyres_winter_page0.json ... tyres_winter_page45.json
├── tyres_summer_page*.json
├── tyres_all_season_page*.json
├── disks_page*.json
├── all_codes.json (20 490 кодов)
└── catalog_manifest.json (SHA256)

## Architecture Decision (подтверждено)

- Одна БД для всех поставщиков.
- supplier_X_raw.* — сырые данные (1:1 с JSON).
- unified.* — нормализованный слой (продукты, склады, offers, matching).
- Новый поставщик добавляется только через raw-адаптер.

## Next Step

Этап 3 — Batch-запросы GetGoodsPriceRestByCode (410 чанков по 50 кодов).

## Status

**Closed.** Готов к продолжению.
