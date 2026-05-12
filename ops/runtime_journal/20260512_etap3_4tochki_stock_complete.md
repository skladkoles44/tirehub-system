# Runtime journal — Этап 3: Batch-остатки 4точки

**Date:** 2026-05-12
**Time:** 07:22:29 UTC
**Host:** cv6319345.novalocal
**Scope:** tirehub-system / 4tochki integration (Read-side)
**Этап:** 3 — Batch-остатки (GetGoodsPriceRestByCode)

## Verdict

**Этап 3 успешно завершён.**

Остатки и цены для всех 20 490 товаров собраны через GetGoodsPriceRestByCode.
0 упавших чанков из 410. Данные готовы к загрузке в БД.

## Results

- **Всего уникальных кодов:** 20 490
- **Размер чанка:** 50 кодов
- **Всего чанков:** 410
- **Товаров с остатками:** 20 487
- **Упавших чанков:** 0
- **Время выполнения:** 183 секунды (3 минуты)
- **Среднее время на чанк:** ~0.4 сек

## Структура stock_chunk файла

```json
{
  "error": { "code": null, "comment": null },
  "price_rest_list": {
    "price_rest": [
      {
        "code": "3151008",
        "whpr": {
          "wh_price_rest": [
            { "price": "3942", "price_rozn": "4974", "rest": 1, "wrh": 1046 },
            { "price": "3942", "price_rozn": "4974", "rest": 2, "wrh": 2184 }
          ]
        }
      }
    ]
  }
}
```

## Связь с каталогом (Этап 2)

- all_codes.json (20 490 кодов) → разбит на 410 чанков по 50
- Каждый чанк → GetGoodsPriceRestByCode → stock_chunk_NNNN.json
- 3 товара не вернулись: остаток 0 (API не возвращает товары с нулевым остатком — подтверждено документацией 4точек)

## Данные на VPS

- **Каталог:** /var/tmp/4tochki_catalog/ (all_codes.json + страницы)
- **Остатки:** /var/tmp/4tochki_stock/ (410 stock_chunk файлов + stock_stats.json)
- **Политика:** данные НЕ УДАЛЯТЬ

## Technical Findings

1. **API стабильно:** 410 последовательных запросов без единого сбоя. Retry не понадобились.
2. **Полный цикл — 3 минуты:** можно обновлять остатки каждые 15–30 минут.
3. **Структура ответа:** price_rest_list.price_rest[] — отличается от GetFindTyre (TyrePriceRest[]) и GetFindDisk (DiskPriceRest[]).
4. **Товары с остатком 0 исключаются из ответа** — поведение API подтверждено.

## Next Step

Этап 4 — нормализация и загрузка в БД (SQLite). Данные готовы.

## Status

**Closed.** Готов к продолжению.
