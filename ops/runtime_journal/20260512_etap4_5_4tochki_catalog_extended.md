# Runtime journal — Этап 4.5: Расширение каталога и полный пересбор остатков

**Date:** 2026-05-12
**Time:** 10:04:53 UTC
**Host:** cv6319345.novalocal
**Scope:** tirehub-system / 4tochki integration (Read-side)
**Этап:** 4.5 — Расширение каталога (camera, wheel, fastener, oil)

## Verdict

**Этап 4.5 успешно завершён.**

Каталог расширен с 2 до 6 типов товаров. Добавлено 449 новых товаров.
Выполнен полный пересбор остатков: 20 939 товаров, 28 839 офферов, 0 ошибок.

## Results

- **Типов товаров:** 6 (tyre, disk, camera, wheel, fastener, oil)
- **Товаров:** 20 939 (было 20 490)
- **Офферов:** 28 839 (было 28 263)
- **Складов:** 36
- **Новых товаров:** 449
- **Время пересбора:** 202s
- **Упавших чанков:** 0/419

## New Types

| Тип | Источник | Товаров |
|-----|----------|---------|
| camera | GetFindCamera | 50 |
| wheel | GetFindWheel | 24 |
| fastener | GetFastener | 350 |
| oil | GetOil | 25 |

## Technical Findings

1. **GetPressureSensor и GetConsumable не работают** без фильтров — отложено.
2. **Decimal в wh_price_rest** требует safe_float() и json.dumps(default=str).
3. **set -a не всегда экспортирует** переменные для дочерних Python-процессов — добавлен явный export.
4. **CONSTRAINT обновляется итеративно** — DROP + ADD для каждого нового типа без потери данных.

## Previous Milestones

- Этап 1 (11 мая 18:42 UTC) — справочники
- Этап 2 (11 мая 17:47 UTC) — каталог: 20 490 SKU
- Этап 3 (12 мая 06:56 UTC) — batch-остатки: 20 487 items, 0 failed, 183s
- Этап 4 (12 мая 08:58 UTC) — PostgreSQL: 36 складов, 20 490 товаров, 28 263 офферов

## Next Step

Настройка регулярного обновления остатков (cron + ETL-скрипт).

## Status

**Closed.** Готов к продолжению.
