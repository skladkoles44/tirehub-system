# Runtime journal — Этап 4.6: ETL-скрипт и Cron

**Date:** 2026-05-12
**Time:** 11:17:46 UTC
**Host:** cv6319345.novalocal
**Scope:** tirehub-system / 4tochki integration (Read-side)
**Этап:** 4.6 — ETL-скрипт + автоматизация (Cron)

## Verdict

**Этап 4.6 успешно завершён.**

Создан ETL-скрипт с двумя режимами (stock/full). Настроен cron для автоматического обновления остатков каждые 30 минут и каталога раз в сутки. Настроена ротация логов.

## Results

- **ETL-скрипт:** /opt/canonical-core/scripts/etl/4tochki_etl.py
- **Режим stock:** каждые 30 минут, ~3 минуты, обновляет _4tochki_offers
- **Режим full:** раз в сутки (03:00), ~5 минут, каталог + остатки
- **Логи:** /var/log/4tochki_etl.log, /var/log/4tochki_etl_full.log
- **Ротация:** 7 дней, сжатие, copytruncate

## Technical Findings

1. SSH-соединение рвётся при долгих операциях — cron решает проблему.
2. set -a; source .env не всегда экспортирует — добавлен явный export.
3. ON CONFLICT DO UPDATE — офферы обновляются без дубликатов.
4. Миграция схемы БД вынесена из ETL — нет DDL-блокировок.

## Previous Milestones

- Этап 1 (11 мая 18:42 UTC) — справочники
- Этап 2 (11 мая 17:47 UTC) — каталог: 20 490 SKU
- Этап 3 (12 мая 06:56 UTC) — batch-остатки
- Этап 4 (12 мая 08:58 UTC) — PostgreSQL: 36 складов, 20 490 товаров, 28 263 офферов
- Этап 4.5 (12 мая 09:58 UTC) — расширение до 6 типов, 20 939 товаров, 28 839 офферов

## Current State

- PostgreSQL: 28 841 офферов, 20 939 товаров, 36 складов, 6 типов
- Cron: stock каждые 30 мин, full раз в сутки
- Логи: /var/log/4tochki_etl.log (ротация 7 дней)

## Next Step

Этап 5 — единый каталог (MDM/PIM) при подключении второго поставщика.

## Status

**Closed.** Система в production-режиме.
