# 2026-03-18 — normalizer_v1 (L1)

## Слой
L1 Normalization (atomic → canonical)

## Что реализовано
- вход: NDJSON (atomic_rows)
- выход:
  - good.ndjson (canonical товары)
  - reject.ndjson (отбракованные строки)
  - manifest.json (метрики и статистика)

## Основные свойства
- чистый слой (нет env / путей / IO-логики вне аргументов)
- CLI:
  - --input
  - --out-dir
  - --reject-mode (full/minimal)
- canonical normalization:
  - brand / season / studded
  - size (width/height/diameter)
- collapse:
  - grouping по canonical_key
- dedup:
  - stocks по (warehouse, qty, price, oem, source_sku)
- reject pipeline:
  - reason-based (width_range, no_brand, etc.)
  - original_line_no для трассировки
- manifest:
  - rows_total / rows_nonempty / rows_parsed_ok
  - grouped_keys / good / reject
  - stats (reject reasons)
  - runtime

## Инженерные решения
- canonical_key стабилизирован (studded = 0/1)
- deterministic output:
  - сортировка grouped keys
  - сортировка stocks
- JSON ошибки не валят процесс
- lineage сохраняется (sample)

## Проверка
- py_compile: OK
- smoke test: OK
  - dedup работает
  - reject работает
  - manifest корректен

## Вывод
Normalizer v1 зафиксирован как:
→ чистый, минимальный, рабочий L1 слой

## Следующий этап
- прогон на реальных atomic_rows
- анализ:
  - false rejects
  - слабые нормализации (model, price, qty)
- подготовка к L2 (entity resolution / catalog layer)
