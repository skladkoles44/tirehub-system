# 2026-03-18 — repo transition archive wave3

## Что сделано
Выполнена третья волна очистки live-code зоны.

## Что убрано из live-zone
- scripts/etl/unknown_header_harvest.py
- scripts/probe/mass_probe_v2.py

## Куда перенесено
- scripts/archive/transition_legacy_2026-03/etl/unknown_header_harvest.py
- scripts/archive/transition_legacy_2026-03/probe/mass_probe_v2.py

## Почему
- unknown_header_harvest.py содержит legacy hardcoded paths
- mass_probe_v2.py завязан на removed scripts/ingestion runner path и supplier-era logic
- оба файла не соответствуют текущему live L0/L1 контуру

## Дополнительно
- пустой directory skeleton .pydeps удалён рекурсивно, где это было возможно

## Результат
Live scripts zone стала чище: removed non-generic legacy utility and broken legacy probe from active contour while preserving code history inside repo archive.
