CURATED V1.1
Назначение: потребитель SSOT сегмента (facts/<...>/<run_id>.ndjson). Делает бизнес-отбор “продаваемых” строк поверх валидных фактов (GOOD), не меняя сами факты.
Вход: manifest.json (ssot/manifests/<run_id>.json) с paths.segment
Выходы (curated_v1/out/<run_id>/):
- curated.ndjson: только строки, где parsed.price>0 и parsed.qty>0 (то есть годно для витрины/офферов)
- dropped_samples.ndjson: диагностические примеры “не прошло бизнес-фильтр” (по умолчанию до 50 строк, выключается --max-dropped-samples 0)
- curated.stats.json: счётчики и drop_counts (по причинам), sha256 входов
Ключевая идея: “BAD” живёт только в emitter bad_rows.ndjson. Здесь нет BAD, тут только DROP (валидно, но не продаётся).
drop_reason:
- price_missing (price is null)
- price_nonpositive (price<=0)
- qty_missing (qty is null)
- qty_nonpositive (qty<=0)
- комбинации через _and_
CLI:
--manifest PATH (обяз.)
--out-dir DIR (по умолчанию curated_v1/out)
--max-dropped-samples N (по умолчанию 50)
