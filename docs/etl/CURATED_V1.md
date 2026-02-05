CURATED V1 (WBP)
Назначение: первый потребитель SSOT. Строит витрину curated поверх facts-segment по run_id без вмешательства в SSOT.
Вход:
- SSOT segment: ssot/facts/{supplier_id}/{parser_id}/{YYYY-MM}/{run_id}.ndjson
- SSOT manifest: ssot/manifests/{run_id}.json
Выход:
- curated_v1/out/{run_id}/curated.ndjson
- curated_v1/out/{run_id}/curated.stats.json
- curated_v1/out/{run_id}/stderr.log
Правило включения (P0): parsed.price > 0 AND parsed.qty > 0.
Детерминизм: при одинаковых входах и run_id вывод байт-в-байт идентичен (JSON sort_keys, separators, LF, порядок как в segment).
