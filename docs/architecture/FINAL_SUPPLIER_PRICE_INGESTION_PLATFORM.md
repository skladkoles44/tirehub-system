FINAL ARCHITECTURE — SUPPLIER PRICE INGESTION PLATFORM

Цель системы:
любой supplier файл → извлечь структуру → нормализовать данные → устранить дубликаты SKU → сформировать SSOT → обновлять систему инкрементально.

Слои архитектуры:
L0 structural extraction
L1 layout resolution
L2 semantic consolidation
L3 incremental snapshot ingestion

L0 — RUNNER v4.1 (STRUCTURAL EXTRACTION)

Pipeline:
container_reader
sheet_scanner
table_detector
header_detector
merged_cells_propagation
header_flattener
column_classifier
layout_fingerprint
mapping_loader
row_iterator
warehouse_compactor
sanity_validator
good_emitter
reject_emitter
stats_collector
schema_drift_detector
events

Runner output:
1 GOOD строка = 1 строка таблицы.
Warehouses сохраняются compact array.

L1 — LAYOUT RESOLUTION

layout → mapping

Fingerprint:
roles = [sku, brand, price, warehouse...]
signature = join(roles)
fingerprint = sha1(signature)

registry:
config/layout_registry.yaml

unknown layout → LayoutUnknownEvent + samples/layouts/<hash>.json

layout drift detection:
previous_layout_hash
current_layout_hash

L2 — SKU COLLAPSE

collapse key:
(supplier_id, parsed.sku)

pipeline:
read GOOD NDJSON
group by supplier_id + sku
aggregate
emit collapsed row

aggregation:
brand → most_frequent
name → longest
price → max
warehouses → merge
row_index → min
flags → union

L3 — INCREMENTAL SNAPSHOT INGESTION

snapshot model:
snapshot_id
supplier_id
sku
data
processed_at

algorithm:
previous_snapshot
current_snapshot
diff
insert / update / delete

FINAL FLOW

supplier file
↓
Runner v4.1
↓
GOOD NDJSON
↓
SKU collapse
↓
snapshot diff
↓
SSOT update
↓
decomposer
↓
offer schema
↓
serving

System properties:
deterministic extraction
layout auto-detection
compact warehouse storage
SKU deduplication
layout drift detection
incremental ingestion
snapshot versioning
SSOT storage
