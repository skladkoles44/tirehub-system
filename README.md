# tirehub-system

ETL-система для приёма прайс-листов поставщиков и построения витрины поверх проверенных данных.

Ключевая модель:  
**SSOT (append-only journal) + NDJSON-first**  
Бизнес-логика допускается только в слое Curated / Offers.

---

# Canon

**ETL CANON V1 (SSOT / Marketplace-ready)**  
`docs/etl/ETL_CANON_V1.md`

Canon — это источник истины по архитектуре, контрактам и границам ответственности.

---

# Pipeline Model

Extractor → Emitter → Gate → Ingestion → Curated / Offers

### Responsibility boundaries

- Extractor — извлекает строки
- Emitter — принимает решение GOOD/BAD
- Gate — принимает решение PASS/WARN/FAIL по файлу
- Ingestion — append-only фиксация фактов
- Curated / Offers — единственное место бизнес-логики

---

# Ingestion Model (SSOT)

- NDJSON-first
- Каждая строка — атомарный факт
- Append-only
- Дубликаты допустимы
- История не переписывается

Architecture lock: `ARCHITECTURE_v1_LOCK.md`

---

# NDJSON (GOOD fact)

Минимальные поля:

supplier_id  
parser_id  
mapping_version  
ndjson_contract_version  
emitter_version  
run_id  
effective_at  
raw  
parsed  
quality_flags  
_meta  

parsed.price — int (копейки) или null  
parsed.qty — int или null  

---

# Run (example: Kolobox)

VPS (etl):

```bash
scripts/run_kolobox_full_v1.sh
Pipeline: emitter → gate → ingest → curated
Repo Layout
docs/ — спецификации и канон
mappings/ — mapping-файлы
inputs/ — локальные входные файлы (не коммитить)
out/ — локальные артефакты (не коммитить)
Versioning Rules (critical)
Любой breaking change → новый parser_id
Любое изменение mapping → новый mapping_version
Breaking NDJSON contract → новый ndjson_contract_version + новый parser_id
Status
Canon v1 active
Architecture v1 locked
EOF sed -n '1,260p' README.md | termux-clipboard-set
