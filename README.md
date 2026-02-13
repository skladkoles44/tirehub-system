# tirehub-system

ETL-система для приёма прайс-листов поставщиков и построения витрины поверх проверенных данных.

Ключевая модель:  
**SSOT (append-only journal) + NDJSON-first**  
Бизнес-логика допускается только в слое Curated / Offers.

---

# Canon

- **ETL CANON V1 (SSOT / Marketplace-ready):** `docs/etl/ETL_CANON_V1.md`
- **Architecture lock v1:** `ARCHITECTURE_v1_LOCK.md`

Canon — источник истины по архитектуре и контрактам.

---

# Alignment Plan

- **Canon Alignment Plan v1:** `docs/etl/PLAN_CANON_ALIGNMENT_V1.md`

План приведения реализации к ETL Canon V1.  
Содержит milestones, definition-of-done и критерии соответствия.

---

# Pipeline Model

Extractor → Emitter → Gate → Ingestion → Curated / Offers

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
Status
Canon v1 active
Architecture v1 locked
Alignment in progress

## Test Environment

- docs/etl/ETL_CANON_TEST.md — Test environment contract (branch: test)


Docs

- docs/etl/ETL_CANON.md — Production Canon


Archive

- docs/etl/archive/ETL_CANON_V1.md
- docs/etl/archive/ETL_CANON_V1_QA.md
