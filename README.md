# tirehub-system. этап ingestion

ETL-компонент для обработки и нормализации входных файлов.

Компонент преобразует исходные данные различных форматов из разных файлов в структурированный слой (SSOT), предназначенный для дальнейшего использования прикладными сервисами.

Текущий фокус — корректная экстракция, техническая валидация и стабильная запись фактов.

## Architecture (ingestion stage)

Extractor → Emitter → Gate → Ingestion → SSOT

## Environments

PHONE / TEST / PROD — одинаковая структура, отличается только ETL_BASE.

## Git

- main — стабильная ветка
- test — рабочая ветка разработки

## Docs (test branch)

- docs/etl/ETL_CANON_TEST.md — Test environment contract

## Archive

- docs/etl/archive/
- docs/etl/archive/ETL_CANON_V1.md
- docs/etl/archive/ETL_CANON_V1_QA.md

---

## MAX Bots Reliability Notes (production)

This project’s MAX Bot1/Bot2 runtime uses a shared SQLite database.

Key invariants (enforced per-connection in both bots):

- journal_mode = WAL
- synchronous = NORMAL
- busy_timeout = 5000 ms
- foreign_keys = ON

Operational verification highlights:

- Broken claim rows were detected earlier and have been fixed (claim_id present while claim_ts missing).
- Dead rows observed were legitimate terminal outcomes (synthetic cleaned rows and a 404 error case after max attempts).

Delivery semantics: at-least-once with bounded retries and dead-lettering.

