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
