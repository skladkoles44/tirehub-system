# SERVING STATUS V1 (Test)

Дата фиксации: 2026-02-17  
Ветка: test  
Статус: WORKING (MVP)

---

## 1. Реализовано

### Serving schema v1
Файл:
- sql/serving/serving_schema_v1.sql

Содержит:
- master_products
- supplier_sku_map
- supplier_offers_latest
- schema_migrations
- индексы для агрегатора
- идемпотентность (CREATE IF NOT EXISTS)

### Provision-скрипт
Файл:
- etl_ops/provision/apply_serving_schema_v1.sh

Свойства:
- принимает DB_URL
- ON_ERROR_STOP
- проверка таблиц через pg_tables
- проверка версии serving_v1
- идемпотентный запуск

---

## 2. apply_to_postgres_v1.py

Стабилизировано:

- lock_timeout без bind-параметров
- commit после advisory lock (SQLAlchemy 2 autobegin fix)
- поддержка psycopg
- поддержка --vacuum
- поддержка --log-file
- повторный запуск идемпотентен

---

## 3. Тестирование (Test_etl)

Выполнено:

- схема применена
- apply тестового NDJSON
- повторный запуск без дублей
- WARN по invalid updated_at корректный

Результат:

- master_products: 5
- supplier_offers_latest: 3
- supplier_sku_map: корректно заполнена

---

## 4. Инварианты MVP

- schema как код в репозитории
- схема применяется отдельно от ingestion
- ingestion не создаёт таблицы
- serving-слой изолирован
- повторный apply безопасен

---

## 5. Ограничения (осознанные)

- нет версионированных миграций (только v1)
- нет soft-delete логики
- нет истории изменений (latest only)

---

Статус: можно использовать для тестовой витрины.
