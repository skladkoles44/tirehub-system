# Паспорт поставщика: 4точки (4tochki.ru)

**Версия:** 1.3
**Дата обновления:** 2026-05-16
**Статус:** Production

## 1. Общая информация

| Поле | Значение |
|------|----------|
| Поставщик | 4точки (4tochki.ru) |
| Сайт | https://www.4tochki.ru |
| Тип API | SOAP (WCF) |
| WSDL | https://api-b2b.4tochki.ru/WCF/ClientService.svc?wsdl |
| Используется методов | 10 read |

## 2. Аутентификация

| Поле | Значение |
|------|----------|
| Тип | Логин/пароль в каждом запросе |
| Переменные в .env | FTO_LOGIN, FTO_PASSWORD |
| Файл | /opt/canonical-core/.env.4tochki |

## 3. Ключевые цифры (16 мая 2026)

| Показатель | Значение |
|------------|----------|
| Типов товаров | 6 |
| Всего товаров | ~21 000 |
| Складов | 36 |
| Офферов | ~28 500 |
| Время stock | ~75-85s |
| Время full | ~4-5 мин |

## 4. ETL v3.5.3

| Поле | Значение |
|------|----------|
| Файл | /opt/canonical-core/scripts/etl/4tochki_etl.py |
| Режимы | stock (каждые 30 мин), full (раз в сутки 03:00) |
| Логи | /var/log/4tochki_etl_{mode}.log (JSON, run_id) |
| Особенности | orjson, parallel catalog (6 workers), sequential streaming stock, pg_notify, partial indexes, graceful shutdown |

## 5. Таблицы PostgreSQL

| Таблица | Назначение |
|---------|-----------|
| _4tochki_warehouses | Склады (36) |
| _4tochki_products | Каталог (is_active) |
| _4tochki_offers | Остатки и цены (is_active) |

## 6. Статус

Production. ETL v3.5.3 задеплоен.
