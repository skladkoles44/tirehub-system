# Паспорт поставщика: Шинсервис (shinservice.ru)

**Версия:** 3.0
**Дата обновления:** 2026-05-13
**Статус:** ✅ Интеграция завершена, ETL в production

---

## 1. Общая информация

| Поле | Значение |
|------|----------|
| Поставщик | ООО «ШИНСЕРВИС» |
| Сайт | https://www.shinservice.ru |
| B2B-портал | https://duplo.shinservice.ru |
| Тип API | REST (выгрузки по прямым ссылкам) |
| Версия API | 2.9.1 |
| Версия ETL | 9.0 |

---

## 2. Аутентификация

| Поле | Значение |
|------|----------|
| Тип | Bearer token |
| Способ | Заголовок Authorization |
| Токен в .env | /opt/canonical-core/.env.shinservice |

---

## 3. Endpoints

### Базовый URL REST API
https://duplo-api.shinservice.ru/api/v1/exporter

### Типы выгрузок

| Тип | Параметр | Содержание |
|-----|----------|------------|
| Каталог | type=catalog | sku, brand, model, gtin, характеристики, фото |
| Цены | type=price | sku, price, price_retail, price_margin, price_discount |
| Остатки | type=stock | sku, store_id, stock_total, stock_local |

### Форматы
- JSON (рекомендован)
- CSV
- XML
- XLSX

### Шаблон URL
{BASE}/{UUID}/download?type={catalog|price|stock}&format={json}

---

## 4. Ключи доступа

| Назначение | UUID |
|------------|------|
| Шины (все) | 019dbb42-9e14-b7d0-a829-b64101ead29f |
| Диски (все) | 019dbb40-9828-be33-9728-e5d7db368ca6 |

---

## 5. Структура данных

### 5.1 Каталог (type=catalog) — массив объектов

**Пример:**
{
  "type": "tyre",
  "sku": "1015976",
  "brand_sku": "3994100",
  "brand": "Pirelli",
  "model": "Ice Zero FR",
  "params": {
    "gtin": "08019227399417",
    "season": "W",
    "width": 225,
    "profile": 65,
    "diameter": 17,
    "load_index": "106",
    "speed_index": "T",
    "pins": false,
    "runflat": false,
    "extra_load": true
  },
  "image": "https://..."
}

| Поле | Тип | Описание |
|------|-----|----------|
| sku | string | Код товара |
| brand | string | Бренд |
| model | string | Модель |
| params.gtin | string | GTIN (штрихкод) |
| params.season | string | Сезон (W/S) |
| params.width | integer | Ширина |
| params.profile | integer | Профиль |
| params.diameter | integer | Диаметр |
| params.load_index | string | Индекс нагрузки |
| params.speed_index | string | Индекс скорости |
| params.pins | boolean | Шипы |
| params.runflat | boolean | RunFlat |
| params.extra_load | boolean | Extra Load |
| image | string | Ссылка на фото |

### 5.2 Цены (type=price) — массив объектов

{
  "sku": "1015976",
  "price": 9384,
  "price_retail": 11550,
  "price_margin": null,
  "price_discount": null
}

| Поле | Тип | Описание |
|------|-----|----------|
| sku | string | Код товара |
| price | integer | Цена B2B (руб) |
| price_retail | integer | Розничная цена |
| price_margin | integer | Маржа |
| price_discount | integer | Скидка |

### 5.3 Остатки (type=stock) — массив объектов

{
  "sku": "1015976",
  "store_id": 610,
  "stock_local": 0,
  "stock_total": 1,
  "transition_cost_min": 0,
  "transition_cost_max": 0
}

| Поле | Тип | Описание |
|------|-----|----------|
| sku | string | Код товара |
| store_id | integer | ID склада |
| stock_total | integer | Общий остаток |
| stock_local | integer | Локальный остаток |

### 5.4 Склады (из stock API)

| Поле | Тип | Описание |
|------|-----|----------|
| shop_id | integer | ID склада |
| title | string | Название (генерируется как Shop {id}) |

⚠️ API не отдаёт названия складов и адреса. Названия генерируются ETL.

---

## 6. Инфраструктура на VPS

| Компонент | Путь |
|-----------|------|
| ETL-скрипт | /opt/canonical-core/scripts/etl/shinservice_etl.py |
| VENV | /opt/canonical-core/.venvs/shinservice/ |
| Cron | /etc/cron.d/shinservice-etl |
| Логи ETL | /var/log/shinservice_etl.log |
| Логи Cron | /var/log/shinservice_cron.log |
| Logrotate | /etc/logrotate.d/shinservice |
| Конфиг | /opt/canonical-core/.env.shinservice |

---

## 7. Таблицы PostgreSQL

| Таблица | Назначение | Записей |
|---------|------------|---------|
| _shinservice_products | Каталог товаров | 15 942 |
| _shinservice_offers | Остатки (shop_id>0) + Цены (shop_id=0) | 212 000+ |
| _shinservice_shops | Справочник складов | 14 |
| _shinservice_etl_runs | История запусков ETL | 4 |
| _shinservice_etl_errors | Dead-letter queue | 0 |

**Размеры:**
- _shinservice_offers: 134 MB
- _shinservice_products: 23 MB

---

## 8. Cron расписание

| Режим | Расписание |
|-------|------------|
| stock (остатки) | Каждые 30 минут |
| full (каталог + цены + остатки) | Ежедневно в 03:30 |

---

## 9. Logrotate

файлы: /var/log/shinservice_etl.log и /var/log/shinservice_cron.log
период: daily
хранение: 7 дней
сжатие: gzip

---

## 10. Статус интеграции

| Этап | Статус | Дата |
|------|--------|------|
| R0 — разведка API | ✅ | 2026-05-12 |
| R1 — подтверждение | ✅ | 2026-05-13 |
| Таблицы PostgreSQL | ✅ | 2026-05-13 |
| ETL-скрипт v9.0 | ✅ | 2026-05-13 |
| Cron | ✅ | 2026-05-13 |
| Logrotate | ✅ | 2026-05-13 |
| Последний запуск full | ✅ success | 2026-05-13 18:01 |

**Последний запуск:**
- run_id: 239c673a
- records_processed: 227 936
- records_failed: 0

---

## 11. Известные ограничения

| Ограничение | Описание |
|-------------|----------|
| Названия складов | API не отдаёт, генерируются как Shop {id} |
| Адреса складов | Отсутствуют |
| GTIN | Есть только у шин (в params.gtin) |
| Токен | Требуется для доступа к API |

---

## 12. Контакты поддержки

| Канал | Контакт |
|-------|---------|
| B2B-портал | https://duplo.shinservice.ru |
| Email | b2b_yaroslavl@shinservice.ru |
| Телефон | +7 4852 58-04-68 |

---

## 13. Владельцы

| Роль | Кто |
|------|-----|
| GitHub репозиторий | skladkoles44 |
| VPS | root |
| База данных | postgres |

---

**Документ поддерживается в актуальном состоянии.**
**Версия 3.0 — соответствует production.**
