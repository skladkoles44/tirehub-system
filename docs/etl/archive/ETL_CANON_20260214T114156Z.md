# ETL CANON (Production)

Документ описывает обязательный контракт прод-системы ETL.
Любое отклонение считается нарушением канона.

---

## 1. Принципы

1. SSOT — единственный источник истины.
2. SSOT append-only.
3. run_id генерируется автоматически.
4. filename — метаданные, не идентификатор.
5. Gate не содержит бизнес-логики.
6. Data Quality обязательна.
7. Manifest в SSOT — единственная authoritative версия.
8. Retention управляется политикой.
9. Disk Pressure не может уничтожать SSOT.

---

## 2. Структура каталогов (prod)

/home/etl/

repo/tirehub-system

etl_data/
  raw_v1/
    inbox/<supplier>/
    quarantine/<supplier>/
      duplicates/
      failed_gate/
    archive/
    ssot/
      runs/<run_id>/
      accepted_runs/
      locks/
      manifests/

  curated_v1/

etl_ops/
  runs/<run_id>/
  diag/
  tmp/
  config/

---

## 3. Inbox Model

### 3.1 Разрешено

- ≥1 файлов
- одинаковые имена
- произвольное время поступления

### 3.2 Выбор файла

Алгоритм:

1. Берём все файлы supplier.
2. Сортировка: mtime desc.
3. Проверка SHA256.
4. Если (supplier, sha256) уже в accepted_runs:
   - событие DUPLICATE_INPUT
   - файл → quarantine/<supplier>/duplicates/
   - продолжить цикл.
5. Первый уникальный файл → selected_file.
6. Остальные остаются в inbox.

Важно: обрабатывается один файл за запуск.

---

## 4. run_id

Формат:

<supplier>_<YYYYMMDDTHHMMSSZ>

- UTC
- обязательно Z
- генерируется системой
- ручное формирование запрещено

---

## 5. Gate (технический)

Gate проверяет:

- файл существует
- размер > 0
- читаемость
- корректность формата
- checksum вычисляется

Если пустой файл → FAIL.

Gate не содержит бизнес-правил.

---

## 6. Emit

Результаты:

- facts.ndjson
- bad.ndjson
- run.manifest.json
- run.stats.json

### 6.1 Форматы

fact (минимум)

{
"run_id": "...",
"supplier": "...",
"row_id": "...",
"payload": {...}
}

bad

{
"run_id": "...",
"supplier": "...",
"row_number": 123,
"reason": "..."
}

manifest

{
"run_id": "...",
"supplier": "...",
"input_sha256": "...",
"input_filename": "...",
"started_at": "...",
"finished_at": "...",
"fact_count": 12345,
"bad_count": 34,
"status": "PASS|FAIL"
}

Manifest хранится только в:

etl_data/raw_v1/ssot/runs/<run_id>/run.manifest.json

Это единственная authoritative версия.

etl_ops/runs/<run_id>/ может содержать symlink.

---

## 7. SSOT

Append-only.

Запрещено:

- удалять runs
- изменять manifest
- перезаписывать facts

Индекс дедупликации:

accepted_runs/
<supplier>_<sha256>.json

---

## 8. Data Quality (обязательно для prod)

DQ выполняется в каждом run.

Обязательные пороги:

- max_bad_ratio
- min_fact_count
- max_fact_count_delta

Если пороги не заданы → FAIL конфигурации.
Если превышены → FAIL run.

empty_file не является метрикой DQ — это FAIL в Gate.

---

## 9. Retention

### 9.1 Никогда не удаляются

- ssot/
- accepted_runs/

### 9.2 Удаляются автоматически

Класс — Retention

- archive — configurable (default 90d)
- quarantine — 30d
- etl_ops/runs — configurable (default 180d)
- diag — 30d
- tmp — 7d

Quarantine имеет retention.
Forensic hold допускается только вручную.

---

## 10. Disk Pressure Policy

Если свободное место < threshold:

Порядок удаления:

1. archive
2. quarantine
3. diag
4. etl_ops/tmp
5. etl_ops/runs

SSOT — никогда.

---

## 11. tmp

- используется только во время run
- очищается после run
- retention 7 дней
- участвует в Disk Pressure после diag

---

## 12. Оркестрация

- Один entrypoint.
- Ручной запуск допустим.
- run_id генерируется системой.
- Параллельные run для одного supplier запрещены.

---

## 13. Конфигурация

etl_ops/config/

- вне repo
- chmod 600
- owner etl
- содержит DQ thresholds
- secrets запрещены в коде

---

## 14. Запрещено

- использовать filename как идентификатор
- читать manifest из etl_ops как источник истины
- изменять SSOT
- запускать без DQ-порогов
- хранить секреты в repo

---

## 15. Итог

Система:

- не перезапускает дубликаты
- не теряет SSOT
- контролирует качество данных
- управляет диском
- не допускает двойной истины
