ETL CANON. этап ingestion

1. Назначение

Компонент ingestion предназначен для:

- приёма входных файлов
- технической проверки их структуры и целостности
- построчной нормализации и классификации строк
- записи фактов
- публикации актуального технического состояния (state)

Документ описывает только ingestion-контур.
Бизнес-логика и прикладные сервисы сюда не входят.

State — единственный публикуемый слой для downstream-потребителей.
Run-артефакты, quarantine и manifest — операционный след с ограниченным сроком хранения.


2. Модель данных

2.1 FULL SNAPSHOT

Каждый входной файл обязан представлять полный актуальный срез данных (FULL SNAPSHOT).

Это означает:

- файл описывает всё текущее состояние
- если SKU отсутствует в файле → остаток считается 0
- дельта-файлы запрещены

Нарушение этого правила считается нарушением контракта поставки данных.


2.2 Replace State Model

Только успешный PASS-run публикует новое состояние.

Публикация происходит как полная замена предыдущего состояния для данного supplier.

Промежуточные снапшоты не сохраняются как активные.


3. Структура каталогов (Production)

etl_data/raw_v1/
  inbox/<supplier>/
  quarantine/<supplier>/
    duplicates/
    broken/
  archive/<supplier>/
    obsolete/
  accepted_runs/<supplier>/<sha_prefix2>/<sha256>.json
  runs/<run_id>/
    facts.ndjson
    bad.ndjson
    stats.json
    manifest.json
    diag/                    # опционально, retention 3–7 дней
  state_store/<supplier>/<run_id>/
    facts.ndjson
  state/current/<supplier>              # POSIX: symlink
  state/current/<supplier>.json         # Object storage pointer

DQ-конфиг:
etl_ops/config/<supplier>.yaml


4. Inbox Model

4.1 Выбор файла

Алгоритм:

1. Получить список файлов из inbox/<supplier>/
2. Отсортировать по mtime desc
3. Вычислить SHA256 для каждого файла
4. Файлы, SHA которых уже присутствует в accepted_runs → переместить в quarantine/<supplier>/duplicates/
5. Среди оставшихся выбрать самый свежий уникальный файл
6. Все остальные уникальные файлы этого supplier переместить в archive/<supplier>/obsolete/
7. Обработать только выбранный файл

Inbox должен оставаться чистым.

mtime используется только как эвристика порядка.
Истинная хронология определяется run_id.

Если в inbox одновременно присутствуют несколько уникальных файлов одного supplier, обрабатывается только самый свежий.
Промежуточные снапшоты сознательно пропускаются и архивируются как obsolete в рамках Replace State Model.


4.2 File Stability Rule

Файл считается готовым к обработке, если:

- его размер не менялся за последние file_stability_window секунд (default 30)
  или
- используется атомарный rename (.tmp → финальное имя)

Цель — предотвратить чтение недокачанного файла.


5. Lock per supplier

Оркестрация обязана обеспечивать взаимоисключение запусков для одного supplier.

Параллельные run запрещены.


6. run_id

Формат:

<supplier>_<YYYYMMDDTHHMMSSZ>_<rand6>

Требования:

- UTC, обязательно суффикс Z
- rand6 — 6 hex-символов [0-9a-f]
- уникальность гарантируется генератором


7. Gate (технический + schema check)

FAIL если:

- файл отсутствует
- размер == 0
- файл не читается
- формат не распознан
- обязательные колонки отсутствуют
- header не соответствует ожидаемой структуре

При FAIL:

- файл перемещается в quarantine/<supplier>/broken/
- имя сохраняется (допустимо добавление суффикса .failed)
- state не публикуется
- создаётся manifest со status=FAIL

Gate не содержит бизнес-логики.


8. Data Quality (DQ)

Конфиг per-supplier обязателен:

- min_fact_count
- max_bad_ratio
- allow_empty_snapshot: true|false

Формула:

bad_ratio = bad_rows / (good_rows + bad_rows)

Если конфиг отсутствует → FAIL (CONFIG_DQ_MISSING).

Если allow_empty_snapshot = true → snapshot с 0 facts допустим.
Иначе 0 facts → FAIL.

Рекомендуемые значения:

- min_fact_count ≥ 1
- max_bad_ratio ≤ 0.10


9. Run-артефакты

Хранятся в runs/<run_id>/

9.1 facts.ndjson

Каждая строка обязана содержать:

- run_id
- supplier
- payload

При multi-category:

- category_key

Допускаются:

- row_id
- row_number

Пример:

{
  "run_id": "brinex_20260213T194500Z_1a2b3c",
  "supplier": "brinex",
  "category_key": "sheet14",
  "row_id": "a94a8fe5...",
  "payload": {
    "sku": "ABC123",
    "qty": 5,
    "price_minor": 10000
  }
}

Файл facts.ndjson является самодостаточным.

Рекомендуется нормализовать денежные значения в payload до:
- integer в minor units (копейки, поле price_minor)
- либо строки фиксированной точности ("100.00")

Использование float не рекомендуется из-за потенциальной потери точности при сериализации.


9.2 row_id

row_id = sha256(canonical_json_string(payload))

run_id в вычисление не входит.

canonical_json_string:

- ключи сортируются
- JSON сериализуется без пробелов
- null-поля сохраняются для сохранения структуры данных
- сериализация должна быть детерминированной


9.3 bad.ndjson

Содержит:

- run_id
- supplier
- row_number
- reason_code

reason_code берётся из docs/etl/reason_codes.yaml.


9.4 stats.json

Минимальный контракт:

{
  "total_rows": 1000,
  "good_rows": 950,
  "bad_rows": 50,
  "bad_ratio": 0.05
}

Допустим breakdown по category и reason.


10. Manifest

Обязательные поля:

- run_id
- supplier
- input_filename
- input_sha256
- started_at
- finished_at
- good_rows
- bad_rows
- status (PASS|FAIL)
- code_commit

manifest хранится как операционный след.


11. accepted_runs

Путь:

accepted_runs/<supplier>/<sha_prefix2>/<sha256>.json

где sha_prefix2 — первые 2 hex-символа SHA256.

Создаётся только при PASS.

Минимальный контракт:

{
  "run_id": "...",
  "accepted_at": "ISO8601",
  "input_filename": "...",
  "manifest_path": "..."
}

Retention: 180–365 дней.


12. State

Immutable слой:

state_store/<supplier>/<run_id>/facts.ndjson

Содержимое state_store никогда не изменяется после публикации.
Любые изменения (в том числе ручные) запрещены.


12.1 Публикация state

POSIX → переключение symlink
Object storage → атомарный pointer-object


12.2 State Retention Safety

state_store/<supplier>/<run_id> хранится минимум state_retention_hours
после переключения current
(default 12 часов, configurable).


13. Retention

Удаляются:

- quarantine/*
- archive/*
- obsolete/*
- старые runs/*
- diag/ (3–7 дней)

Не удаляются:

- активный state
- accepted_runs до истечения retention


14. Мониторинг

Рекомендуется:

- monitor age(state/current/<supplier>)
- alert при N подряд одинаковых SHA (drift detection)
- monitor repeated FAIL

Force-publish запрещён.


15. Idempotency

Повторная обработка того же файла невозможна благодаря accepted_runs.

Если run прерван:

- state не переключается
- возможен новый run


16. Границы ответственности

Extractor → чтение
Emitter → GOOD/BAD
Gate → технический + schema check
DQ → контроль качества
Ingestion → orchestrator


17. Запрещено

- дельта-файлы
- запуск без DQ-конфига
- параллельные run одного supplier
- прямое чтение state_store
- перезапись immutable state
- свободный текст вместо reason_code
