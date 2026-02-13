ETL CANON V1 (SSOT / Marketplace-ready)

Назначение  
Контур предназначен для приёма прайс-листов поставщиков, фиксации фактов «что поставщик прислал» без бизнес-эвристик и построения витрины только поверх проверенных и валидных данных.  
SSOT является журналом фактов, а не «актуальным состоянием».

Общие принципы  
Система построена по модели: Extractor → Emitter → Gate → Ingestion → Curated / Offers.  
NDJSON-first. Каждая строка-факт атомарна и независима.

Extractor извлекает строки из входного файла и ничего не решает.  
Emitter решает судьбу каждой строки.  
Gate решает судьбу файла как события.  
Ingestion фиксирует историю навсегда.  
Curated / Offers — единственное место бизнес-логики.

Термины  
parser_id — идентификатор логики парсинга: поставщик + формат/лейаут + реализация emitter.  
Любой breaking change алгоритма или контракта = новый parser_id.  

mapping_schema_version — версия схемы mapping.yaml (ключ version в YAML).  
mapping_version — версия конкретного mapping-файла как артефакта.  

ndjson_contract_version — версия структуры NDJSON.  
Breaking change требует нового ndjson_contract_version и нового parser_id.  

effective_at — момент актуальности данных, задаётся оркестратором.  
run_id — идентификатор одного прогона (один входной файл).  

SSOT — append-only журнал фактов.

Extractor  
Extractor открывает входной файл, находит таблицы и читает строки.  
Extractor передаёт строки дальше построчно, по мере чтения.  
Extractor не принимает решений, не фильтрует и не хранит данные.

Emitter  
Вход: строки от Extractor + mapping.yaml.  
Поддержка v1: только XLS через xlrd 1.2.0.

Emitter обрабатывает файл построчно.  
Для каждой строки Emitter принимает решение GOOD или BAD.

GOOD-строка  
Строка валидна и соответствует контракту.  
GOOD-строка превращается в факт NDJSON.

BAD-строка  
Строка невалидна и не превращается в факт.  
Причина фиксируется, строка сохраняется отдельно.

Emitter не применяет бизнес-правила.  
Emitter нормализует SKU (Unicode NFKC, trim, collapse whitespace).  
Emitter формирует stats с причинами GOOD/BAD.

NDJSON контракт (GOOD)  
Каждая строка содержит:  
supplier_id, parser_id, mapping_version, ndjson_contract_version, emitter_version, run_id, effective_at, raw, parsed, quality_flags, _meta.

parsed.price — int (копейки) или null.  
parsed.qty — int или null.  
raw сохраняет исходные значения.  

Gate  
Gate работает с файлом как событием.  
Gate принимает решение PASS / WARN / FAIL.

FAIL возможен только при ошибках уровня файла.  
Наличие BAD-строк не является FAIL.

Ingestion  
Работает только при PASS или WARN.  
Принимает только GOOD-факты.  
SSOT append-only, дубликаты допустимы.

Curated / Offers  
Работает поверх SSOT.  
Условие оффера: parsed.price > 0 и parsed.qty > 0.  
Вся бизнес-логика реализуется только здесь.

Версионирование  
Новый parser_id обязателен при изменении алгоритмов, контрактов или формата.  
Новый mapping_version обязателен при любом изменении mapping.

Граница ответственности  
Extractor извлекает.  
Emitter решает судьбу строк.  
Gate решает судьбу файла.  
Ingestion хранит историю.  
Curated реализует бизнес.

Это канон v1.
