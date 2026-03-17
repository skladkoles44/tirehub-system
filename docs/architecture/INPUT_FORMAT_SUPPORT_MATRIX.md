# INPUT FORMAT SUPPORT MATRIX

Статус на 2026-03-17

| Формат | Статус | Библиотека | Приоритет | Комментарий | Когда добавлять |
|---|---|---|---|---|---|
| XLSX | supported | openpyxl | P0 | Основной формат прайсов | уже сейчас |
| XLS | supported | xlrd | P0 | Старые прайсы, 1С-выгрузки | уже сейчас |
| CSV | supported | csv (stdlib) | P0 | Универсальная выгрузка, API, ручные файлы | уже сейчас |
| JSON | supported | json (stdlib) | P0 | API-фиды, современные поставщики | уже сейчас |
| ZIP | supported | zipfile (stdlib) | P0 | Контейнер для xlsx/csv/json | уже сейчас |
| XML | planned | xml.etree.ElementTree | P1 | 1С, ERP, B2B-выгрузки | в ближайший месяц |
| YAML | planned | PyYAML | P2 | Редко как прайс, чаще конфиг | по мере нужды |
| ODS | planned | odfpy | P3 | LibreOffice | если встретится |
| RAR/7z | planned | rarfile / py7zr | P4 | Экзотика | только по факту |
| XLSB | planned | pyxlsb | P5 | Binary Excel | если встретится |

P0 — must-have baseline
P1 — следующий обязательный шаг
P2–P5 — по мере боли
