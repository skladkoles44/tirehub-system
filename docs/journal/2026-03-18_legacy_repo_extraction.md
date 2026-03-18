# 2026-03-18 — legacy repo extraction

## Что сделано
Из репозитория вынесен старый supplier-specific ingestion contour в отдельный архивный каталог вне repo.

## Что именно вынесено
- scripts/ingestion/**
- scripts/run_kolobox_full_v1.sh
- scripts/dev/centrshin_list_arrays.sh
- old supplier mappings/configs for kolobox/centrshin/brinex
- old kolobox evidence/docs/contracts/specs tied to legacy inbox/parser scheme

## Что НЕ трогали
- docs/journal/**
- docs/spec/ETL_EXTRACTOR_SPEC_FINAL_v3_3.md
- docs/spec/parser_framework_v1.md
- scripts/enrichment/brinex_parsed_enricher_v1.py
- scripts/probe/**
- scripts/connectors/mail_ingest_worker_v1.py
- config/suppliers_registry.yaml
- current L0/L1 contract docs

## Куда вынесено
- archive root: /storage/emulated/0/Download/ETL/repo_legacy_extracted_20260318_220134
- backup tar: /storage/emulated/0/Download/ETL/repo_legacy_extracted_20260318_220134/legacy_repo_backup.tar.gz
- manifest list: /storage/emulated/0/Download/ETL/repo_legacy_extracted_20260318_220134/legacy_repo_paths.txt

## Причина
Очистка репозитория от старой ingestion-схемы перед дальнейшим развитием новой L0/L1/L2 архитектуры.

## Результат
Repo очищен от legacy supplier-specific ingestion contour, при этом все файлы сохранены отдельно вне repo с сохранением исходных путей.
