# 2026-03-18 — repo runtime extraction

## Что сделано
Из репозитория вынесены runtime/dev artifacts, не являющиеся кодом системы.

## Что вынесено
- .pydeps/**
- tmp/**
- normalizer_v1.py (root-level stray file)

## Что НЕ трогали
- scripts/normalization/normalizer_v1.py
- docs/contracts/atomic_rows_v1.md
- docs/journal/**
- scripts/probe/**
- текущие config/docs/contracts

## Куда вынесено
- archive root: /storage/emulated/0/Download/ETL/repo_runtime_extracted_20260318_221427
- backup tar: /storage/emulated/0/Download/ETL/repo_runtime_extracted_20260318_221427/runtime_repo_backup.tar.gz
- manifest list: /storage/emulated/0/Download/ETL/repo_runtime_extracted_20260318_221427/runtime_repo_paths.txt

## Причина
Очистка repo от runtime/dev хвостов после выноса legacy ingestion contour.

## Результат
Repo стал чище: removed .pydeps, tmp, root stray normalizer file from tracked workspace while preserving all contents outside repo.
