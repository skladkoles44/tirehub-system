# legacy_donor_map

## Правило
Архив и dump используются только как donor/reference.
Они не являются source of truth.

## Статусы
- take: переносим как базу
- reference: берём идеи/паттерны
- discard: не переносим в новый контур
- split: переносим, но режем на части

## Таблица
| donor_file | status | target_module | note |
|---|---|---|---|
| mail_ingest_worker_v1.py | take | src/connectors/mail_intake | evidence-first intake |
| mail_unpacker_v1.py | take | src/connectors/mail_intake | unpack step |
| runner_v5_6_3.py | take | src/extract/atomic_runner | atomic extraction |
| runner_with_fs_state.py | reference | src/extract/fs_state | only after core smoke |
| column_classifier.py | take | src/semantic/roles | role classifier |
| enrich_roles.py | take | src/semantic/roles | semantic enrich |
| identity_key.py | discard | - | replaced by v2 |
| identity_key_v2.py | take | src/identity | main identity engine |
| size_extractor.py | take | src/identity | size utility |
| normalizer_v3_1.py | split | src/normalize/supplier_offer | split into small modules |
| schema_registry.py | reference | src/schema_memory | enable later |
| layout_fingerprint.py | reference | src/schema_memory | enable later |
| schema_drift.py | reference | src/schema_memory | enable later |
