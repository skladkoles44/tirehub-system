# Spec files analysis (snapshot)

| file | role | status | relevance | notes |
|-----|-----|-----|-----|-----|
| ETL_EXTRACTOR_SPEC_FINAL_v3_3.md | Extractor architecture spec | legacy | medium | historical design for canonical extraction |
| backfill_snapshots_v1.6_plus.md | snapshot lifecycle | infra | medium | useful for immutable dataset policy |
| canonical_storage_contract.md | storage boundary contract | legacy | medium | built around canonical.ndjson |
| decomposer_v1.7_content_first_ssot.md | claim decomposition model | research | medium | L0/L1/L2 concept |
| glossary_etl.md | terminology | mixed | medium | terminology mostly from extractor era |
| golden_set_contract_v1.md | testing contract | active | medium | deterministic extraction validation |
| supplier_sources.md | supplier registry / sources | active | high | describes supplier input topology |
| offer_schema_v1.md | business offer contract | active | high | serving layer interface |
| parser_framework_v1.md | ingestion parser architecture | active | high | canonical ingestion framework |

## Observations
The repository currently contains two conceptual ETL lines:

1. Extractor / decomposer architecture (legacy research layer)
2. Parser framework + ingestion pipeline (current production direction)

New canonical chain:

supplier source -> parser framework -> GOOD facts -> offer schema -> serving layer

Legacy extractor documents remain useful as research/design references but are not the primary production architecture.
