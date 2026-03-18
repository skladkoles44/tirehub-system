# Decomposer architectural contract v1.7 (content-first SSOT)

## Purpose
Decomposer enriches canonical items by decomposing free-text fields (primarily `name`, optionally `raw`) into auditable claims with evidence, without breaking accounting determinism.

## Input (minimum)
Canonical item with trace + versioning:
- supplier, source_file, source_table, row_index, qty_column_index
- name (string) and optionally raw (string[])
- snapshot_id, ruleset_versions, decomposer_version

## Truth layers
- L0 Raw Text: unchanged input text (SSOT)
- L1 Structured Claims: deterministic rules (regex/dicts/grammars), fully reproducible
- L2 Enriched Claims: probabilistic suggestions (optional), never overrides L1 in accounting

## Evidence inheritance
Every claim MUST include evidence.
- L1 evidence types: dict_match / regex / rule_id / match spans
- L2 evidence types: model_version / confidence / context references

## Bridge rule (hard)
- Accounting path uses ONLY L1 claims.
- L2 can only emit audit challenges; conflicts are logged, not applied to accounting.

## Conflicts and audit
- Row-level conflicts written to `decomposition_row_audit`
- Aggregated causes tracked in `decomposition_conflict_signatures`
- signature_core derived from canonical core key; token_pattern included only for discrete fields (token_pattern_fields ruleset)

## Quality flags
- needs_review: conflict present (audit exists)
- blocked_for_aggregation: supplier degraded/blocked by quality gate
- other flags as per sanity checks

## Determinism
Same input + same rulesets + same decomposer_version => bit-identical outputs (canonical artifacts + audit artifacts), per project JSON canonicalization.
