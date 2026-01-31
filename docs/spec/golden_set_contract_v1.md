# Golden set contract v1

## Goal
A versioned NDJSON case set that validates:
- deterministic L1 extraction
- rejection behavior (critical sanity)
- expected conflicts + audit logging (signature causes)
- operational applicability of patches via both backfill modes

## Baskets
- golden/positive.ndjson: L1 claims must match; no conflicts; no critical sanity errors
- golden/negative.ndjson: must go to rejected with expected error_code/sanity_check_id
- golden/conflict.ndjson: conflict expected; audit row must exist; accounting uses only L1; quality_flags includes needs_review

## Case format (NDJSON, 1 line = 1 case)
Required fields:
- case_id
- supplier_hint (e.g., "any")
- raw_text
- expected.l1_claims (field -> array of {value, deterministic})
- expected.sanity (critical_errors[], warnings[])
- expected.conflicts[] (field, expect_conflict, expect_core_key, expect_conflict_type)
- expected.accounting_resolution (use_only_l1, quality_flags_must_include[])

## Core key vs hash
Tests store expect_core_key (preimage) rather than only signature_core hash.
Runtime must validate:
hash(expect_core_key) == signature_core using current hashing parameters.

## Versioning + manifest
golden/manifest.json must include:
- golden_set_version
- created_at
- min_decomposer_version
- min_ruleset_versions
- sha256 for each golden file

## Patch acceptance criteria
For any ruleset patch:
- reprocess_by_signature on conflict cases for the target core_key/signature_core
- reprocess_supplier_batch on a small golden batch
- regressions forbidden on positive/negative baskets
