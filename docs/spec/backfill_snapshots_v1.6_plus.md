## ОБЯЗАТЕЛЬНЫЙ ШАГ ПОСЛЕ BACKFILL
- publish_curated(snapshot_id, curated_version)

# Backfill + snapshots contract v1.6+

## Principle
Data is immutable. No UPDATE-in-place. Any change is a new snapshot.

## Snapshots
- Each run (initial or backfill) creates a new snapshot record.
- canonical_items_source rows reference snapshot_id.

Snapshot types:
- initial
- backfill_batch (reprocess_supplier_batch)
- backfill_signature (reprocess_by_signature)

## Backfill modes
1) reprocess_supplier_batch
Inputs:
- supplier_id
- date_range (by processed_at)
- target_ruleset_version
Output:
- new_snapshot_id
- affected_rows_count

2) reprocess_by_signature
Inputs:
- signature_core
- target_ruleset_version
- scope (all_history | last_days:N)
Output:
- new_snapshot_id
- affected_rows_count
- affected_suppliers

## Idempotency
Re-running the same backfill job over the same source snapshot + same versions MUST produce identical results (new snapshot still allowed, but contents/checksum identical).

## Downstream pinning
Downstream systems MUST explicitly pin snapshot_id (via VIEW, function parameter, or API query).
Older snapshots remain accessible for a defined retention period (operational policy).

## Verification
A snapshot becomes “ready” only after:
- deterministic artifact checks (checksum)
- golden set validation for patched signatures (when applicable)
