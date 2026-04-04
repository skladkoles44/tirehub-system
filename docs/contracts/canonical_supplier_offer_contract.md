# canonical supplier offer contract

## required
- supplier_id
- source_type
- source_object_id
- run_id
- offer_key
- warehouse_key
- availability_status

## nullable
- supplier_sku
- raw_name
- item_type
- warehouse_raw
- stock_qty_raw
- stock_qty_normalized
- price_purchase_cents
- currency
- identity_key
- quality_flags
- is_reject
- reject_reason

## rules
- missing price != 0
- missing supplier_id != unknown
- missing warehouse_key != valid current offer
- missing identity_key != publishable automatically
