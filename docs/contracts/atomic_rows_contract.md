# atomic_rows contract

## required
- row_id
- source_file
- file_hash
- ingestion_id
- supplier_id
- sheet
- row_index
- columns

## rules
- supplier_id is required
- no implicit supplier_id="unknown" in production path
- columns is append-only extracted structure
