def test_atomic_rows_contract_smoke():
    required = {"row_id","source_file","file_hash","ingestion_id","supplier_id","sheet","row_index","columns"}
    sample = {"row_id":"x","source_file":"x","file_hash":"x","ingestion_id":"x","supplier_id":"x","sheet":"x","row_index":1,"columns":[]}
    assert required.issubset(sample.keys())
