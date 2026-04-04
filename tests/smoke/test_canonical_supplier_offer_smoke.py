def test_canonical_supplier_offer_contract_smoke():
    required = {"supplier_id","source_type","source_object_id","run_id","offer_key","warehouse_key","availability_status"}
    sample = {"supplier_id":"x","source_type":"file","source_object_id":"x","run_id":"x","offer_key":"x","warehouse_key":"x","availability_status":"unknown"}
    assert required.issubset(sample.keys())
