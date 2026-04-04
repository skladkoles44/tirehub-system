import json
from pathlib import Path
from tempfile import TemporaryDirectory

from src.semantic.roles import classify_columns, enrich
from src.identity import enrich_from_name, build_identity_key
from src.extract.atomic_runner import run as runner_run
from src.normalize.supplier_offer import normalize_stock, derive_availability, build_canonical_supplier_offer
from src.normalize.supplier_offer.price_normalization import pick_purchase_price, to_cents

roles = classify_columns(["Артикул", "Цена оптовая", "Цена Розничная", "Остаток", "Сезонность"])
assert roles == ["sku", "price_wholesale", "price_retail", "stock", "season"]

row = {"name": "175/65R14 Viatti Brina Nordico (V-522) 82T"}
row2 = enrich_from_name(dict(row))
assert row2["width"] == 175
assert row2["height"] == 65
assert row2["diameter"] == 14
assert str(row2["load_index"]) == "82"
assert row2["speed_index"] == "T"

with TemporaryDirectory() as td:
    inp = Path(td) / "in.ndjson"
    out = Path(td) / "out.ndjson"
    inp.write_text(json.dumps({
        "columns": [
            {"header": "Артикул", "value": "123"},
            {"header": "Цена оптовая", "value": "4152"},
            {"header": "Остаток", "value": "больше 30"},
        ]
    }, ensure_ascii=False) + "\n", encoding="utf-8")
    enrich(inp, out)
    obj = json.loads(out.read_text(encoding="utf-8").splitlines()[0])
    got = [c["role"] for c in obj["columns"]]
    assert got == ["sku", "price_wholesale", "stock"]

assert callable(runner_run)

key, method, strength, raw, variant = build_identity_key({
    "supplier_id": "centrshin",
    "supplier_sku": "3151002",
    "brand": "Viatti",
    "model": "Brina Nordico (V-522)",
    "width": 175,
    "height": 65,
    "diameter": 14,
    "load_index": "82",
    "speed_index": "T",
    "name": "175/65R14 Viatti Brina Nordico (V-522) 82T",
})
assert key == "centrshin:3151002"
assert method == "sku"
assert strength == "high"

stock = normalize_stock("больше 30")
assert stock["kind"] == "gt"
assert derive_availability(stock) == "limited"
assert to_cents(4152) == 415200
assert pick_purchase_price({"supplier_price": 111, "retail": 222}) == 111
assert pick_purchase_price({"price": "не число", "retail": 222}) == 222
assert derive_availability(normalize_stock("в пути")) == "backorder"

offer = build_canonical_supplier_offer({
    "supplier_id": "centrshin",
    "source_file": "/tmp/stock.xlsx",
    "ingestion_id": "run123",
    "warehouse_key": "main",
    "supplier_sku": "3151002",
    "name": "175/65R14 Viatti Brina Nordico (V-522) 82T",
    "brand": "Viatti",
    "model": "Brina Nordico (V-522)",
    "season": "Зимние",
    "speed_index": " T (до 190) ",
    "prices": {"wholesale": 4152, "retail": 4851},
    "stock": stock,
})
assert offer["source_object_id"] == "/tmp/stock.xlsx"
assert offer["identity_key"] == "centrshin:3151002"
assert offer["price_purchase_cents"] == 415200
assert offer["availability_status"] == "limited"
assert offer["width"] == 175
assert offer["height"] == 65
assert offer["diameter"] == 14
assert offer["season"] == "winter"
assert offer["load_index"] == "82"
assert offer["speed_index"] == "T"

offer2 = build_canonical_supplier_offer({
    "supplier_id": "centrshin",
    "source_object_id": "obj-124",
    "ingestion_id": "run124",
    "warehouse_key": "main",
    "supplier_sku": "",
    "raw_name": "fallback raw name",
    "brand": "Viatti",
    "model": "Brina Nordico (V-522)",
    "width": 175,
    "height": 65,
    "diameter": 14,
    "season": "winter",
    "load_index": "82",
    "speed_index": "T",
    "prices": {"retail": 4851},
    "stock": {"qty": 1, "raw": "1", "kind": "exact"},
})
assert offer2["raw_name"] == "fallback raw name"
assert offer2["supplier_sku"] is None
assert offer2["source_object_id"] == "obj-124"

offer3 = build_canonical_supplier_offer({
    "supplier_id": "centrshin",
    "source_object_id": "obj-125",
    "ingestion_id": "run125",
    "warehouse_key": "main",
    "name": "205/55R16 Test Brand Model 91V",
    "prices": {"retail": 5000},
    "stock": {"qty": 1, "raw": "1", "kind": "exact"},
})
assert offer3["width"] == 205
assert offer3["height"] == 55
assert offer3["diameter"] == 16
assert offer3["speed_index"] == "V"

offer4 = build_canonical_supplier_offer({
    "supplier_id": "centrshin",
    "source_object_id": "obj-126",
    "ingestion_id": "run126",
    "warehouse_key": "main",
    "raw_name": "x",
    "season": "monsoon",
    "prices": {"retail": 1},
    "stock": {"qty": 1, "raw": "1", "kind": "exact"},
})
assert offer4["season"] == "monsoon"

failed = False
try:
    build_canonical_supplier_offer({
        "supplier_id": "centrshin",
        "ingestion_id": "run127",
        "warehouse_key": "main",
        "raw_name": "x",
        "prices": {"retail": 1},
        "stock": {"qty": 1, "raw": "1", "kind": "exact"},
    })
except ValueError as e:
    failed = "source_object_id" in str(e)
assert failed

print("SMOKE=OK")
print("IDENTITY_KEY=" + str(offer["identity_key"]))
print("SOURCE_OBJECT_ID=" + str(offer["source_object_id"]))
print("PRICE_PURCHASE_CENTS=" + str(offer["price_purchase_cents"]))
print("AVAILABILITY=" + str(offer["availability_status"]))
print("SEASON=" + str(offer["season"]))
print("LOAD_INDEX=" + str(offer["load_index"]))
print("SPEED_INDEX=" + str(offer["speed_index"]))
