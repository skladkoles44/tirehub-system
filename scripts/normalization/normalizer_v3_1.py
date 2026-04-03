#!/usr/bin/env python3

import os, re, json, time, hashlib, math
from pathlib import Path
from collections import Counter

PRICE_HEADER_HINTS_WHOLESALE = {
    "цена оптовая","оптовая цена","оптовая","опт",
    "price wholesale","wholesale price",
}
PRICE_HEADER_HINTS_RETAIL = {
    "цена розничная","розничная цена","розничная","розница",
    "price retail","retail price",
}

TEXT_STOCK_PATTERNS = [
    (re.compile(r"^\s*больше\s+(\d+)\s*$", re.I),"gt"),
    (re.compile(r"^\s*>=\s*(\d+)\s*$", re.I),"gte"),
    (re.compile(r"^\s*(\d+)\+\s*$", re.I),"gte"),
]

def now(): return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def sha256_file(p:Path):
    h=hashlib.sha256()
    with p.open("rb") as f:
        for c in iter(lambda:f.read(1024*1024),b""): h.update(c)
    return f"sha256:{h.hexdigest()}"

def safe_slug(s:str):
    s=re.sub(r"[^\w.\-]+","_",s.strip())
    return re.sub(r"_+","_",s).strip("_") or "unknown"

def to_text(v):
    if v is None: return None
    s=str(v).strip()
    return s or None

def to_number(v):
    if v is None or isinstance(v,bool): return None
    if isinstance(v,int): return v
    if isinstance(v,float):
        if not math.isfinite(v): return None
        return int(v) if v.is_integer() else v
    s=str(v).strip().replace("\xa0","").replace(" ","").replace(",",".")
    try:
        x=float(s)
        if not math.isfinite(x): return None
        return int(x) if x.is_integer() else x
    except: return None

def normalize_stock(v):
    if v is None:
        return {"qty":None,"raw":None,"kind":None}

    n=to_number(v)
    if n is not None:
        return {"qty":int(n),"raw":str(v),"kind":"exact"}

    s=str(v).strip()
    sl=s.lower()

    if sl in ("в пути","in transit"):
        return {"qty":None,"raw":s,"kind":"in_transit"}

    for rx,k in TEXT_STOCK_PATTERNS:
        m=rx.match(s)
        if m:
            return {"qty":int(m.group(1)),"raw":s,"kind":k}

    return {"qty":None,"raw":s,"kind":"text"}

def classify_price(header,name,idx):
    h=(header or "").lower()
    n=(name or "").lower()

    if h in PRICE_HEADER_HINTS_WHOLESALE or "опт" in h or n in PRICE_HEADER_HINTS_WHOLESALE:
        return "wholesale"
    if h in PRICE_HEADER_HINTS_RETAIL or "розн" in h or n in PRICE_HEADER_HINTS_RETAIL:
        return "retail"

    return ["primary","secondary"][idx] if idx<2 else f"price_{idx+1}"

def pick_price(prices):
    if "wholesale" in prices: return prices["wholesale"]
    if "retail" in prices: return prices["retail"]
    return None

def index_columns(cols):
    first,roles,prices={}, {}, {}
    pi=0
    for c in cols:
        role=c.get("role") or "unknown"
        if isinstance(role,dict):
            role=role.get("role","unknown")
        role=str(role)

        val=c.get("value")
        roles.setdefault(role,[]).append(val)

        if role not in first:
            first[role]=val

        if role=="price":
            num=to_number(val)
            if num is not None:
                kind=classify_price(c.get("header"),c.get("name"),pi)
                prices[kind]=num
                pi+=1

    return first,roles,prices

from scripts.etl.size_extractor import enrich_from_name
from scripts.etl.identity_key import build_identity_key

def build_candidate(first,roles,prices,stock,rec):
    if not any([first.get("sku"),first.get("name"),stock["qty"],prices]):
        return None

    return {
        "source_file":rec.get("source_file"),
        "sheet_name":rec.get("sheet_name"),
        "table_index":rec.get("table_index"),
        "row_index":rec.get("row_index"),
        "fingerprint":((rec.get("layout") or {}).get("fingerprint")),

        "sku":to_text(first.get("sku")),
        "name":to_text(first.get("name") or first.get("model")),
        "brand":to_text(first.get("brand")),
        "model":to_text(first.get("model")),

        "size":[to_text(v) for v in roles.get("size",[]) if to_text(v)],
        "season":to_text(first.get("season")),

        "prices":prices,
        "stock":stock,
    }

def build_good(c):
    c=enrich_from_name(c)

    return {
        "supplier_sku":c.get("sku"),
        "normalizer_version":"3.1",
        "identity_basis":c.get("identity_basis"),

        "name":c.get("name"),
        "brand":c.get("brand"),
        "model":c.get("model"),

        "price":pick_price(c.get("prices",{})),
        "stock_qty":(c.get("stock") or {}).get("qty"),

        "size":c.get("size") or [],
        "prices":c.get("prices",{}),
        "stock":c.get("stock"),

        "identity_key":build_identity_key(c),

        "lineage":{
            "source_file":c.get("source_file"),
            "sheet_name":c.get("sheet_name"),
            "table_index":c.get("table_index"),
            "row_index":c.get("row_index"),
            "fingerprint":c.get("fingerprint"),
        }
    }

def process_row(rec):
    if "base" in rec and "offers" in rec:
        base=rec["base"]
        results=[]

        for off in rec["offers"]:
            c={
                "source_file":rec.get("source_file"),
                "sheet_name":rec.get("sheet_name"),
                "row_index":rec.get("row_index"),

                "sku":base.get("sku"),
                "name":base.get("name") or base.get("model"),
                "brand":base.get("brand"),
                "model":base.get("model"),

                "prices":{},
                "stock":{"qty":off.get("stock"),"raw":off.get("stock"),"kind":"exact"},
            }

            if not c["name"]:
                return "reject",None,"missing_name",rec

            c["identity_basis"]="sku" if c["sku"] else "derived_no_sku"
            results.append(build_good(c))

        return "multi",results,None,None

    first,roles,prices=index_columns(rec.get("columns",[]))
    stock=normalize_stock(first.get("stock"))

    c=build_candidate(first,roles,prices,stock,rec)
    if not c:
        return "reject",None,"empty_row",rec

    if not c["name"]:
        return "reject",c,"missing_name",rec

    if c["sku"]:
        c["identity_basis"]="sku"
    elif any([c.get("brand"),c.get("model"),c.get("size")]):
        c["identity_basis"]="derived_no_sku"
    else:
        return "reject",c,"missing_identity",rec

    return "good",build_good(c),None,None

def normalize_atomic_file(inp:Path,out_dir:Path,reject_mode="full"):
    out_dir.mkdir(parents=True,exist_ok=True)

    good_path = out_dir / 'good.ndjson'
    reject_path = out_dir / 'reject.ndjson'

    # ensure files exist
    good_path.touch(exist_ok=True)
    reject_path.touch(exist_ok=True)


    good_p=out_dir/"good.ndjson"
    rej_p=out_dir/"reject.ndjson"
    man_p=out_dir/"manifest.json"

    stats=Counter()
    reject_reasons=Counter()

    with inp.open() as src, \
         good_p.open("w") as g, \
         rej_p.open("w") as r:

        for line in src:
            stats["rows_in"]+=1
            rec=json.loads(line)

            typ,obj,reason,orig=process_row(rec)

            if typ=="good":
                g.write(json.dumps(obj,ensure_ascii=False)+"\n")
                stats["rows_good"]+=1

            elif typ=="multi":
                for x in obj:
                    g.write(json.dumps(x,ensure_ascii=False)+"\n")
                    stats["rows_good"]+=1

            else:
                reject_reasons[reason]+=1
                reject={
                    "ts":now(),
                    "normalizer_version":"3.1",
                    "reason":reason,
                    "candidate":obj,
                }
                if reject_mode=="full":
                    reject["original"]=orig

                r.write(json.dumps(reject,ensure_ascii=False)+"\n")
                stats["rows_reject"]+=1

    manifest={
        "ts":now(),
                    "normalizer_version":"3.1",
        "input":str(inp),
        "hash":sha256_file(inp),
        "stats":dict(stats),
        "rejects":dict(reject_reasons),
    }

    man_p.write_text(json.dumps(manifest,indent=2,ensure_ascii=False))
    return manifest

if __name__ == "__main__":
    import argparse
    from pathlib import Path

    ap = argparse.ArgumentParser(description="Normalizer v3.1")
    ap.add_argument("--atomic", required=True, help="path to atomic_rows.ndjson")
    ap.add_argument("--out-dir", required=True, help="output directory")
    ap.add_argument("--reject-mode", default="full", choices=["full","minimal"])

    args = ap.parse_args()

    print("RUN NORMALIZER...")
    print("INPUT:", args.atomic)
    print("OUT:", args.out_dir)

    result = normalize_atomic_file(
        Path(args.atomic),
        Path(args.out_dir),
        args.reject_mode
    )

    print("DONE")
    print(result)
