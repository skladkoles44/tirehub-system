#!/usr/bin/env python3
import json, re, sys
from pathlib import Path

def norm(s: str) -> str:
    s = (s or "").replace("\u00a0", " ")
    s = s.strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

def load_evidence(dirpath: Path):
    out=[]
    for p in sorted(dirpath.glob("*.evidence.json")):
        out.append((p, json.loads(p.read_text(encoding="utf-8"))))
    return out

def header_cells(j):
    h=j.get("header",{})
    hr=int(h.get("header_rows", 1))
    r1=[norm(x) for x in h.get("row1_cells", [])]
    r2=[norm(x) for x in h.get("row2_cells", [])]
    return hr, r1, r2

def signature_tokens(hr, r1, r2):
    toks=[]
    if hr==2:
        for i,a in enumerate(r1):
            b = r2[i] if i < len(r2) else ""
            if a or b:
                toks.append(f"{a}|{b}" if b else a)
    else:
        toks=[t for t in r1 if t]
    # de-dup preserve order
    seen=set(); out=[]
    for t in toks:
        if t and t not in seen:
            seen.add(t); out.append(t)
    return out

# Minimal YAML reader for the parts we need:
# We only support:
# - dispatch.rules[].layout, dispatch.rules[].header_rows, dispatch.rules[].signature_contains_all (list of strings)
# - layouts.<name>.required_columns (list of selectors: either string (for 1-row) or [name, idx] for 2-row)
# This is intentionally strict; if file deviates, it fails loudly.
def load_contract_minimal(path: Path):
    text = path.read_text(encoding="utf-8").splitlines()
    cur=None
    mode=None
    layouts={}
    rules=[]
    layout_name=None
    in_layouts=False
    in_dispatch=False
    in_rules=False
    cur_rule=None

    def parse_list_item(line):
        # "- foo" or "- [a, 0]"
        s=line.strip()[2:].strip()
        if s.startswith("[") and s.endswith("]"):
            inner=s[1:-1].strip()
            parts=[p.strip().strip("'\"") for p in inner.split(",")]
            if len(parts)!=2: raise ValueError(f"bad selector: {line}")
            return [parts[0], int(parts[1])]
        return s.strip("'\"")

    i=0
    while i < len(text):
        line=text[i]
        raw=line
        line=line.rstrip("\n")
        if not line.strip() or line.lstrip().startswith("#"):
            i+=1; continue

        if re.match(r"^layouts:\s*$", line):
            in_layouts=True; in_dispatch=False; i+=1; continue
        if re.match(r"^dispatch:\s*$", line):
            in_dispatch=True; in_layouts=False; i+=1; continue

        if in_layouts:
            m=re.match(r"^  ([a-zA-Z0-9_]+):\s*$", line)
            if m:
                layout_name=m.group(1)
                layouts.setdefault(layout_name, {"required_columns":[]})
                i+=1; continue
            if layout_name and re.match(r"^    required_columns:\s*$", line):
                # consume list items
                i+=1
                req=[]
                while i < len(text) and text[i].startswith("      - "):
                    req.append(parse_list_item(text[i]))
                    i+=1
                layouts[layout_name]["required_columns"]=req
                continue

        if in_dispatch:
            if re.match(r"^  rules:\s*$", line):
                in_rules=True; i+=1; continue
            if in_rules:
                if re.match(r"^    - ", line):
                    cur_rule={"layout":None,"header_rows":None,"signature_contains_all":[],"name":None}
                    rules.append(cur_rule)
                    # could be "name:" on same line? ignore
                    i+=1; continue
                if cur_rule is not None:
                    m=re.match(r"^      name:\s*(.+)\s*$", line)
                    if m: cur_rule["name"]=m.group(1).strip().strip("'\""); i+=1; continue
                    m=re.match(r"^      layout:\s*(.+)\s*$", line)
                    if m: cur_rule["layout"]=m.group(1).strip().strip("'\""); i+=1; continue
                    m=re.match(r"^      header_rows:\s*([0-9]+)\s*$", line)
                    if m: cur_rule["header_rows"]=int(m.group(1)); i+=1; continue
                    if re.match(r"^      signature_contains_all:\s*\[\s*\]\s*$", line):
                        cur_rule["signature_contains_all"]=[]; i+=1; continue
                    if re.match(r"^      signature_contains_all:\s*$", line):
                        i+=1
                        sig=[]
                        while i < len(text) and text[i].startswith("        - "):
                            sig.append(text[i].strip()[2:].strip().strip("'\""))
                            i+=1
                        cur_rule["signature_contains_all"]=sig
                        continue

        i+=1

    if not rules:
        raise ValueError("NO_DISPATCH_RULES_FOUND")
    return layouts, rules

def rule_matches(rule, file_sig):
    need=[norm(x) for x in (rule.get("signature_contains_all") or []) if x is not None]
    if not need:
        return False
    # file_sig tokens are already normalized; for 2-row we have "h1|h2"
    # match if any token contains needed string OR equals it
    for n in need:
        found=False
        for t in file_sig:
            if t==n or n in t:
                found=True; break
        if not found:
            return False
    return True

def required_present(req_cols, hr, r1, r2):
    # req selector: string or [name, idx]
    if not req_cols:
        return True, []
    missing=[]
    if hr==2:
        s1=set(r1); s2=set(r2)
        for sel in req_cols:
            if isinstance(sel, list):
                name=norm(sel[0]); idx=int(sel[1])
                if idx==0:
                    if name not in s1: missing.append(sel)
                elif idx==1:
                    if name not in s2: missing.append(sel)
                else:
                    missing.append(sel)
            else:
                name=norm(sel)
                if name not in s1 and name not in s2:
                    missing.append(sel)
    else:
        s=set(r1)
        for sel in req_cols:
            if isinstance(sel, list):
                # not expected for hr=1
                name=norm(sel[0])
                if name not in s: missing.append(sel)
            else:
                name=norm(sel)
                if name not in s: missing.append(sel)
    return (len(missing)==0), missing

def main():
    if len(sys.argv) < 3:
        print("usage: validate_mapping_contract.py <evidence_dir> <contract_yaml> [--print-signatures]")
        return 2
    evidence_dir=Path(sys.argv[1])
    contract=Path(sys.argv[2])
    do_print="--print-signatures" in sys.argv[3:]

    layouts, rules = load_contract_minimal(contract)
    ev=load_evidence(evidence_dir)
    if not ev:
        print("NO_EVIDENCE")
        return 1

    ok=True
    for p,j in ev:
        hr, r1, r2 = header_cells(j)
        sig = signature_tokens(hr, r1, r2)
        if do_print:
            print(f"\n=== {p.name} ===")
            print("file:", j.get("file"))
            print("header_rows:", hr)
            print("signature_contains_all candidates sample:", sig[:40])

        matched=[]
        for rule in rules:
            if rule.get("header_rows") is not None and int(rule["header_rows"]) != hr:
                continue
            if rule_matches(rule, sig):
                matched.append(rule)

        if len(matched)!=1:
            ok=False
            print(f"\nFAIL: dispatch not unique for {p.name} (matched={len(matched)})")
            for r in matched:
                print("  -", r.get("name"), "layout=", r.get("layout"))
            if not matched:
                print("  - no rule matched; fill signature_contains_all for this file")
            continue

        rule=matched[0]
        layout=rule.get("layout")
        req = layouts.get(layout, {}).get("required_columns", [])
        pres, missing = required_present(req, hr, r1, r2)
        if not pres:
            ok=False
            print(f"\nFAIL: required_columns missing for {p.name} layout={layout}")
            print("missing:", missing)

    if ok:
        print("\nOK: contract matches evidence (dispatch unique; required_columns present)")
        return 0
    return 1

if __name__=="__main__":
    raise SystemExit(main())
