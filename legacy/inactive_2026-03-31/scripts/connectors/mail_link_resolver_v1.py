#!/usr/bin/env python3
"""
mail_link_resolver_v1.py
Извлечение ссылок из .eml → link_events.ndjson

Свойства:
- deterministic
- idempotent (event_id от eml_hash + raw_url)
- HTML + TEXT всегда
- redirect decode
- фильтрация по config
"""

import json
import hashlib
import argparse
import email
import urllib.parse
import re
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Tuple
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# optional deps
try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except:
    HAS_BS4 = False

try:
    import yaml
    HAS_YAML = True
except:
    HAS_YAML = False


class MailLinkResolver:

    def __init__(self, config_path: Optional[str] = None):
        self.config = self._load_config(config_path)
        self.file_ext = ['.xls','.xlsx','.csv','.ods','.zip','.pdf','.doc','.docx']

    # ---------------- CONFIG ----------------

    def _load_config(self, path):
        cfg = {
            "global": {
                "min_confidence": 0.5,
                "deny_patterns": ["unsubscribe","tracking","facebook","instagram"]
            },
            "suppliers": {}
        }

        if path and HAS_YAML and Path(path).exists():
            with open(path) as f:
                user = yaml.safe_load(f) or {}
                cfg["global"].update(user.get("global", {}))
                cfg["suppliers"].update(user.get("suppliers", {}))

        return cfg

    # ---------------- PARSE EMAIL ----------------

    def parse_eml(self, path: Path):
        raw = path.read_bytes()
        eml_hash = hashlib.sha256(raw).hexdigest()

        msg = email.message_from_bytes(raw)

        html, text = "", ""

        for part in msg.walk():
            ctype = part.get_content_type()
            disp = str(part.get("Content-Disposition"))

            if "attachment" in disp:
                continue

            payload = part.get_payload(decode=True)
            if not payload:
                continue

            try:
                decoded = payload.decode(errors="ignore")
            except:
                continue

            if ctype == "text/html":
                html += decoded
            elif ctype == "text/plain":
                text += decoded

        return html, text, eml_hash

    # ---------------- EXTRACT ----------------

    def extract_html(self, html):
        out = []
        if not html or not HAS_BS4:
            return out

        soup = BeautifulSoup(html, "html.parser")

        for a in soup.find_all("a", href=True):
            u = a["href"].strip()

            if u.startswith(("mailto:", "javascript:", "#", "tel:", "data:")):
                continue

            out.append({
                "url": u,
                "anchor": a.get_text(strip=True),
                "source": "html"
            })

        return out

    def extract_text(self, text):
        out = []
        if not text:
            return out

        urls = re.findall(r'https?://[^\s<>"\']+', text)

        for u in urls:
            if u.startswith(("mailto:", "javascript:")):
                continue

            out.append({
                "url": u,
                "anchor": "",
                "source": "text"
            })

        return out

    # ---------------- NORMALIZE ----------------

    def decode_redirect(self, url):
        keys = ["url","u","redirect","target","link","goto","r"]

        for k in keys:
            m = re.search(rf"[?&]{k}=([^&]+)", url)
            if m:
                try:
                    d = urllib.parse.unquote(m.group(1))
                    if d.startswith("http"):
                        return d
                except:
                    pass

        return url

    def normalize(self, url):
        url = self.decode_redirect(url)

        try:
            p = urllib.parse.urlparse(url)
            host = p.hostname.lower() if p.hostname else ""
            path = p.path.rstrip("/")

            return f"{p.scheme}://{host}{path}"
        except:
            return url

    # ---------------- CLASSIFY ----------------

    def classify(self, url, anchor, supplier):
        u = url.lower()
        a = anchor.lower()

        typ = "unknown"
        ext = None
        conf = 0.0

        # file detection
        for e in self.file_ext:
            if u.endswith(e) or f"{e}?" in u:
                typ = "file"
                ext = e[1:]
                conf = 0.85

        # api
        if "/api/" in u:
            typ = "api"
            conf += 0.4

        # anchor signals
        if any(k in a for k in ["скачать","download","price","прайс","stock","остатки"]):
            conf += 0.4

        return typ, ext, min(conf, 1.0)

    # ---------------- FILTER ----------------

    def filter(self, url, conf, supplier):
        g = self.config["global"]
        s = self.config["suppliers"].get(supplier, {})

        min_conf = s.get("min_confidence", g["min_confidence"])

        if conf < min_conf:
            return False

        deny = list(g.get("deny_patterns", [])) + s.get("deny_patterns", [])

        if any(d in url.lower() for d in deny):
            return False

        allow = s.get("allow_patterns")
        if allow:
            if not any(a in url.lower() for a in allow):
                return False

        return True

    # ---------------- PROCESS ----------------

    def process_file(self, path: Path, supplier):
        html, text, eml_hash = self.parse_eml(path)

        links = self.extract_html(html) + self.extract_text(text)

        uniq = {}
        for l in links:
            uniq[l["url"]] = l

        events = []

        for l in uniq.values():
            raw = l["url"]

            event_id = hashlib.sha256(
                f"{eml_hash}:{raw}".encode()
            ).hexdigest()

            norm = self.normalize(raw)

            typ, ext, conf = self.classify(norm, l["anchor"], supplier)

            if not self.filter(norm, conf, supplier):
                continue

            events.append({
                "event_id": event_id,
                "eml_hash": eml_hash,
                "supplier_id": supplier,
                "url_raw": raw,
                "url_normalized": norm,
                "detected_type": typ,
                "file_ext": ext,
                "confidence": conf,
                "source_part": l["source"],
                "anchor_text": l["anchor"][:200],
                "created_at": datetime.utcnow().isoformat() + "Z"
            })

        return events

    def run(self, dir_path, supplier):
        out = []

        files = sorted(Path(dir_path).glob("*.eml"))

        for f in files:
            out.extend(self.process_file(f, supplier))

        return out


# ---------------- MAIN ----------------

def main():
    p = argparse.ArgumentParser()

    p.add_argument("--eml-dir", required=True)
    p.add_argument("--supplier-id", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--config")

    args = p.parse_args()

    resolver = MailLinkResolver(args.config)

    events = resolver.run(args.eml_dir, args.supplier_id)

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)

    with open(out, "w") as f:
        for e in events:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")

    print(f"OK: {len(events)} events")


if __name__ == "__main__":
    main()
