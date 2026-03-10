#!/usr/bin/env python3
import imaplib, ssl, os, sys, email
from email.header import decode_header, make_header
from pathlib import Path

def load_env_file(path: str) -> None:
    p = Path(path)
    if not p.exists():
        raise SystemExit(f"ENV_FILE_NOT_FOUND: {p}")
    for raw in p.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip())

def need(name: str) -> str:
    v = os.environ.get(name, "").strip()
    if not v:
        raise SystemExit(f"ENV_MISSING: {name}")
    return v

def hdr(v: str | None) -> str:
    if not v:
        return ""
    try:
        return str(make_header(decode_header(v)))
    except Exception:
        return v

def main() -> int:
    env_file = os.environ.get("MAIL_INGEST_ENV_FILE", "").strip()
    if env_file:
        load_env_file(env_file)

    host = need("IMAP_HOST")
    port = int(os.environ.get("IMAP_PORT", "993"))
    user = need("IMAP_USER")
    password = need("IMAP_PASS")
    mailbox = os.environ.get("IMAP_MAILBOX", "INBOX").strip() or "INBOX"
    max_msgs = int(os.environ.get("MAIL_INGEST_DRYRUN_MAX", "10"))

    print("== dry-run config ==")
    print(f"IMAP_HOST={host}")
    print(f"IMAP_PORT={port}")
    print(f"IMAP_USER={user}")
    print(f"IMAP_MAILBOX={mailbox}")
    print(f"MAIL_INGEST_DRYRUN_MAX={max_msgs}")

    ctx = ssl.create_default_context()
    with imaplib.IMAP4_SSL(host, port, ssl_context=ctx) as m:
        typ, _ = m.login(user, password)
        if typ != "OK":
            raise SystemExit("IMAP_LOGIN_FAIL")
        print("IMAP_LOGIN_OK")

        typ, _ = m.select(mailbox, readonly=True)
        if typ != "OK":
            raise SystemExit(f"IMAP_SELECT_FAIL: {mailbox}")
        print("IMAP_SELECT_OK")

        typ, data = m.search(None, "UNSEEN")
        if typ != "OK":
            raise SystemExit("IMAP_SEARCH_FAIL")
        ids = [x for x in (data[0].decode("utf-8").strip().split() if data and data[0] else []) if x]
        print(f"UNSEEN_COUNT={len(ids)}")
        if ids:
            print("UNSEEN_IDS=" + ",".join(ids[:20]))
        else:
            print("UNSEEN_IDS=")

        probe = ids[:max_msgs]
        print(f"PROBE_COUNT={len(probe)}")

        for mid in probe:
            print(f"--- MESSAGE {mid} BEGIN ---")
            typ, msg_data = m.fetch(mid, "(BODY.PEEK[])")
            if typ != "OK" or not msg_data:
                print("FETCH_FAIL")
                print(f"--- MESSAGE {mid} END ---")
                continue

            raw = None
            for part in msg_data:
                if isinstance(part, tuple) and len(part) >= 2:
                    raw = part[1]
                    break
            if raw is None:
                print("NO_RAW_MESSAGE")
                print(f"--- MESSAGE {mid} END ---")
                continue

            msg = email.message_from_bytes(raw)
            print("FROM=" + hdr(msg.get("From")))
            print("SUBJECT=" + hdr(msg.get("Subject")))
            print("DATE=" + hdr(msg.get("Date")))

            att_count = 0
            for part in msg.walk():
                if part.is_multipart():
                    continue
                cdisp = part.get_content_disposition()
                fname = part.get_filename()
                if cdisp == "attachment" or fname:
                    att_count += 1
                    print(f"ATTACHMENT_{att_count}_NAME=" + hdr(fname or ""))
                    print(f"ATTACHMENT_{att_count}_TYPE=" + str(part.get_content_type() or ""))
                    payload = part.get_payload(decode=True)
                    print(f"ATTACHMENT_{att_count}_SIZE={len(payload) if payload else 0}")
            print(f"ATTACHMENT_COUNT={att_count}")
            print(f"--- MESSAGE {mid} END ---")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
