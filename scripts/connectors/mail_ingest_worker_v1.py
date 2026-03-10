#!/usr/bin/env python3
import imaplib, ssl, os, json, email
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

def hdr(v):
    if not v:
        return ""
    try:
        return str(make_header(decode_header(v)))
    except Exception:
        return v

def state_load(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}

def state_save(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)

def parse_uid_fetch(data):
    out = []
    for part in data or []:
        if isinstance(part, tuple) and len(part) >= 2:
            meta, raw = part[0], part[1]
            meta_s = meta.decode("utf-8", "ignore") if isinstance(meta, (bytes, bytearray)) else str(meta)
            uid = ""
            if "UID " in meta_s:
                uid = meta_s.split("UID ", 1)[1].split()[0].rstrip(")")
            out.append((uid, raw))
    return out

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
    state_path = Path(need("MAIL_INGEST_STATE"))
    bootstrap = os.environ.get("MAIL_INGEST_BOOTSTRAP", "0").strip() == "1"
    force_bootstrap = os.environ.get("MAIL_INGEST_FORCE_BOOTSTRAP", "0").strip() == "1"

    print("== dry-run config ==")
    print(f"IMAP_HOST={host}")
    print(f"IMAP_PORT={port}")
    print(f"IMAP_USER={user}")
    print(f"IMAP_MAILBOX={mailbox}")
    print(f"MAIL_INGEST_DRYRUN_MAX={max_msgs}")
    print(f"MAIL_INGEST_STATE={state_path}")
    print(f"MAIL_INGEST_BOOTSTRAP={'1' if bootstrap else '0'}")
    print(f"MAIL_INGEST_FORCE_BOOTSTRAP={'1' if force_bootstrap else '0'}")

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

        typ, data = m.response("UIDVALIDITY")
        uidvalidity = ""
        if typ == "UIDVALIDITY" and data and data[0]:
            uidvalidity = data[0].decode("utf-8", "ignore") if isinstance(data[0], (bytes, bytearray)) else str(data[0])
        print(f"UIDVALIDITY={uidvalidity}")

        st = state_load(state_path)
        print("== state before ==")
        print(json.dumps(st, ensure_ascii=False))

        typ, data = m.uid("search", None, "1:*")
        if typ != "OK":
            raise SystemExit("IMAP_UID_SEARCH_FAIL")
        all_uids = [x for x in (data[0].decode("utf-8").strip().split() if data and data[0] else []) if x]
        print(f"UID_TOTAL={len(all_uids)}")
        max_uid = all_uids[-1] if all_uids else ""
        print(f"UID_MAX={max_uid}")

        if bootstrap:
            if st and not force_bootstrap:
                raise SystemExit("BOOTSTRAP_REFUSED_STATE_EXISTS")
            new_state = {"uidvalidity": uidvalidity, "last_uid": int(max_uid) if max_uid else 0}
            state_save(state_path, new_state)
            print("BOOTSTRAP_DONE=1")
            print("== state after ==")
            print(json.dumps(new_state, ensure_ascii=False))
            return 0

        if not st:
            raise SystemExit("STATE_MISSING_RUN_BOOTSTRAP_FIRST")

        if str(st.get("uidvalidity", "")) != str(uidvalidity):
            raise SystemExit(f"UIDVALIDITY_CHANGED: state={st.get('uidvalidity','')} mailbox={uidvalidity}")

        last_uid = int(st.get("last_uid", 0))
        print(f"LAST_UID={last_uid}")

        typ, data = m.uid("search", None, f"{last_uid + 1}:*")
        if typ != "OK":
            raise SystemExit("IMAP_UID_RANGE_SEARCH_FAIL")
        found_uids = [x for x in (data[0].decode("utf-8").strip().split() if data and data[0] else []) if x]
        new_uids = [u for u in found_uids if int(u) > last_uid]
        print(f"NEW_UID_COUNT={len(new_uids)}")
        if new_uids:
            print("NEW_UIDS=" + ",".join(new_uids[:20]))
        else:
            print("NEW_UIDS=")

        probe = new_uids[:max_msgs]
        print(f"PROBE_COUNT={len(probe)}")
        if not probe:
            return 0

        typ, msg_data = m.uid("fetch", ",".join(probe), "(UID BODY.PEEK[])")
        if typ != "OK":
            raise SystemExit("IMAP_UID_FETCH_FAIL")

        rows = parse_uid_fetch(msg_data)
        for uid, raw in rows[:max_msgs]:
            print(f"--- MESSAGE UID {uid} BEGIN ---")
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
            print(f"--- MESSAGE UID {uid} END ---")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
