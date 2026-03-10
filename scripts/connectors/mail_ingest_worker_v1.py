#!/usr/bin/env python3
import imaplib, ssl, os, json, email, re, time, fnmatch, atexit, signal
import yaml
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

_OWNER_LOCK_RELEASE = None

def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True

def _release_owner_lock() -> None:
    global _OWNER_LOCK_RELEASE
    if _OWNER_LOCK_RELEASE is None:
        return
    try:
        _OWNER_LOCK_RELEASE()
    finally:
        _OWNER_LOCK_RELEASE = None

def _install_owner_signal_handlers() -> None:
    def _handler(signum, frame):
        _release_owner_lock()
        raise SystemExit(128 + signum)
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, _handler)

def acquire_owner_lock(lock_path: Path):
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    while True:
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o664)
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                fh.write(f"{os.getpid()}\n")
            def _release():
                try:
                    txt = lock_path.read_text(encoding="utf-8").strip()
                except FileNotFoundError:
                    return
                if txt == str(os.getpid()):
                    try:
                        lock_path.unlink()
                    except FileNotFoundError:
                        pass
            return _release
        except FileExistsError:
            try:
                txt = lock_path.read_text(encoding="utf-8").strip()
            except FileNotFoundError:
                continue
            try:
                other_pid = int(txt)
            except ValueError:
                other_pid = None
            if other_pid and _pid_alive(other_pid):
                raise SystemExit(f"MAIL_INGEST_OWNER_LOCKED pid={other_pid} path={lock_path}")
            try:
                lock_path.unlink()
            except FileNotFoundError:
                continue
            except OSError as ex:
                raise SystemExit(f"MAIL_INGEST_OWNER_STALE_UNLINK_FAIL path={lock_path} err={ex}")
        except OSError as ex:
            raise SystemExit(f"MAIL_INGEST_OWNER_CREATE_FAIL path={lock_path} err={ex}")

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

def sanitize_filename(name: str) -> str:
    name = hdr(name or "")
    name = re.sub(r'[<>:"/\\\\|?*]', "_", name)
    name = re.sub(r"\s+", "_", name.strip())
    name = re.sub(r"_+", "_", name)
    name = name.strip("._")
    if not name:
        name = f"unnamed_{int(time.time())}.bin"
    return name

def guess_supplier_from_filename(filename: str) -> str:
    n = (filename or "").lower()
    if "b2bbr" in n or "brinex" in n:
        return "Brinex"
    if "linaris" in n or "tiresopt" in n:
        return "Linaris"
    if "колобокс" in n or "kolobox" in n:
        return "Kolobox"
    if "centrshin" in n or "центршин" in n:
        return "Centrshin"
    if "shinservice" in n:
        return "Shinservice"
    return "Unknown"

def load_suppliers_registry(path: Path):
    if not path.exists():
        raise SystemExit(f"SUPPLIERS_REGISTRY_NOT_FOUND: {path}")
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except Exception as ex:
        raise SystemExit(f"SUPPLIERS_REGISTRY_PARSE_FAIL: {path} :: {ex}")
    if not isinstance(raw, dict):
        raise SystemExit(f"SUPPLIERS_REGISTRY_SCHEMA_FAIL: root must be mapping path={path}")
    suppliers = raw.get("suppliers")
    if not isinstance(suppliers, list) or not suppliers:
        raise SystemExit(f"SUPPLIERS_REGISTRY_SCHEMA_FAIL: suppliers must be non-empty list path={path}")
    out = []
    seen_supplier = set()
    seen_slug = set()
    for idx, item in enumerate(suppliers, 1):
        if not isinstance(item, dict):
            raise SystemExit(f"SUPPLIERS_REGISTRY_SCHEMA_FAIL: suppliers[{idx}] must be mapping path={path}")
        supplier = str(item.get("supplier", "")).strip()
        slug = str(item.get("slug", "")).strip()
        inbox_dir = str(item.get("inbox_dir", "")).strip()
        patterns = item.get("filename_patterns")
        accepted_mailboxes = item.get("accepted_mailboxes", [])
        senders = item.get("senders", [])
        if not supplier:
            raise SystemExit(f"SUPPLIERS_REGISTRY_SCHEMA_FAIL: suppliers[{idx}].supplier empty path={path}")
        if not slug:
            raise SystemExit(f"SUPPLIERS_REGISTRY_SCHEMA_FAIL: suppliers[{idx}].slug empty path={path}")
        if not inbox_dir:
            raise SystemExit(f"SUPPLIERS_REGISTRY_SCHEMA_FAIL: suppliers[{idx}].inbox_dir empty path={path}")
        if not isinstance(patterns, list) or not patterns or not all(isinstance(x, str) and x.strip() for x in patterns):
            raise SystemExit(f"SUPPLIERS_REGISTRY_SCHEMA_FAIL: suppliers[{idx}].filename_patterns must be non-empty list[str] path={path}")
        if not isinstance(accepted_mailboxes, list) or not all(isinstance(x, str) for x in accepted_mailboxes):
            raise SystemExit(f"SUPPLIERS_REGISTRY_SCHEMA_FAIL: suppliers[{idx}].accepted_mailboxes must be list[str] path={path}")
        if not isinstance(senders, list) or not all(isinstance(x, str) for x in senders):
            raise SystemExit(f"SUPPLIERS_REGISTRY_SCHEMA_FAIL: suppliers[{idx}].senders must be list[str] path={path}")
        k1 = supplier.lower()
        k2 = slug.lower()
        if k1 in seen_supplier:
            raise SystemExit(f"SUPPLIERS_REGISTRY_SCHEMA_FAIL: duplicate supplier={supplier} path={path}")
        if k2 in seen_slug:
            raise SystemExit(f"SUPPLIERS_REGISTRY_SCHEMA_FAIL: duplicate slug={slug} path={path}")
        seen_supplier.add(k1)
        seen_slug.add(k2)
        out.append({
            "supplier": supplier,
            "slug": slug,
            "inbox_dir": inbox_dir,
            "patterns": [x.strip() for x in patterns if str(x).strip()],
            "accepted_mailboxes": accepted_mailboxes,
            "senders": senders,
        })
    return out
def match_supplier_from_registry(filename: str, registry) -> str:
    n = filename or ""
    n_low = n.lower()

    for item in registry:
        inbox_dir = item.get("inbox_dir") or item.get("supplier") or "Unknown"
        for pat in item.get("patterns", []):
            if fnmatch.fnmatchcase(n, pat) or fnmatch.fnmatchcase(n_low, pat.lower()):
                return inbox_dir
    return "Unknown"

def save_attachment_bytes(var_root: Path, supplier_dir: str, filename: str, payload: bytes) -> Path:
    target_dir = var_root / "inputs" / "inbox" / supplier_dir
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / filename
    tmp = target.with_name(target.name + ".tmp")
    with open(tmp, "wb") as f:
        f.write(payload)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, target)
    return target

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
    owner_lock_path = Path(os.environ.get("MAIL_INGEST_OWNER_LOCK", str(state_path.with_name("mail_ingest_owner.lock"))))
    print(f"MAIL_INGEST_OWNER_LOCK={owner_lock_path}")
    global _OWNER_LOCK_RELEASE
    _OWNER_LOCK_RELEASE = acquire_owner_lock(owner_lock_path)
    atexit.register(_release_owner_lock)
    _install_owner_signal_handlers()
    print(f"MAIL_INGEST_OWNER_PID={os.getpid()}")
    var_root = Path(need("ETL_VAR_ROOT"))
    registry_path = Path(os.environ.get("SUPPLIERS_REGISTRY_PATH", "config/suppliers_registry.yaml"))
    registry = load_suppliers_registry(registry_path)
    bootstrap = os.environ.get("MAIL_INGEST_BOOTSTRAP", "0").strip() == "1"
    force_bootstrap = os.environ.get("MAIL_INGEST_FORCE_BOOTSTRAP", "0").strip() == "1"
    download_mode = os.environ.get("MAIL_INGEST_DOWNLOAD", "0").strip() == "1"

    print("== config ==")
    print(f"IMAP_HOST={host}")
    print(f"IMAP_PORT={port}")
    print(f"IMAP_USER={user}")
    print(f"IMAP_MAILBOX={mailbox}")
    print(f"MAIL_INGEST_DRYRUN_MAX={max_msgs}")
    print(f"MAIL_INGEST_STATE={state_path}")
    print(f"MAIL_INGEST_BOOTSTRAP={'1' if bootstrap else '0'}")
    print(f"MAIL_INGEST_FORCE_BOOTSTRAP={'1' if force_bootstrap else '0'}")
    print(f"MAIL_INGEST_DOWNLOAD={'1' if download_mode else '0'}")
    print(f"ETL_VAR_ROOT={var_root}")
    print(f"SUPPLIERS_REGISTRY_PATH={registry_path}")
    print(f"SUPPLIERS_REGISTRY_COUNT={len(registry)}")

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
        print("NEW_UIDS=" + ",".join(new_uids[:20]) if new_uids else "NEW_UIDS=")

        probe = new_uids[:max_msgs]
        print(f"PROBE_COUNT={len(probe)}")
        if not probe:
            return 0

        typ, msg_data = m.uid("fetch", ",".join(probe), "(UID BODY.PEEK[])")
        if typ != "OK":
            raise SystemExit("IMAP_UID_FETCH_FAIL")

        rows = parse_uid_fetch(msg_data)
        for uid, raw in rows[:max_msgs]:
            uid_i = int(uid)
            print(f"--- MESSAGE UID {uid} BEGIN ---")
            msg = email.message_from_bytes(raw)
            print("FROM=" + hdr(msg.get("From")))
            print("SUBJECT=" + hdr(msg.get("Subject")))
            print("DATE=" + hdr(msg.get("Date")))

            att_count = 0
            saved_any = False
            relevant_any = False
            hard_fail = False

            for part in msg.walk():
                if part.is_multipart():
                    continue
                cdisp = part.get_content_disposition()
                fname = part.get_filename()
                if not (cdisp == "attachment" or fname):
                    continue

                att_count += 1
                safe_name = sanitize_filename(fname or "")
                payload = part.get_payload(decode=True) or b""
                supplier_dir = match_supplier_from_registry(safe_name, registry)
                if supplier_dir == "Unknown":
                    supplier_dir = guess_supplier_from_filename(safe_name)
                target = var_root / "inputs" / "inbox" / supplier_dir / safe_name

                print(f"ATTACHMENT_{att_count}_SAFE_NAME={safe_name}")
                print(f"ATTACHMENT_{att_count}_TYPE={str(part.get_content_type() or '')}")
                print(f"ATTACHMENT_{att_count}_SIZE={len(payload)}")
                print(f"ATTACHMENT_{att_count}_SUPPLIER={supplier_dir}")
                print(f"ATTACHMENT_{att_count}_WOULD_SAVE_TO={target}")

                if supplier_dir != "Unknown":
                    relevant_any = True

                if download_mode:
                    if supplier_dir == "Unknown":
                        print(f"ATTACHMENT_{att_count}_SKIP=UNKNOWN_SUPPLIER")
                        continue
                    if not payload:
                        print(f"ATTACHMENT_{att_count}_SAVE_FAIL=EMPTY_PAYLOAD")
                        hard_fail = True
                        break
                    try:
                        saved_path = save_attachment_bytes(var_root, supplier_dir, safe_name, payload)
                        print(f"ATTACHMENT_{att_count}_SAVED_TO={saved_path}")
                        saved_any = True
                    except Exception as e:
                        print(f"ATTACHMENT_{att_count}_SAVE_FAIL={e}")
                        hard_fail = True
                        break

            print(f"ATTACHMENT_COUNT={att_count}")

            if att_count == 0:
                print("UID_ACTION=ADVANCE_NO_ATTACHMENTS")
                if download_mode:
                    st["last_uid"] = max(int(st.get("last_uid", 0)), uid_i)
                    st["uidvalidity"] = uidvalidity
                    state_save(state_path, st)
                    print(f"WATERMARK_UPDATED_TO={st['last_uid']}")
            elif hard_fail:
                print("UID_ACTION=HOLD_HARD_FAIL")
                print("WATERMARK_NOT_UPDATED")
            elif saved_any:
                print("UID_ACTION=ADVANCE_SAVED")
                if download_mode:
                    st["last_uid"] = max(int(st.get("last_uid", 0)), uid_i)
                    st["uidvalidity"] = uidvalidity
                    state_save(state_path, st)
                    print(f"WATERMARK_UPDATED_TO={st['last_uid']}")
            elif relevant_any:
                print("UID_ACTION=HOLD_RELEVANT_NOT_SAVED")
                print("WATERMARK_NOT_UPDATED")
            else:
                print("UID_ACTION=ADVANCE_SKIP_UNKNOWN")
                if download_mode:
                    st["last_uid"] = max(int(st.get("last_uid", 0)), uid_i)
                    st["uidvalidity"] = uidvalidity
                    state_save(state_path, st)
                    print(f"WATERMARK_UPDATED_TO={st['last_uid']}")

            print(f"--- MESSAGE UID {uid} END ---")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
