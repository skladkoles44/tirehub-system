#!/usr/bin/env python3
"""
mail_unpacker_v1.py (FINAL)

ABSOLUTE STUPID UNPACKER:
- читает RAW .eml из in_dir
- извлекает attachments → out_dir/attachments/
- извлекает body → out_dir/body/
- пишет NDJSON manifest
- ничего не знает про окружение

Пути — аргументы командной строки
"""

import sys
import os
import json
import hashlib
import email
from pathlib import Path
from email import policy

# ==================== LIMITS ====================

MAX_EML_SIZE = 50 * 1024 * 1024      # 50MB
MAX_PART_SIZE = 20 * 1024 * 1024     # 20MB

# ==================== UTILS ====================

def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def sha256_file(path: Path) -> str:
    """Вычисляет SHA256 файла"""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def safe_filename(name: str) -> str:
    """Санитизирует имя файла, добавляя hash при коллизиях"""
    if not name:
        return "unknown"
    clean = "".join(c if c.isalnum() or c in "._-" else "_" for c in name)
    if clean != name:
        suffix = hashlib.md5(name.encode()).hexdigest()[:6]
        clean = f"{clean}_{suffix}"
    return clean

def fsync_directory(path: Path):
    """Синхронизирует директорию на диск"""
    try:
        dir_fd = os.open(str(path), os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except Exception:
        pass  # если не поддерживается, игнорируем

def write_if_not_exists(path: Path, data: bytes):
    """Идемпотентная запись с fsync"""
    if path.exists():
        return
    
    tmp = path.with_name(path.name + ".tmp")
    
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    
    os.rename(tmp, path)
    fsync_directory(path.parent)

# ==================== CORE ====================

def process_eml(eml_path: Path, att_dir: Path, body_dir: Path, manifest):
    """Обрабатывает один .eml файл"""
    size = eml_path.stat().st_size
    
    if size > MAX_EML_SIZE:
        manifest.write(json.dumps({
            "source": eml_path.name,
            "type": "skip",
            "reason": "eml_too_large",
            "size": size
        }, ensure_ascii=False) + "\n")
        return
    
    # Хеш всего письма (связь)
    eml_hash = sha256_file(eml_path)
    
    with open(eml_path, "rb") as f:
        msg = email.message_from_binary_file(f, policy=policy.default)
    
    part_index = 0
    
    for part in msg.walk():
        if part.is_multipart():
            continue
        
        payload = part.get_payload(decode=True)
        if payload is None:
            continue
        
        if len(payload) > MAX_PART_SIZE:
            manifest.write(json.dumps({
                "source": eml_path.name,
                "eml_hash": eml_hash,
                "type": "skip",
                "reason": "part_too_large",
                "size": len(payload)
            }, ensure_ascii=False) + "\n")
            continue
        
        part_hash = sha256_bytes(payload)
        ctype = part.get_content_type()
        filename = part.get_filename()
        
        content_disposition = part.get("Content-Disposition", "")
        is_attachment = "attachment" in content_disposition.lower()
        
        # ==================== ATTACHMENT ====================
        if filename or is_attachment:
            orig_name = filename or "unknown"
            fname = safe_filename(orig_name)
            out_name = f"{part_hash[:12]}_{fname}"
            out_path = att_dir / out_name
            
            write_if_not_exists(out_path, payload)
            
            manifest.write(json.dumps({
                "source": eml_path.name,
                "eml_hash": eml_hash,
                "type": "attachment",
                "file": out_name,
                "sha256": part_hash,
                "content_type": ctype,
                "orig_name": orig_name,
                "content_disposition": content_disposition
            }, ensure_ascii=False) + "\n")
        
        # ==================== BODY ====================
        else:
            ext = "bin"
            if ctype == "text/plain":
                ext = "txt"
            elif ctype == "text/html":
                ext = "html"
            
            out_name = f"{part_hash[:12]}_part{part_index}.{ext}"
            out_path = body_dir / out_name
            
            write_if_not_exists(out_path, payload)
            
            manifest.write(json.dumps({
                "source": eml_path.name,
                "eml_hash": eml_hash,
                "type": "body",
                "file": out_name,
                "sha256": part_hash,
                "content_type": ctype,
                "part_index": part_index
            }, ensure_ascii=False) + "\n")
        
        part_index += 1

# ==================== ENTRY ====================

def run(in_dir: Path, out_root: Path):
    """Запуск unpacker"""
    in_dir = Path(in_dir)
    out_root = Path(out_root)
    
    if not in_dir.exists():
        print(f"ERROR: INPUT_DIR_NOT_FOUND: {in_dir}", file=sys.stderr)
        sys.exit(1)
    
    # Структура выходной директории
    att_dir = out_root / "attachments"
    body_dir = out_root / "body"
    att_dir.mkdir(parents=True, exist_ok=True)
    body_dir.mkdir(parents=True, exist_ok=True)
    
    manifest_path = out_root / "manifest.ndjson"
    
    with open(manifest_path, "a", encoding="utf-8") as manifest:
        eml_files = sorted(in_dir.glob("*.eml"))
        
        for eml_path in eml_files:
            try:
                process_eml(eml_path, att_dir, body_dir, manifest)
            except Exception as e:
                manifest.write(json.dumps({
                    "source": eml_path.name,
                    "type": "error",
                    "error": str(e)
                }, ensure_ascii=False) + "\n")
                manifest.flush()
                os.fsync(manifest.fileno())

def main():
    if len(sys.argv) < 3:
        print("usage: mail_unpacker_v1.py <in_dir> <out_dir>")
        print("")
        print("  in_dir   : directory with .eml files (from mail_intake)")
        print("  out_dir  : output directory")
        print("             will create:")
        print("               out_dir/attachments/  - extracted files")
        print("               out_dir/body/         - text/html/plain parts")
        print("               out_dir/manifest.ndjson - NDJSON log")
        sys.exit(1)
    
    in_dir = sys.argv[1]
    out_dir = sys.argv[2]
    
    run(in_dir, out_dir)

if __name__ == "__main__":
    main()
