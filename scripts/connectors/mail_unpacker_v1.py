#!/usr/bin/env python3
"""
mail_unpacker_v1.py — чистый, без дублирования
"""

import sys
import os
import json
import hashlib
import email
import re
from pathlib import Path
from email import policy

MAX_EML_SIZE = 50 * 1024 * 1024
MAX_PART_SIZE = 20 * 1024 * 1024


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def clean_orig_name(name: str) -> str:
    """Минимальная очистка имени, не трогаем расширение"""
    if not name:
        return name
    
    # Сохраняем расширение
    ext = ""
    for e in ['.xlsx', '.xls', '.xlsm', '.ods', '.csv']:
        if name.lower().endswith(e):
            ext = e
            name = name[:-len(e)]
            break
    
    # Убираем мусор
    name = re.sub(r'_[a-f0-9]{6}$', '', name)
    name = re.sub(r'[_ ]*XLS[_ ]*', '', name, flags=re.IGNORECASE)
    name = re.sub(r'[_ ]*XLSX[_ ]*', '', name, flags=re.IGNORECASE)
    name = re.sub(r'___.*$', '', name)
    name = re.sub(r'\._+', '.', name)
    name = re.sub(r'__+', '_', name)
    name = re.sub(r'_\.', '.', name)
    name = name.strip('_')
    
    # Добавляем расширение
    if ext:
        name = name + ext
    elif '.' not in name:
        name = name + '.bin'
    
    return name
    
    # Сохраняем расширение
    ext = ""
    for e in ['.xlsx', '.xls', '.xlsm', '.ods', '.csv']:
        if e in name.lower():
            ext = e
            break
    
    # Убираем хеш-суффиксы
    name = re.sub(r'_[a-f0-9]{6}$', '', name)
    
    # Убираем суффиксы _XLS, _XLSX
    name = re.sub(r'[_ ]*XLS[_ ]*', '', name, flags=re.IGNORECASE)
    name = re.sub(r'[_ ]*XLSX[_ ]*', '', name, flags=re.IGNORECASE)
    
    # Убираем лишние символы
    name = re.sub(r'___.*$', '', name)
    name = re.sub(r'\._+', '.', name)
    name = re.sub(r'__+', '_', name)
    name = re.sub(r'_\.', '.', name)
    
    # Добавляем расширение обратно
    if ext and not name.endswith(ext):
        name = name + ext
    
    # Если всё ещё нет расширения
    if '.' not in name:
        name = name + '.bin'
    
    return name
    
    # Убираем суффиксы типа _XLS_, _XLSX_
    name = re.sub(r'[_ ]*XLS[_ ]*', '', name, flags=re.IGNORECASE)
    name = re.sub(r'[_ ]*XLSX[_ ]*', '', name, flags=re.IGNORECASE)
    name = re.sub(r'[_ ]*ODS[_ ]*', '', name, flags=re.IGNORECASE)
    name = re.sub(r'[_ ]*CSV[_ ]*', '', name, flags=re.IGNORECASE)
    
    # Убираем хеш-суффиксы в конце
    name = re.sub(r'_[a-f0-9]{6}$', '', name)
    
    # Убираем двойные подчёркивания
    name = re.sub(r'__+', '_', name)
    
    return name
    
    # Убираем суффиксы типа _XLS_, _XLSX_
    name = re.sub(r'[_ ]*XLS[_ ]*', '', name, flags=re.IGNORECASE)
    name = re.sub(r'[_ ]*XLSX[_ ]*', '', name, flags=re.IGNORECASE)
    name = re.sub(r'[_ ]*ODS[_ ]*', '', name, flags=re.IGNORECASE)
    name = re.sub(r'[_ ]*CSV[_ ]*', '', name, flags=re.IGNORECASE)
    
    # Убираем хеш-суффиксы в конце
    name = re.sub(r'_[a-f0-9]{6}$', '', name)
    
    # Убираем двойные подчёркивания
    name = re.sub(r'__+', '_', name)
    
    return name
    name = re.sub(r'___.*$', '', name)
    name = re.sub(r'\._+', '.', name)
    name = re.sub(r'_[a-f0-9]{6}$', '', name)
    name = re.sub(r'[_ ]*XLS[_ ]*', '', name, flags=re.IGNORECASE)
    name = re.sub(r'[_ ]*XLSX[_ ]*', '', name, flags=re.IGNORECASE)
    name = re.sub(r'[_ ]*ODS[_ ]*', '', name, flags=re.IGNORECASE)
    name = re.sub(r'[_ ]*CSV[_ ]*', '', name, flags=re.IGNORECASE)
    name = re.sub(r'__+', '_', name)
    name = re.sub(r'_\.', '.', name)
    for ext in ['.xls', '.xlsx', '.xlsm', '.ods', '.csv']:
        if ext in name.lower():
            pos = name.lower().find(ext)
            name = name[:pos + len(ext)]
            break
    if '.' not in name:
        name = name + '.bin'
    return name


def safe_filename(name: str, file_hash: str = None) -> str:
    """Санитизирует имя файла, сохраняя расширение"""
    if not name:
        return "unknown"
    
    # Сохраняем расширение
    ext = ""
    for e in ['.xlsx', '.xls', '.xlsm', '.ods', '.csv']:
        if name.lower().endswith(e):
            ext = e
            name = name[:-len(e)]
            break
    
    # Убираем хеш из начала
    name = re.sub(r'^[a-f0-9]{12}_', '', name)
    
    # Очищаем имя
    clean = "".join(c if c.isalnum() or c in "._-" else "_" for c in name)
    clean = re.sub(r'_+', '_', clean)
    clean = clean.strip('_')
    
    # Добавляем расширение (один раз)
    if ext:
        clean = clean + ext
    elif file_hash:
        clean = f"{clean}_{file_hash[:8]}.bin"
    else:
        clean = clean + '.bin'
    
    return clean


def fsync_directory(path: Path):
    try:
        fd = os.open(str(path), os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except Exception:
        pass


def write_if_not_exists(path: Path, data: bytes):
    if path.exists():
        return
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.rename(tmp, path)
    fsync_directory(path.parent)


def process_eml(eml_path: Path, att_dir: Path, body_dir: Path, manifest):
    size = eml_path.stat().st_size
    if size > MAX_EML_SIZE:
        manifest.write(json.dumps({"source": eml_path.name, "type": "skip", "reason": "eml_too_large"}) + "\n")
        return
    
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
            continue
        
        part_hash = sha256_bytes(payload)
        ctype = part.get_content_type()
        filename = part.get_filename()
        content_disposition = part.get("Content-Disposition", "")
        is_attachment = "attachment" in content_disposition.lower()
        
        if filename or is_attachment:
            orig_name = clean_orig_name(filename or "unknown")
            fname = safe_filename(orig_name, part_hash[:8])
            out_name = f"{part_hash[:12]}_{fname}"
            out_path = att_dir / out_name
            write_if_not_exists(out_path, payload)
            manifest.write(json.dumps({
                "source": eml_path.name, "eml_hash": eml_hash, "type": "attachment",
                "file": out_name, "sha256": part_hash, "content_type": ctype,
                "orig_name": orig_name, "content_disposition": content_disposition
            }) + "\n")
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
                "source": eml_path.name, "eml_hash": eml_hash, "type": "body",
                "file": out_name, "sha256": part_hash, "content_type": ctype, "part_index": part_index
            }) + "\n")
        part_index += 1


def run(in_dir: Path, out_root: Path):
    in_dir = Path(in_dir)
    out_root = Path(out_root)
    if not in_dir.exists():
        print(f"ERROR: INPUT_DIR_NOT_FOUND: {in_dir}", file=sys.stderr)
        sys.exit(1)
    
    att_dir = out_root / "attachments"
    body_dir = out_root / "body"
    att_dir.mkdir(parents=True, exist_ok=True)
    body_dir.mkdir(parents=True, exist_ok=True)
    
    manifest_path = out_root / "manifest.ndjson"
    with open(manifest_path, "a", encoding="utf-8") as manifest:
        for eml_path in sorted(in_dir.glob("*.eml")):
            try:
                process_eml(eml_path, att_dir, body_dir, manifest)
            except Exception as e:
                manifest.write(json.dumps({"source": eml_path.name, "type": "error", "error": str(e)}) + "\n")
                manifest.flush()
                os.fsync(manifest.fileno())


def main():
    if len(sys.argv) < 3:
        print("usage: mail_unpacker_v1.py <in_dir> <out_dir>")
        sys.exit(1)
    run(sys.argv[1], sys.argv[2])


if __name__ == "__main__":
    main()
