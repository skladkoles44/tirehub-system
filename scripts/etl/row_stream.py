from scripts.etl.header_detector import detect_header

def row_iterator(sheet, start_idx=None, scan_limit=20):
    if start_idx is None:
        info = detect_header(sheet, scan_limit=scan_limit)
        start_idx = info["header_idx"] + 1
        if info.get("skip_index_row"):
            start_idx += 1

    for i, row in enumerate(sheet.iter_rows(values_only=True)):
        if i < start_idx:
            continue
        yield row
