def good_emitter(parsed, flags, source_file, sheet_name, row_idx):
    return {"status": "good", "data": parsed, "flags": flags}

def reject_emitter(row, flags, source_file, sheet_name, row_idx):
    return {"status": "reject", "row": row, "flags": flags}
