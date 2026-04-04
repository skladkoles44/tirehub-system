from .runner_donor import run as donor_run

def run(inp, out, file_hash=None, supplier_id=None):
    return donor_run(inp, out, file_hash=file_hash, supplier_id=supplier_id)
