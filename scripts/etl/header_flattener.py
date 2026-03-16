def header_flattener(headers):
    return [str(h) if h is not None else "" for h in headers]
