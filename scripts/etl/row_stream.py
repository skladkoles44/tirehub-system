def row_iterator(sheet):
    for row in sheet.iter_rows(values_only=True):
        yield row
