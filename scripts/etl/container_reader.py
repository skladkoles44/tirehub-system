from pathlib import Path
from openpyxl import load_workbook

def container_reader(path: Path):
    wb=load_workbook(path,read_only=True,data_only=True)
    for sheet_name in wb.sheetnames:
        yield sheet_name,wb[sheet_name]
