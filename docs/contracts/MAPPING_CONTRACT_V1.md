# Mapping contract v1

Purpose
Mapping contract defines how supplier file columns are mapped to canonical parsed fields.

This contract allows parser framework to remain generic and reusable across suppliers.

Structure

mapping_version
version of mapping rules.

header_detection
rules used to detect header rows.

column_detection
rules for identifying columns.

Fields

sku_column
column that identifies product.

price_column
column containing base price.

qty_columns
one or multiple stock columns.

warehouse_column
optional warehouse identifier column.

currency_column
optional currency field.

Policies

mapping rules must not contain business interpretation.

Parser uses mapping only to extract structural fields.

Versioning

mapping_version must increment whenever detection rules change.
