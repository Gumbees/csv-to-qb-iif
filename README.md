csv-to-qb-iif
================

Convert purchase order CSV exports into QuickBooks IIF Purchase Orders.

Features
--------
- Robust date parsing from multiple columns (e.g., `PO Date`, `Date`) and formats
- Groups lines by Vendor + RefNumber + Date to produce one TRNS with multiple SPLs
- Sanitizes tab/newline characters for IIF safety

Requirements
------------
- Python 3.8+

Usage
-----
```bash
python csv_to_iif.py "Purchase_Order Export-3.csv" "output_po_exact_format_3.iif"
```

Notes
-----
- CSV files and generated `.iif` files are intentionally ignored by Git via `.gitignore`.
- If the CSV lacks a date field, the script defaults to today's date. If you want a fixed date, we can add a CLI flag (e.g., `--date MM/DD/YYYY`).

License
-------
Proprietary or internal use. Update as needed.


