csv-to-qb-iif
================

Convert CSV exports into QuickBooks IIF Bills (with optional Streamlit GUI).

Features
--------
- Robust date parsing from multiple columns (e.g., `PO Date`, `Date`) and formats
- Groups lines by Vendor + RefNumber + Date to produce one TRNS with multiple SPLs
- Sanitizes tab/newline characters for IIF safety

Requirements
------------
- Python 3.8+

CLI Usage
---------
```bash
python csv_to_iif.py "Purchase_Order Export-3.csv" "bills_output.iif"
```

GUI (Streamlit)
---------------
1. Install deps: `pip install -r requirements.txt`
2. Run: `streamlit run app.py`
3. Drag-and-drop a CSV, preview Bills in a gallery or list view, and download the IIF.

Notes
-----
- CSV files and generated `.iif` files are intentionally ignored by Git via `.gitignore`.
- If the CSV lacks a date field, the script defaults to today's date. If you want a fixed date, we can add a CLI flag (e.g., `--date MM/DD/YYYY`).
- Item lines are exported on SPL with `QNTY`, `PRICE`, and `INVITEM`. `MEMO` is the CSV `Description` only (left blank if missing).

Electron Desktop App
--------------------
- Dev run: `npm start`
- Build Windows installer: `npm run build`
- Command-line mode (headless) using the packaged exe or dev:
  - Dev: `npx electron . -- --input "Purchase_Order Export-4.csv" --output "bills_output.iif"`
  - Packaged exe: `csv-to-qb-iif-gui.exe --input "input.csv" --output "out.iif"`
  - Also supports positional args: `csv-to-qb-iif-gui.exe input.csv out.iif`

License
-------
Proprietary or internal use. Update as needed.


