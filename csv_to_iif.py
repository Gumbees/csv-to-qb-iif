#!/usr/bin/env python3
import csv
import sys
from datetime import datetime
from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP


def format_date(date_str: str) -> str:
    """Return date in MM/DD/YYYY. Be liberal in what we accept.

    - Accepts many common formats (with/without time, dashes/slashes, ISO, month names)
    - If empty/unparseable, defaults to today's date
    """
    if not date_str:
        return datetime.now().strftime("%m/%d/%Y")

    candidate = date_str.strip()

    known_formats = [
        "%m/%d/%Y %I:%M %p",
        "%m/%d/%Y %H:%M",
        "%m/%d/%Y",
        "%m/%d/%y",
        "%m-%d-%Y",
        "%m-%d-%y",
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d",
        "%b %d, %Y",
        "%b %d %Y",
        "%B %d, %Y",
        "%B %d %Y",
    ]

    for fmt in known_formats:
        try:
            parsed = datetime.strptime(candidate, fmt)
            return parsed.strftime("%m/%d/%Y")
        except ValueError:
            pass

    # Try first token (e.g., drop time)
    first_token = candidate.split()[0]
    for fmt in known_formats:
        try:
            parsed = datetime.strptime(first_token, fmt)
            return parsed.strftime("%m/%d/%Y")
        except ValueError:
            pass

    # Try replacing dashes with slashes in case of minor delimiter differences
    alt = first_token.replace("-", "/")
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y/%m/%d"):
        try:
            parsed = datetime.strptime(alt, fmt)
            return parsed.strftime("%m/%d/%Y")
        except ValueError:
            pass

    # Best-effort ISO parse if present
    try:
        parsed = datetime.fromisoformat(candidate.replace("Z", ""))
        return parsed.strftime("%m/%d/%Y")
    except Exception:
        pass

    # Fallback to today's date to keep IIF valid
    return datetime.now().strftime("%m/%d/%Y")


def extract_date_value(row: dict) -> str:
    """Find a date-like field from common header names, return raw string or empty."""
    possible_keys = (
        "Date",
        "TxnDate",
        "Transaction Date",
        "PO Date",
        "DocDate",
        "PODate",
        "DATE",
    )
    for key in possible_keys:
        if key in row and row.get(key):
            return str(row.get(key)).strip()
    return ""


def sanitize_tsv(value: str) -> str:
    if value is None:
        return ""
    return str(value).replace("\t", " ").replace("\r", " ").replace("\n", " ")


def convert_csv_to_iif(csv_file: str, output_file: str) -> None:
    with open(csv_file, "r", encoding="utf-8-sig", newline="") as input_stream:
        reader = csv.DictReader(input_stream)
        rows = list(reader)

    with open(output_file, "w", encoding="utf-8", newline="") as out:
        # Align exactly with provided purchase_order.iif headers and columns
        out.write("!TRNS\tTRNSID\tTRNSTYPE\tDATE\tACCNT\tNAME\tCLASS\tAMOUNT\tDOCNUM\tMEMO\tCLEAR\tTOPRINT\tNAMEISTAXABLE\tADDR1\n")
        out.write("!SPL\tSPLID\tTRNSTYPE\tDATE\tACCNT\tNAME\tCLASS\tAMOUNT\tDOCNUM\tMEMO\tCLEAR\tQNTY\tPRICE\tINVITEM\n")
        out.write("!ENDTRNS\t\t\t\t\t\t\t\t\t\t\t\t\t\n")

        groups = defaultdict(list)
        for row in rows:
            vendor = sanitize_tsv(row.get("Vendor", "").strip())
            ref_num = sanitize_tsv(row.get("RefNumber", "").strip())
            raw_date = extract_date_value(row)
            date = format_date(raw_date)
            if vendor and ref_num:
                groups[(vendor, ref_num, date)].append(row)

        for (vendor, ref_num, date), group_rows in groups.items():
            # Sum line amounts for the PO
            total_amount = Decimal("0.00")
            computed_lines = []
            for row in group_rows:
                item = sanitize_tsv(row.get("Item", "").strip())
                description = sanitize_tsv(row.get("Description", "").strip())
                qty_str = row.get("Qty", "").strip()
                cost_str = row.get("Cost", "").strip()
                try:
                    quantity = Decimal(qty_str) if qty_str else Decimal("0")
                except Exception:
                    quantity = Decimal("0")
                try:
                    unit_cost = Decimal(cost_str) if cost_str else Decimal("0")
                except Exception:
                    unit_cost = Decimal("0")
                line_amount = (quantity * unit_cost).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                unit_cost = unit_cost.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                total_amount += line_amount
                computed_lines.append({
                    "item": item,
                    "description": description,
                    "quantity": quantity,
                    "unit_cost": unit_cost,
                    "line_amount": line_amount,
                })

            memo_text = sanitize_tsv(ref_num) if ref_num else ""

            # TRNS row (Purchase Orders, negative amount)
            out.write(
                "\t".join([
                    "TRNS",
                    "",  # TRNSID blank
                    "PURCHORD",
                    date,
                    "Purchase Orders",
                    vendor,
                    "",  # CLASS blank
                    f"-{total_amount:.2f}",
                    memo_text,  # DOCNUM as PO number
                    "",  # MEMO blank
                    "N",  # CLEAR
                    "Y",  # TOPRINT per sample
                    "N",  # NAMEISTAXABLE
                    "",   # ADDR1 blank
                ]) + "\n"
            )

            # SPL rows for each item line (Inventory Asset, positive amounts)
            for line in computed_lines:
                out.write(
                    "\t".join([
                        "SPL",
                        "",  # SPLID blank
                        "PURCHORD",
                        date,
                        "Inventory Asset",
                        "",  # NAME blank (Customer:Job)
                        "",  # CLASS blank
                        f"{line['line_amount']:.2f}",
                        "",  # DOCNUM blank
                        line["description"] or line["item"],  # MEMO
                        "N",  # CLEAR
                        str(line["quantity"]),
                        f"{line['unit_cost']:.2f}",
                        line["item"],
                    ]) + "\n"
                )

            out.write("ENDTRNS\t\t\t\t\t\t\t\t\t\t\t\t\t\n")


def main() -> None:
    if len(sys.argv) != 3:
        print("Usage: python csv_to_iif.py input.csv output.iif")
        sys.exit(1)

    csv_file = sys.argv[1]
    output_file = sys.argv[2]

    try:
        convert_csv_to_iif(csv_file, output_file)
        print(f"Successfully converted {csv_file} to {output_file}")
    except Exception as exc:
        print(f"Error: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()