"""
csv_to_json.py
--------------
Converts a CSV file to a JSON file, preserving all columns.

Usage:
    python csv_to_json.py input.csv               # Output: input.json
    python csv_to_json.py input.csv output.json   # Custom output path

As a function:
    from csv_to_json import csv_to_json
    csv_to_json("data.csv", "data.json")
"""

import csv
import json
import sys
from pathlib import Path


def csv_to_json(input_path: str, output_path: str = None, indent: int = 2) -> str:
    """
    Convert a CSV file to a JSON file (list of row dicts).

    Args:
        input_path  : Path to the input CSV file.
        output_path : Path for the output JSON file.
                      Defaults to same name/location as input with .json extension.
        indent      : JSON indentation level (default 2).

    Returns:
        The output file path as a string.
    """
    input_path = Path(input_path).resolve()

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    output_path = Path(output_path).resolve() if output_path else input_path.with_suffix(".json")

    # Read CSV
    with open(input_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        columns = reader.fieldnames or []

    if not rows:
        print(f"[Warning] CSV file is empty or has only headers: {input_path}")

    # Write JSON
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=indent, ensure_ascii=False)

    print(f"✓ Converted : {input_path}")
    print(f"  Columns   : {columns}")
    print(f"  Rows      : {len(rows)}")
    print(f"  Output    : {output_path}")

    return str(output_path)


# if __name__ == "__main__":
#     if len(sys.argv) < 2:
#         print("Usage: python csv_to_json.py input.csv [output.json]")
#         sys.exit(1)

#     csv_to_json(
#         input_path=sys.argv[1],
#         output_path=sys.argv[2] if len(sys.argv) > 2 else None,
#     )

if __name__ == "__main__":
    csv_to_json(
        # input_path="C:/Users/yourname/Documents/your_file.csv",  # ← your CSV path here
        input_path="currency.csv"
    )
