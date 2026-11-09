
import csv
import os
from pathlib import Path
from uuid import uuid4


def process_csv(filename, job_id):
    input_dir = Path("./tests")
    output_dir = Path("./output")

    # Find the input CSV
    if not input_dir.is_dir():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    file_match = next(
        (path for path in input_dir.rglob("*")
         if path.is_file() and path.name == filename),
        None
    )

    if file_match is None:
        raise FileNotFoundError(f"CSV file not found: {filename}")

    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"cleaned_{job_id}.csv"
    temp_path = output_dir / f"{job_id}.{uuid4().hex}.tmp"

    unique_rows = []
    seen = set()
    total_rows = 0
    missing_values = 0

    # Read CSV and remove duplicates
    with open(file_match, "r", encoding="utf-8-sig", newline="") as file:
        reader = csv.reader(file)
        header = next(reader, None)

        if header is None:
            raise ValueError("CSV file is empty")

        for row in reader:
            total_rows += 1
            row_tuple = tuple(row)

            if row_tuple not in seen:
                seen.add(row_tuple)
                unique_rows.append(row)

                for value in row:
                    if not value.strip():
                        missing_values += 1

                if len(row) < len(header):
                    missing_values += len(header) - len(row)

    # Write atomically
    try:
        with open(temp_path, "w", encoding="utf-8", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(header)
            writer.writerows(unique_rows)

        os.replace(temp_path, output_path)

    finally:
        temp_path.unlink(missing_ok=True)

    unique = len(unique_rows)

    return {
        "total_rows": total_rows,
        "unique_rows": unique,
        "duplicate_rows": total_rows - unique,
        "missing_values": missing_values,
        "output_file": str(output_path)
    }
