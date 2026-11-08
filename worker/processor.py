
import csv
from pathlib import Path

def process_csv(filename):
    directory = Path("./tests")
    file_match = next(directory.rglob(filename), None)

    if not file_match:
        raise FileNotFoundError(f"CSV file not found: {filename}")

    # Create output directory if it doesn't exist
    output_dir = Path("./output")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"cleaned_{file_match.name}"

    unique_rows = []
    seen = set()

    total_rows = 0
    missing_values = 0

    # Read CSV and remove duplicates
    with open(file_match, mode="r", encoding="utf-8-sig", newline="") as file:
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

                # Count missing values in unique rows
                for value in row:
                    if not value.strip():
                        missing_values += 1

                # Count missing trailing columns
                if len(row) < len(header):
                    missing_values += len(header) - len(row)

    # Write cleaned data without modifying original
    with open(output_path, mode="w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(header)
        writer.writerows(unique_rows)

    unique = len(unique_rows)
    duplicate = total_rows - unique

    return {
        "total_rows": total_rows,
        "unique_rows": unique,
        "duplicate_rows": duplicate,
        "missing_values": missing_values,
        "output_file": str(output_path)
    }
