
import csv
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

from shared.s3_client import s3_client


def process_csv(filename, job_id):
    bucket = os.environ["S3_BUCKET"]
    output_dir = Path("./output")

    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"cleaned_{job_id}.csv"
    temp_path = output_dir / f"{job_id}.{uuid4().hex}.tmp"

    unique_rows = []
    seen = set()
    total_rows = 0
    missing_values = 0

    # Each attempt downloads its own input; cleanup also runs on failure.
    with TemporaryDirectory(prefix="job-input-") as input_dir:
        local_path = Path(input_dir) / "input.csv"
        s3_client.download_file(
            bucket, f"input/{filename}", str(local_path)
        )

        # Read CSV and remove duplicates.
        with open(local_path, "r", encoding="utf-8-sig", newline="") as file:
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

    # Only return success after S3 has the complete output. Retries reuse the
    # same key, so another attempt replaces this job's object.
    output_s3_key = f"output/cleaned_{job_id}.csv"
    s3_client.upload_file(str(output_path), bucket, output_s3_key)

    unique = len(unique_rows)

    return {
        "total_rows": total_rows,
        "unique_rows": unique,
        "duplicate_rows": total_rows - unique,
        "missing_values": missing_values,
        "output_file": str(output_path),
        "output_s3_key": output_s3_key,
    }
