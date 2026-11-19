"""Measure CSV job throughput through the configured S3/SQS test stack."""

import argparse
import csv
import hashlib
import io
import json
import math
import os
import platform
import statistics
import subprocess
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

POLL_SECONDS = 0.2
TERMINAL = {"completed", "dead"}


def make_payload(rows):
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(["id", "value"])
    for i in range(rows):
        writer.writerow([i // 2, f"value-{i // 2}"])
    return buffer.getvalue().encode("utf-8")


def environment(region, db_version, redis_version):
    lockfile = Path(__file__).with_name("uv.lock")
    try:
        commit = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True, stderr=subprocess.DEVNULL
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        commit = None
    return dict(
        commit=commit,
        lock_sha256=hashlib.sha256(lockfile.read_bytes()).hexdigest() if lockfile.exists() else None,
        os=platform.platform(),
        cpu=platform.processor(),
        logical_cpus=os.cpu_count(),
        python=platform.python_version(),
        postgres=db_version,
        redis=redis_version,
        aws_region=region,
    )


def run_batch(s3, bucket, read_jobs, batch, payload, timeout):
    prefix = f"bench-{uuid4().hex}"
    filenames = [f"{prefix}-{i}.csv" for i in range(batch)]
    started_utc = datetime.now(timezone.utc).isoformat()
    uploaded = 0
    jobs = []
    error = None
    start = time.perf_counter()
    deadline = start + timeout
    try:
        for filename in filenames:
            if time.perf_counter() >= deadline:
                break
            s3.put_object(Bucket=bucket, Key=f"input/{filename}", Body=payload)
            uploaded += 1
        while True:
            jobs = read_jobs(filenames)
            if {job.filename for job in jobs if job.status in TERMINAL} == set(filenames):
                break
            remaining = deadline - time.perf_counter()
            if remaining <= 0:
                break
            time.sleep(min(POLL_SECONDS, remaining))
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"

    elapsed = time.perf_counter() - start
    states = Counter(job.status for job in jobs)
    completed = states["completed"]
    missing = batch - len({job.filename for job in jobs})
    timed_out = elapsed >= timeout
    return dict(
        started_utc=started_utc,
        prefix=prefix,
        elapsed_sec=elapsed,
        uploaded=uploaded,
        completed=completed,
        missing=missing,
        states=dict(states),
        retries=sum(max(0, job.attempts - 1) for job in jobs),
        jobs_sec=completed / elapsed,
        timed_out=timed_out,
        error=error,
        ok=uploaded == completed == len(jobs) == batch and missing == 0
        and error is None and not timed_out,
        job_details=[
            dict(filename=job.filename, job_id=job.job_id, status=job.status,
                 attempts=job.attempts, last_error=job.last_error)
            for job in jobs
        ],
    )


def print_summary(path):
    groups = defaultdict(list)
    for line in path.read_text(encoding="utf-8").splitlines():
        result = json.loads(line)
        groups[result["batch"], result["rows"], result["workers"]].append(result)

    medians = {}
    for key, runs in groups.items():
        expected = runs[0]["repeats"]
        if len(runs) == expected and all(run["ok"] and run["repeats"] == expected for run in runs):
            if {run["repetition"] for run in runs} == set(range(1, expected + 1)):
                medians[key] = statistics.median(run["jobs_sec"] for run in runs)

    print("| Batch size (files) | CSV data rows/file | Workers | Median completed jobs/sec | Speedup vs 1 worker |")
    print("| --- | --- | --- | --- | --- |")
    for batch, rows, workers in sorted(groups):
        rate = medians.get((batch, rows, workers))
        baseline = medians.get((batch, rows, 1))
        rate_text = f"{rate:.3f}" if rate is not None else "Incomplete"
        speedup = f"{rate / baseline:.2f}x" if rate is not None and baseline else "Pending"
        print(f"| {batch:,} | {rows:,} | {workers} | {rate_text} | {speedup} |")


def measure(args):
    from dotenv import load_dotenv

    load_dotenv()
    bucket = os.getenv("S3_BUCKET")
    if not bucket or not os.getenv("SQS_QUEUE_URL"):
        raise SystemExit("Set S3_BUCKET and SQS_QUEUE_URL for your test stack before benchmarking.")

    import boto3
    from botocore.config import Config
    from sqlalchemy import select, text

    from db.database import SessionLocal
    from db.models import Job
    from shared.redis_client import redis_client

    s3 = boto3.client(
        "s3", config=Config(connect_timeout=10, read_timeout=30, retries={"max_attempts": 2})
    )
    with SessionLocal() as db:
        db_version = db.scalar(text("SHOW server_version"))
        unfinished = db.scalar(select(Job.job_id).where(~Job.status.in_(TERMINAL)).limit(1))
    if unfinished is not None:
        raise SystemExit("The database contains unfinished jobs. Drain the test stack before benchmarking.")
    if not redis_client.exists("jobs") or not any(
        group["name"] == "csv-workers" for group in redis_client.xinfo_groups("jobs")
    ):
        raise SystemExit("Start the CSV worker pool before benchmarking.")
    redis_version = redis_client.info("server")["redis_version"]
    metadata = environment(s3.meta.region_name, db_version, redis_version)

    def read_jobs(filenames):
        with SessionLocal() as db:
            return list(db.scalars(select(Job).where(Job.filename.in_(filenames))))

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("a", encoding="utf-8") as output:
        for batch in args.batches:
            for rows in args.rows:
                payload = make_payload(rows)
                for repetition in range(1, args.repeats + 1):
                    result = run_batch(s3, bucket, read_jobs, batch, payload, args.timeout)
                    result.update(
                        batch=batch, rows=rows, bytes_per_file=len(payload),
                        workers=args.workers, repetition=repetition, repeats=args.repeats,
                        timeout_sec=args.timeout, poll_sec=POLL_SECONDS, environment=metadata,
                    )
                    output.write(json.dumps(result) + "\n")
                    output.flush()
                    outcome = "ok" if result["ok"] else "failed"
                    print(
                        f"{batch} files x {rows} rows, {args.workers} workers, "
                        f"run {repetition}/{args.repeats}: {result['completed']}/{batch} completed, "
                        f"{result['jobs_sec']:.3f} jobs/sec ({outcome})",
                        flush=True,
                    )
                    if not result["ok"]:
                        raise SystemExit(f"Batch failed. See {args.out} for job states and errors.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workers", type=int, default=1, help="worker count for labeling results")
    parser.add_argument("--batches", type=int, nargs="+", default=[5], help="files per batch")
    parser.add_argument("--rows", type=int, nargs="+", default=[100], help="data rows per file")
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--timeout", type=float, default=120, help="seconds per batch")
    parser.add_argument("--out", type=Path, required=True, help="JSONL results file")
    parser.add_argument("--summary", action="store_true", help="print a Markdown results table")
    args = parser.parse_args()
    if args.summary:
        print_summary(args.out)
        return
    if min(args.workers, args.repeats, *args.batches, *args.rows) <= 0:
        parser.error("workers, repeats, batch sizes, and row counts must be positive")
    if not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error("timeout must be finite and positive")
    measure(args)


if __name__ == "__main__":
    main()
