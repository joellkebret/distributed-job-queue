# Redis Stream consumer that retrieves and processes queued jobs
# as part of a consumer group.

from shared.redis_client import redis_client
from worker.processor import process_csv
from worker.dlq import send_to_dlq, mark_dead, reconcile_dead_jobs
from redis.exceptions import ResponseError
from db.database import SessionLocal, engine
from sqlalchemy import select, text
from sqlalchemy.exc import SQLAlchemyError
from db.models import Job

import redis
import time
from uuid import uuid4
from datetime import datetime, timedelta, timezone

CONSUMER_NAME = f"worker-{uuid4().hex}"
MAX_ATTEMPTS = 3
BASE_DELAY = 5
MAX_DELAY = 60
DLQ_RECONCILE_INTERVAL = 30

def schedule_retries():
    """Publish due retries before committing their queued state.

    A crash after XADD can publish a duplicate; attempt tokens and the
    processing lock make that duplicate harmless.
    """
    with SessionLocal.begin() as db:
        jobs = db.scalars(
            select(Job)
            .where(
                Job.status == "failed",
                Job.next_retry_at <= datetime.now(timezone.utc),
            )
            .order_by(Job.next_retry_at)
            .limit(100)
            .with_for_update(skip_locked=True)
        ).all()
        for job in jobs:
            redis_client.xadd("jobs", {
                "job_id": job.job_id,
                "retry_attempt": str(job.attempts),
            })
            job.status = "retry_queued"


def process_job(message_id, message_data):
    # Keep a separate transaction open across the processing session's commits.
    # PostgreSQL releases this lock automatically if the worker disconnects.
    with engine.begin() as lock_connection:
        acquired = lock_connection.execute(
            text("SELECT pg_try_advisory_xact_lock(hashtextextended(:job_id, 0))"),
            {"job_id": message_data["job_id"]},
        ).scalar()
        if acquired:
            _process_job(message_id, message_data)
        # A busy job stays pending for crash recovery.


def _process_job(message_id, message_data):
    print(f"Got Message: {message_data}")

    job_id = message_data["job_id"]

    with SessionLocal() as db:
        db_job = db.get(Job, job_id, with_for_update=True)

        if db_job is None:
            print(f"Job {job_id} not found in PostgreSQL")
            return

        # Avoid processing completed or permanently failed jobs
        if db_job.status in ("completed", "dead"):
            if db_job.status == "dead":
                send_to_dlq(db_job)
            redis_client.xack("jobs", "csv-workers", message_id)
            return

        # PostgreSQL already stores this failed job's retry deadline.
        # Acknowledge the old message; schedule_retries() publishes a new one
        # when the deadline arrives.
        if db_job.status == "failed":
            redis_client.xack("jobs", "csv-workers", message_id)
            return

        retry_attempt = message_data.get("retry_attempt")
        if retry_attempt is not None:
            # The message carries the attempt count from when it was scheduled.
            expected = int(retry_attempt)
            valid_attempt = (
                # The job is still waiting for this scheduled retry.
                db_job.status == "retry_queued" and db_job.attempts == expected
            ) or (
                # Processing started and incremented attempts, but may have
                # crashed. Allow the reclaimed message to recover the job.
                db_job.status == "processing" and db_job.attempts > expected
            )
            if not valid_attempt:
                # This message no longer matches the job's retry state.
                redis_client.xack("jobs", "csv-workers", message_id)
                return
        elif db_job.status == "retry_queued":
            # The original pending message must not execute a scheduled retry.
            redis_client.xack("jobs", "csv-workers", message_id)
            return

        if db_job.status == "retry_queued" and db_job.next_retry_at:
            if datetime.now(timezone.utc) < db_job.next_retry_at:
                return

        # Stop jobs that have exhausted their attempts
        # dead jobs go to the dlq
        if db_job.attempts >= MAX_ATTEMPTS:
            mark_dead(db, db_job, db_job.last_error or "Maximum attempts exhausted")
            redis_client.xack("jobs", "csv-workers", message_id)
            return

        filename = db_job.filename

        # Mark job as processing
        db_job.status = "processing"
        db_job.attempts += 1
        db_job.next_retry_at = None
        db.commit()

        try:
            # Process the CSV file
            results = process_csv(filename, job_id)

        except Exception as e:
            db.rollback()
            db_job.last_error = str(e)

            if db_job.attempts >= MAX_ATTEMPTS:
                # No more retries
                mark_dead(db, db_job, e)
                print(f"Job {job_id} exhausted retries: {e}")

            else:
                # Exponential backoff
                delay = min(
                    BASE_DELAY * (2 ** (db_job.attempts - 1)),
                    MAX_DELAY
                )

                db_job.status = "failed"
                db_job.next_retry_at = (
                    datetime.now(timezone.utc)
                    + timedelta(seconds=delay)
                )
                db.commit()

                print(
                    f"Failed job {job_id}: {e}. "
                    f"Next retry in {delay} seconds."
                )

            # The committed deadline survives even if acknowledgment fails.
            redis_client.xack("jobs", "csv-workers", message_id)
            return

        # Mark job as completed
        db_job.status = "completed"
        db_job.next_retry_at = None
        db.commit()

        print(f"Completed job {job_id}: {results}")

        # Acknowledge only after successful processing
        redis_client.xack("jobs", "csv-workers", message_id)

        print(f"Acknowledged {job_id} processing {filename}")


def main(stop_event=None):
    # Create a worker group
    try:
        redis_client.xgroup_create(
            name="jobs",
            groupname="csv-workers",
            id="0",
            mkstream=True
        )
    except ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise

    # Recover pending jobs and listen for new jobs.
    claim_cursor = "0-0"
    next_dlq_reconcile = 0

    # The autoscaler signals retirement without interrupting an active job.
    while stop_event is None or not stop_event.is_set():
        try:
            if time.monotonic() >= next_dlq_reconcile:
                reconcile_dead_jobs(SessionLocal)
                next_dlq_reconcile = time.monotonic() + DLQ_RECONCILE_INTERVAL
            schedule_retries()

            claim_cursor, claimed_messages, *_ = redis_client.xautoclaim(
                name="jobs",
                groupname="csv-workers",
                consumername=CONSUMER_NAME,
                min_idle_time=60_000,
                start_id=claim_cursor,
                count=1
            )

            for message_id, message_data in claimed_messages:
                process_job(message_id, message_data)

            if stop_event is not None and stop_event.is_set():
                break

            response = redis_client.xreadgroup(
                groupname="csv-workers",
                consumername=CONSUMER_NAME,
                streams={"jobs": ">"},
                count=1,
                block=2000
            )

            for stream, messages in response or []:
                for message_id, message_data in messages:
                    process_job(message_id, message_data)

        except redis.exceptions.ConnectionError:
            print("Error: Could not connect to Redis.")
            time.sleep(1)

        except ResponseError as e:
            print(f"Redis error: {e}")
            time.sleep(1)

        except SQLAlchemyError as e:
            print(f"Database error: {e}")
            time.sleep(1)


if __name__ == "__main__":
    main()
