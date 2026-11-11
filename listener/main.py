"""Read S3 upload notifications from SQS and create processing jobs."""

import json
import logging
import os
import time
from urllib.parse import unquote_plus
from uuid import NAMESPACE_URL, uuid5

import boto3
from dotenv import load_dotenv
from sqlalchemy.exc import IntegrityError

from db.database import SessionLocal
from db.models import Job
from shared.redis_client import redis_client

load_dotenv()

logger = logging.getLogger(__name__)


def create_job(record):
    """Save an input upload in PostgreSQL before publishing it to Redis."""
    if record.get("eventSource") != "aws:s3":
        return None
    if not record.get("eventName", "").startswith("ObjectCreated:"):
        return None

    bucket = record["s3"]["bucket"]["name"]
    object_data = record["s3"]["object"]
    key = unquote_plus(object_data["key"])
    if bucket != os.environ["S3_BUCKET"]:
        return None
    if not key.startswith("input/") or not key.endswith(".csv"):
        return None

    # Keep subdirectories: input/batch/file.csv becomes batch/file.csv.
    filename = key.removeprefix("input/")
    if len(filename) > 255:
        raise ValueError("S3 input filename exceeds the database's 255-character limit")
    sequencer = object_data.get("sequencer")
    if not sequencer:
        raise ValueError("S3 upload notification is missing its sequencer")

    # Redelivery of the same upload must reuse its job ID. A fresh upload to
    # the same key has a different sequencer and creates a new job.
    event_identity = json.dumps([bucket, key, sequencer])
    job_id = str(uuid5(NAMESPACE_URL, event_identity))
    with SessionLocal() as db:
        db_job = db.get(Job, job_id)
        if db_job is None:
            db.add(Job(job_id=job_id, filename=filename))
            try:
                db.commit()
            except IntegrityError:
                # Another listener may have inserted this notification first.
                db.rollback()
                if db.get(Job, job_id) is None:
                    raise
            db_job = db.get(Job, job_id)

        # A queued job may need publication repaired after a Redis failure.
        # Duplicate Redis messages are handled by the worker's locks/checks.
        if db_job.status == "queued":
            redis_client.xadd("jobs", {"job_id": job_id, "filename": filename})

    logger.info("Upload %s is job %s", key, job_id)
    return job_id


def process_message(body):
    """Handle every record before acknowledging the enclosing SQS message."""
    notification = json.loads(body)
    if notification.get("Event") == "s3:TestEvent":
        return
    records = notification["Records"]
    if not isinstance(records, list) or not records:
        raise ValueError("Expected a nonempty list of S3 notification records")
    for record in records:
        create_job(record)


def poll_once(sqs_client, queue_url):
    response = sqs_client.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=20,
        VisibilityTimeout=60,
    )
    for message in response.get("Messages", []):
        try:
            process_message(message["Body"])
            # Keep failed notifications in SQS so they can be delivered again.
            sqs_client.delete_message(
                QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"]
            )
        except Exception:
            logger.exception("Notification failed; leaving it in SQS for retry")


def main():
    logging.basicConfig(level=logging.INFO)
    queue_url = os.environ["SQS_QUEUE_URL"]
    os.environ["S3_BUCKET"]  # Fail at startup if the input bucket is missing.
    sqs_client = boto3.client("sqs")
    logger.info("Listening for S3 uploads")
    while True:
        try:
            poll_once(sqs_client, queue_url)
        except Exception:
            logger.exception("Could not poll SQS; retrying in five seconds")
            time.sleep(5)


if __name__ == "__main__":
    main()
