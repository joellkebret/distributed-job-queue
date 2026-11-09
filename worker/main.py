# Redis Stream consumer that retrieves and processes queued jobs
# as part of a consumer group.

from shared.redis_client import redis_client
from worker.processor import process_csv
from redis.exceptions import ResponseError
from db.database import SessionLocal
from db.models import Job

import redis
import time

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


# Always listen for new jobs
while True:
    try:
        response = redis_client.xreadgroup(
            groupname="csv-workers",
            consumername="worker-1",
            streams={"jobs": ">"},
            count=1,
            block=2000
        )

        if not response:
            continue

        # Unpack the response
        for stream, messages in response:
            for message_id, message_data in messages:

                print(f"Got Message: {message_data}")

                # Extract job information from Redis
                job_id = message_data["job_id"]

                # Open database session
                with SessionLocal() as db:
                    db_job = db.get(Job, job_id)

                    if db_job is None:
                        print(f"Job {job_id} not found in PostgreSQL")
                        continue

                    # Avoid processing an already completed job
                    if db_job.status == "completed":
                        redis_client.xack(
                            "jobs", "csv-workers", message_id
                        )
                        continue

                    # Get filename from PostgreSQL
                    filename = db_job.filename

                    try:
                        # Mark job as processing
                        db_job.status = "processing"
                        db_job.attempts += 1
                        db.commit()

                        # Process the CSV file
                        results = process_csv(filename)

                        # Mark job as completed
                        db_job.status = "completed"
                        db.commit()

                        print(f"Completed job {job_id}: {results}")

                        # Acknowledge only after successful processing
                        redis_client.xack(
                            "jobs", "csv-workers", message_id
                        )

                        print(f"Acknowledged {job_id} processing {filename}")

                    except Exception as e:
                        db.rollback()

                        # Record the failure in PostgreSQL
                        db_job.status = "failed"
                        db.commit()

                        print(f"Failed job {job_id}: {e}")

                        # Leave message pending for future recovery

    except redis.exceptions.ConnectionError:
        print("Error: Could not connect to Redis.")
        time.sleep(1)

    except ResponseError as e:
        print(f"Redis error: {e}")
        time.sleep(1)