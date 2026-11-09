# FastAPI producer that accepts job requests and publishes them
# to a Redis Stream for asynchronous processing by workers.

from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
from models.job import JobRequest
from shared.redis_client import redis_client
from db.models import Job
from db.database import get_db


import uuid
app = FastAPI(
    title="Distributed Job Queue",
    description="Asynchronous job processing using Redis Streams"
)


@app.get("/health")
def health():
    redis_client.ping()
    return {"Status" : "OK", "redis" : "connected"}

@app.post("/jobs", status_code=202)
def create_job(job: JobRequest, db: Session = Depends(get_db)):
    # Create unique identifier for each job in redis stream
    job_id = str(uuid.uuid4())

    # Create PostgreSQL record
    db_job = Job(
        job_id=job_id,
        filename=job.filename
    )

    db.add(db_job)
    db.commit()

    # xadd natively creates a stream_id and adds it to the stream log
    redis_client.xadd(
        "jobs",
        {
            "job_id": job_id,
            "filename": job.filename
        }
    )

    # Return immediately without waiting for a worker to process the job
    # The client can use the job ID to check its status later
    return {
        "job_id": job_id,
        "status": "queued"
    }
