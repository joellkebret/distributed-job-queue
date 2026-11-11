# FastAPI endpoints for checking job state and Redis health.

from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy import select
from sqlalchemy.orm import Session
from shared.redis_client import redis_client
from db.models import Job
from db.database import get_db


app = FastAPI(
    title="Distributed Job Queue",
    description="Asynchronous job processing using Redis Streams"
)


@app.get("/health")
def health():
    redis_client.ping()
    return {"Status" : "OK", "redis" : "connected"}

@app.get("/jobs/{job_id}")
def get_job(job_id: str, db: Session = Depends(get_db)):

    match = db.scalars(
        select(Job)
        .where(Job.job_id == job_id)        
    ).first()

    if not match:
        raise HTTPException(status_code=404, detail="Job not found")

    return match
