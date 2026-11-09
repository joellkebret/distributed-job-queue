from sqlalchemy import select

from db.models import Job
from shared.redis_client import redis_client

DLQ_STREAM = "jobs:dead"
DLQ_EVENTS = "jobs:dead:events"

# Verify the indexed entry still exists, allowing repair after stream loss.
# No TTL: expiration would duplicate events on the next reconciliation.
PUBLISH_ONCE = """
local existing = redis.call('HGET', KEYS[2], ARGV[1])
if existing then
    local entries = redis.call('XRANGE', KEYS[1], existing, existing)
    if #entries > 0 then return existing end
end
local id = redis.call('XADD', KEYS[1], '*',
    'event_id', ARGV[1], 'job_id', ARGV[2], 'filename', ARGV[3],
    'attempts', ARGV[4], 'error', ARGV[5], 'failed_at', ARGV[6])
redis.call('HSET', KEYS[2], ARGV[1], id)
return id
"""


def send_to_dlq(job):
    """Publish a committed dead job idempotently across workers and restarts."""
    return redis_client.eval(
        PUBLISH_ONCE, 2, DLQ_STREAM, DLQ_EVENTS,
        f"dead:{job.job_id}:{job.attempts}", job.job_id, job.filename,
        str(job.attempts), job.last_error or "Maximum attempts exhausted",
        job.updated_at.isoformat(),
    )


def mark_dead(db, job, error):
    """Commit first; reconciliation repairs subsequent publication failures."""
    job.status = "dead"
    job.next_retry_at = None
    job.last_error = str(error)
    db.commit()
    send_to_dlq(job)


def reconcile_dead_jobs(session_factory):
    """Revisit all dead jobs, including those acknowledged before Redis loss."""
    cursor = None
    while True:
        with session_factory() as db:
            query = select(Job).where(Job.status == "dead").order_by(Job.job_id)
            if cursor is not None:
                query = query.where(Job.job_id > cursor)
            jobs = db.scalars(query.limit(100)).all()
            for job in jobs:
                send_to_dlq(job)
            if len(jobs) < 100:
                return
            cursor = jobs[-1].job_id
