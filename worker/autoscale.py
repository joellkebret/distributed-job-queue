"""Run a bounded local worker pool sized from unfinished PostgreSQL jobs."""

from dataclasses import dataclass
from datetime import datetime, timezone
import logging
import math
import multiprocessing
import os
import signal
import time

from sqlalchemy import and_, func, or_, select

from db.database import SessionLocal
from db.models import Job

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Settings:
    min_workers: int = 1
    max_workers: int = 4
    jobs_per_worker: int = 5
    interval: float = 5
    scale_down_delay: float = 30

    def __post_init__(self):
        if not 1 <= self.min_workers <= self.max_workers:
            raise ValueError("Require 1 <= MIN_WORKERS <= MAX_WORKERS")
        if self.jobs_per_worker < 1 or self.interval <= 0 or self.scale_down_delay < 0:
            raise ValueError("Jobs per worker and interval must be positive; scale-down delay cannot be negative")

    @classmethod
    def from_env(cls):
        return cls(
            min_workers=int(os.getenv("MIN_WORKERS", "1")),
            max_workers=int(os.getenv("MAX_WORKERS", "4")),
            jobs_per_worker=int(os.getenv("JOBS_PER_WORKER", "5")),
            interval=float(os.getenv("WORKER_SCALE_INTERVAL", "5")),
            scale_down_delay=float(os.getenv("WORKER_SCALE_DOWN_DELAY", "30")),
        )


def count_work():
    # Include in-progress jobs to avoid shrinking just because workers claimed
    # messages. Future retries do not require extra capacity yet.
    with SessionLocal() as db:
        return db.scalar(select(func.count()).select_from(Job).where(or_(
            Job.status.in_(("queued", "retry_queued", "processing")),
            and_(Job.status == "failed", Job.next_retry_at <= datetime.now(timezone.utc)),
        )))


def run_worker(stop_event):
    # Let the parent handle Ctrl+C and ask children to finish gracefully.
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    from worker.main import main

    main(stop_event=stop_event)


class WorkerPool:
    def __init__(self, settings, context=None):
        self.settings = settings
        self.context = context or multiprocessing.get_context("spawn")
        self.workers = []
        self.low_since = None

    def resize(self, jobs, now):
        # Reap exited children, including crashes, before calculating capacity.
        for process, stop in list(self.workers):
            if not process.is_alive():
                process.join()
                self.workers.remove((process, stop))
                logger.info("Worker %s exited (code %s)", process.pid, process.exitcode)

        target = max(self.settings.min_workers, min(
            self.settings.max_workers, math.ceil(jobs / self.settings.jobs_per_worker)
        ))
        active = [(process, stop) for process, stop in self.workers if not stop.is_set()]
        if target >= len(active):
            self.low_since = None
            # Retiring workers still count toward the hard process limit until
            # they exit; never kill a busy worker to make room for another.
            available = self.settings.max_workers - len(self.workers)
            for _ in range(min(target - len(active), available)):
                stop = self.context.Event()
                process = self.context.Process(target=run_worker, args=(stop,))
                process.start()
                self.workers.append((process, stop))
                logger.info("Started worker %s (%s unfinished jobs, target %s)", process.pid, jobs, target)
        else:
            if self.low_since is None:
                self.low_since = now
            if now - self.low_since >= self.settings.scale_down_delay:
                for process, stop in active[target:]:
                    stop.set()
                    logger.info("Retiring worker %s after its current job", process.pid)
                self.low_since = None

    def close(self):
        for process, stop in self.workers:
            stop.set()
        for process, _ in self.workers:
            process.join()


def main():
    logging.basicConfig(level=logging.INFO)
    settings = Settings.from_env()
    pool = WorkerPool(settings)
    logger.info("Autoscaling between %s and %s workers", settings.min_workers, settings.max_workers)
    try:
        while True:
            try:
                pool.resize(count_work(), time.monotonic())
            except Exception:
                # A database outage must not kill children or cause scale-down.
                logger.exception("Could not resize workers; retrying on next check")
            time.sleep(settings.interval)
    except KeyboardInterrupt:
        logger.info("Stopping workers after their current jobs finish")
    finally:
        pool.close()


if __name__ == "__main__":
    main()
