import os
import unittest
from unittest.mock import patch
from uuid import uuid4

os.environ.setdefault("DATABASE_URL", "sqlite://")

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from redis import Redis
from redis.exceptions import RedisError
from db.models import Base, Job
from worker import dlq, main


class DeadJobsTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite://")
        Base.metadata.create_all(self.engine)
        self.sessions = sessionmaker(self.engine)
        self.session_patch = patch.object(main, "SessionLocal", self.sessions)
        self.session_patch.start()
        self.redis_patch = patch.object(main, "redis_client")
        self.redis = self.redis_patch.start()
        self.publish_patch = patch.object(dlq, "send_to_dlq")
        self.publish = self.publish_patch.start()
        self.addCleanup(self.session_patch.stop)
        self.addCleanup(self.redis_patch.stop)
        self.addCleanup(self.publish_patch.stop)
        self.addCleanup(self.engine.dispose)

    def add_job(self, attempts=0, status="queued", last_error=None, job_id="job"):
        with self.sessions.begin() as db:
            db.add(Job(job_id=job_id, filename="missing.csv", attempts=attempts,
                       status=status, last_error=last_error))

    def test_exhausted_before_processing(self):
        self.add_job(3, last_error="previous error")
        with patch.object(main, "process_csv") as process:
            main._process_job("1-0", {"job_id": "job"})
        process.assert_not_called()
        self.publish.assert_called_once()
        with self.sessions() as db:
            job = db.get(Job, "job")
            self.assertEqual((job.status, job.last_error), ("dead", "previous error"))
        self.redis.xack.assert_called_once()

    def test_final_failure(self):
        self.add_job(2)
        with patch.object(main, "process_csv", side_effect=ValueError("bad CSV")):
            main._process_job("1-0", {"job_id": "job"})
        self.publish.assert_called_once()
        with self.sessions() as db:
            job = db.get(Job, "job")
            self.assertEqual((job.status, job.attempts, job.last_error),
                             ("dead", 3, "bad CSV"))
            self.assertIsNone(job.next_retry_at)
        self.redis.xack.assert_called_once()

    def test_nonfinal_failure_persists_error(self):
        self.add_job()
        with patch.object(main, "process_csv", side_effect=ValueError("bad CSV")):
            main._process_job("1-0", {"job_id": "job"})
        self.publish.assert_not_called()
        with self.sessions() as db:
            job = db.get(Job, "job")
            self.assertEqual((job.status, job.last_error), ("failed", "bad CSV"))
            self.assertIsNotNone(job.next_retry_at)

    def test_publish_failure_is_durable_and_reconciled(self):
        self.add_job(3)
        self.publish.side_effect = RedisError("offline")
        with self.assertRaises(RedisError):
            main._process_job("1-0", {"job_id": "job"})
        self.redis.xack.assert_not_called()
        with self.sessions() as db:
            self.assertEqual(db.get(Job, "job").status, "dead")
        self.publish.side_effect = None
        self.publish.reset_mock()
        dlq.reconcile_dead_jobs(self.sessions)
        self.publish.assert_called_once()

    def test_reconciliation_scans_multiple_batches(self):
        for index in range(105):
            self.add_job(3, "dead", job_id=f"job-{index:03}")
        self.add_job()
        dlq.reconcile_dead_jobs(self.sessions)
        self.assertEqual(self.publish.call_count, 105)


class RedisDeduplicationTest(unittest.TestCase):
    def test_atomic_deduplication_and_stream_repair(self):
        client = Redis(decode_responses=True, socket_connect_timeout=1)
        try:
            client.ping()
        except RedisError:
            self.skipTest("Local Redis is unavailable")
        prefix = f"test:dlq:{uuid4().hex}"
        stream, index = prefix + ":stream", prefix + ":index"
        from datetime import datetime, timezone
        job = Job(job_id="job", filename="bad.csv", attempts=3,
                  last_error="bad CSV", updated_at=datetime.now(timezone.utc))
        try:
            with patch.object(dlq, "redis_client", client), \
                 patch.object(dlq, "DLQ_STREAM", stream), \
                 patch.object(dlq, "DLQ_EVENTS", index):
                from concurrent.futures import ThreadPoolExecutor
                with ThreadPoolExecutor(max_workers=8) as pool:
                    ids = list(pool.map(lambda _: dlq.send_to_dlq(job), range(24)))
                self.assertEqual(len(set(ids)), 1)
                self.assertEqual(client.xlen(stream), 1)
                original = client.xrange(stream)[0][1]
                client.delete(stream)
                dlq.send_to_dlq(job)
                self.assertEqual(client.xlen(stream), 1)
                self.assertEqual(client.xrange(stream)[0][1], original)
        finally:
            client.delete(stream, index)
            client.close()


if __name__ == "__main__":
    unittest.main()
