import json
from types import SimpleNamespace
from unittest.mock import Mock
from urllib.parse import quote_plus

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from db.models import Base, Job
from listener import main as listener


def upload(key="input/sample.csv", sequencer="001", bucket="test-bucket"):
    return {
        "eventSource": "aws:s3",
        "eventName": "ObjectCreated:Put",
        "s3": {
            "bucket": {"name": bucket},
            "object": {"key": quote_plus(key), "sequencer": sequencer},
        },
    }


@pytest.fixture
def setup_listener(monkeypatch):
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    sessions = sessionmaker(engine)
    redis = Mock()
    sqs = Mock()
    monkeypatch.setenv("S3_BUCKET", "test-bucket")
    monkeypatch.setattr(listener, "SessionLocal", sessions)
    monkeypatch.setattr(listener, "redis_client", redis)

    def publish(stream, data):
        # The job must be committed before it is visible to a Redis worker.
        with sessions() as db:
            job = db.get(Job, data["job_id"])
            assert job is not None and job.status == "queued"
            assert job.filename == data["filename"]
        return "1-0"

    redis.xadd.side_effect = publish
    sqs.receive_message.return_value = {
        "Messages": [{"Body": json.dumps({"Records": [upload()]}),
                      "ReceiptHandle": "receipt"}]
    }
    yield SimpleNamespace(sessions=sessions, redis=redis, sqs=sqs)
    engine.dispose()


def test_poll_creates_committed_job_before_acknowledging(setup_listener):
    s = setup_listener
    listener.poll_once(s.sqs, "queue-url")
    with s.sessions() as db:
        job = db.scalars(select(Job)).one()
        assert job.filename == "sample.csv"
        assert job.attempts == 0
    s.redis.xadd.assert_called_once()
    s.sqs.delete_message.assert_called_once_with(QueueUrl="queue-url", ReceiptHandle="receipt")


def test_duplicate_upload_reuses_job_but_new_upload_gets_new_job(setup_listener):
    s = setup_listener
    first = listener.create_job(upload("input/batch/a + b.csv"))
    assert listener.create_job(upload("input/batch/a + b.csv")) == first
    second = listener.create_job(upload("input/batch/a + b.csv", "002"))
    assert second != first
    with s.sessions() as db:
        jobs = db.scalars(select(Job)).all()
        assert len(jobs) == 2
        assert all(job.filename == "batch/a + b.csv" for job in jobs)
        db.get(Job, first).status = "completed"
        db.commit()
    s.redis.xadd.reset_mock()
    assert listener.create_job(upload("input/batch/a + b.csv")) == first
    s.redis.xadd.assert_not_called()


def test_redis_failure_leaves_message_and_redelivery_repairs_publication(setup_listener):
    s = setup_listener
    publish = s.redis.xadd.side_effect
    s.redis.xadd.side_effect = ConnectionError("Redis unavailable")
    listener.poll_once(s.sqs, "queue-url")
    s.sqs.delete_message.assert_not_called()
    with s.sessions() as db:
        assert db.scalars(select(Job)).one().status == "queued"
    s.redis.xadd.side_effect = publish
    listener.poll_once(s.sqs, "queue-url")
    with s.sessions() as db:
        assert len(db.scalars(select(Job)).all()) == 1
    s.sqs.delete_message.assert_called_once()


def test_database_failure_never_publishes_or_acknowledges(setup_listener, monkeypatch):
    s = setup_listener
    monkeypatch.setattr(listener, "SessionLocal", Mock(side_effect=RuntimeError("DB unavailable")))
    listener.poll_once(s.sqs, "queue-url")
    s.redis.xadd.assert_not_called()
    s.sqs.delete_message.assert_not_called()


@pytest.mark.parametrize("body", ["not JSON", "{}", '{"Records": []}'])
def test_malformed_notification_is_not_acknowledged(setup_listener, body):
    s = setup_listener
    s.sqs.receive_message.return_value["Messages"][0]["Body"] = body
    listener.poll_once(s.sqs, "queue-url")
    s.redis.xadd.assert_not_called()
    s.sqs.delete_message.assert_not_called()


def test_test_event_and_unrelated_objects_are_ignored(setup_listener):
    s = setup_listener
    listener.process_message(json.dumps({"Event": "s3:TestEvent"}))
    listener.process_message(json.dumps({"Records": [
        upload("output/cleaned_job.csv"), upload("input/file.txt"),
        upload(bucket="another-bucket"),
    ]}))
    s.redis.xadd.assert_not_called()


def test_partial_batch_redelivery_does_not_duplicate_database_jobs(setup_listener):
    s = setup_listener
    body = json.dumps({"Records": [upload(), upload("input/second.csv")]})
    publish = s.redis.xadd.side_effect
    calls = 0

    def fail_second(stream, data):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise ConnectionError("Redis unavailable")
        return publish(stream, data)

    s.redis.xadd.side_effect = fail_second
    with pytest.raises(ConnectionError):
        listener.process_message(body)
    s.redis.xadd.side_effect = publish
    listener.process_message(body)
    with s.sessions() as db:
        assert len(db.scalars(select(Job)).all()) == 2
