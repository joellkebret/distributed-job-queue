from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from threading import Event

import pytest
from fastapi.testclient import TestClient
from redis.exceptions import ConnectionError
from db.models import Job
from listener import main as listener

pytestmark = pytest.mark.integration


def test_upload_submission_status_and_processing(services, monkeypatch):
    s = services
    monkeypatch.setattr(listener, 'SessionLocal', s.sessions)
    monkeypatch.setattr(listener, 'redis_client', s.redis)
    with TestClient(s.api.app) as client:
        assert client.get('/health').status_code == 200
        assert client.post('/jobs', json={'filename': 'good.csv'}).status_code == 404
        assert client.get('/jobs/missing').status_code == 404
        record = {
            'eventSource': 'aws:s3', 'eventName': 'ObjectCreated:Put',
            's3': {'bucket': {'name': 'test-input-bucket'},
                   'object': {'key': 'input/good.csv', 'sequencer': '001'}},
        }
        job_id = listener.create_job(record)
        assert client.get('/jobs/' + job_id).json()['status'] == 'queued'
        message = s.read()
        assert message[1]['job_id'] == job_id
        s.main.process_job(*message)
        result = client.get('/jobs/' + job_id).json()
        assert (result['status'], result['attempts']) == ('completed', 1)
        s.main.process_job(*message)
        assert s.state(job_id).attempts == 1
        assert s.redis.xpending('jobs', 'csv-workers')['pending'] == 0
        length = s.redis.xlen('jobs')
        assert listener.create_job(record) == job_id
        assert s.redis.xlen('jobs') == length


def test_retries_backoff_exhaustion_and_dlq(services):
    s = services
    job_id = s.add(filename='missing.csv')
    s.redis.xadd('jobs', {'job_id': job_id})
    for attempt in range(1, 4):
        before = datetime.now(timezone.utc)
        s.main.process_job(*s.read())
        job = s.state(job_id)
        assert job.attempts == attempt
        assert 'not found' in job.last_error
        assert s.redis.xpending('jobs', 'csv-workers')['pending'] == 0
        if attempt < 3:
            assert job.status == 'failed'
            delay = 5 * 2 ** (attempt - 1)
            assert before + timedelta(seconds=delay) <= job.next_retry_at <= datetime.now(timezone.utc) + timedelta(seconds=delay)
            length = s.redis.xlen('jobs')
            s.main.schedule_retries()
            assert s.redis.xlen('jobs') == length
            with s.sessions.begin() as db:
                db.get(Job, job_id).next_retry_at = before - timedelta(seconds=1)
            with ThreadPoolExecutor(2) as pool:
                list(pool.map(lambda _: s.main.schedule_retries(), range(2)))
            assert s.redis.xlen('jobs') == length + 1
            assert s.state(job_id).status == 'retry_queued'
    assert job.status == 'dead'
    assert job.next_retry_at is None
    s.dlq.reconcile_dead_jobs(s.sessions)
    assert s.redis.xlen('jobs:dead') == 1
    event = s.redis.xrange('jobs:dead')[0][1]
    assert event['job_id'] == job_id and event['attempts'] == '3'
    s.redis.delete('jobs:dead')
    s.dlq.reconcile_dead_jobs(s.sessions)
    assert s.redis.xrange('jobs:dead')[0][1] == event


@pytest.mark.parametrize('status,attempts,token,expected', [
    ('completed', 1, None, 'completed'), ('failed', 1, None, 'failed'),
    ('retry_queued', 1, None, 'retry_queued'), ('retry_queued', 2, '1', 'retry_queued'),
    ('queued', 3, None, 'dead'), ('processing', 3, '2', 'dead'),
])
def test_stale_and_exhausted_deliveries(services, status, attempts, token, expected):
    s = services
    job_id = s.add(status=status, attempts=attempts)
    data = {'job_id': job_id}
    if token is not None:
        data['retry_attempt'] = token
    s.redis.xadd('jobs', data)
    s.main.process_job(*s.read())
    assert (s.state(job_id).status, s.state(job_id).attempts) == (expected, attempts)
    assert s.redis.xpending('jobs', 'csv-workers')['pending'] == 0


def test_xautoclaim_recovers_crashed_processing_worker(services):
    s = services
    job_id = s.add(status='processing', attempts=1)
    s.redis.xadd('jobs', {'job_id': job_id})
    original = s.read('crashed')
    assert s.redis.xreadgroup('csv-workers', 'replacement', {'jobs': '>'}) == []
    assert s.redis.xautoclaim('jobs', 'csv-workers', 'replacement', 60000)[1] == []
    claimed = s.redis.xautoclaim('jobs', 'csv-workers', 'replacement', 0)[1]
    assert claimed == [original]
    s.main.process_job(*claimed[0])
    assert (s.state(job_id).status, s.state(job_id).attempts) == ('completed', 2)
    assert s.redis.xpending('jobs', 'csv-workers')['pending'] == 0


def test_concurrent_duplicate_workers_are_serialized(services, monkeypatch):
    s = services
    job_id = s.add()
    for _ in range(2):
        s.redis.xadd('jobs', {'job_id': job_id})
    first, second = s.read('one'), s.read('two')
    entered, release = Event(), Event()
    real = s.main.process_csv
    def slow(*args):
        entered.set()
        assert release.wait(10)
        return real(*args)
    monkeypatch.setattr(s.main, 'process_csv', slow)
    with ThreadPoolExecutor(2) as pool:
        future = pool.submit(s.main.process_job, *first)
        try:
            assert entered.wait(5)
            pool.submit(s.main.process_job, *second).result(timeout=5)
            assert s.state(job_id).attempts == 1
        finally:
            release.set()
        future.result(timeout=5)
    assert s.redis.xpending('jobs', 'csv-workers')['pending'] == 1
    s.main.process_job(*second)
    assert s.state(job_id).attempts == 1
    assert s.redis.xpending('jobs', 'csv-workers')['pending'] == 0


def test_dlq_failure_persists_dead_state_and_reconciles(services, monkeypatch):
    s = services
    job_id = s.add(attempts=3)
    s.redis.xadd('jobs', {'job_id': job_id})
    message = s.read()
    real = s.redis.eval
    def offline(*args, **kwargs):
        raise ConnectionError('injected publication outage')
    monkeypatch.setattr(s.redis, 'eval', offline)
    with pytest.raises(ConnectionError):
        s.main.process_job(*message)
    assert s.state(job_id).status == 'dead'
    assert s.redis.xpending('jobs', 'csv-workers')['pending'] == 1
    monkeypatch.setattr(s.redis, 'eval', real)
    s.dlq.reconcile_dead_jobs(s.sessions)
    s.main.process_job(*message)
    assert s.redis.xlen('jobs:dead') == 1
    assert s.redis.xpending('jobs', 'csv-workers')['pending'] == 0

@pytest.mark.parametrize('attempts,delay', [(0, 5), (1, 10), (4, 60), (7, 60)])
def test_backoff_cap(services, monkeypatch, attempts, delay):
    s = services
    monkeypatch.setattr(s.main, 'MAX_ATTEMPTS', 10)
    job_id = s.add(filename='missing.csv', attempts=attempts)
    s.redis.xadd('jobs', {'job_id': job_id})
    before = datetime.now(timezone.utc)
    s.main.process_job(*s.read())
    job = s.state(job_id)
    assert job.status == 'failed'
    assert before + timedelta(seconds=delay) <= job.next_retry_at <= datetime.now(timezone.utc) + timedelta(seconds=delay)


def test_scheduler_publication_failure_rolls_back(services, monkeypatch):
    s = services
    due = datetime.now(timezone.utc) - timedelta(seconds=1)
    job_id = s.add(status='failed', attempts=1, next_retry_at=due)
    def offline(*args, **kwargs):
        raise ConnectionError('offline')
    monkeypatch.setattr(s.redis, 'xadd', offline)
    with pytest.raises(ConnectionError):
        s.main.schedule_retries()
    assert s.state(job_id).status == 'failed'
    assert s.state(job_id).next_retry_at == due


def test_independent_workers_consume_distinct_messages(services):
    s = services
    ids = [s.add() for _ in range(8)]
    for job_id in ids:
        s.redis.xadd('jobs', {'job_id': job_id})
    def work(index):
        message = s.read(str(index))
        s.main.process_job(*message)
        return message[0]
    with ThreadPoolExecutor(4) as pool:
        messages = list(pool.map(work, range(8)))
    assert len(set(messages)) == 8
    assert all((s.state(i).status, s.state(i).attempts) == ('completed', 1) for i in ids)
    assert s.redis.xpending('jobs', 'csv-workers')['pending'] == 0
