from datetime import datetime, timedelta, timezone
from threading import Event
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from db.models import Base, Job
from worker import autoscale


class FakeProcess:
    next_pid = 1

    def __init__(self, target, args):
        self.pid = FakeProcess.next_pid
        FakeProcess.next_pid += 1
        self.alive = False
        self.exitcode = None
        self.stop = args[0]

    def start(self):
        self.alive = True

    def is_alive(self):
        return self.alive

    def join(self):
        pass


def pool(**settings):
    context = SimpleNamespace(Process=FakeProcess, Event=Event)
    return autoscale.WorkerPool(autoscale.Settings(**settings), context)


def test_pool_grows_to_cap_and_waits_before_graceful_retirement():
    workers = pool(jobs_per_worker=2, max_workers=3, scale_down_delay=10)
    workers.resize(0, 0)
    assert len(workers.workers) == 1
    workers.resize(100, 1)
    assert len(workers.workers) == 3
    workers.resize(0, 2)
    workers.resize(0, 11)
    assert not any(stop.is_set() for _, stop in workers.workers)
    workers.resize(0, 12)
    assert sum(stop.is_set() for _, stop in workers.workers) == 2
    assert all(process.is_alive() for process, _ in workers.workers)


def test_retiring_workers_still_count_toward_limit_and_exits_are_reaped():
    workers = pool(max_workers=3, jobs_per_worker=1, scale_down_delay=0)
    workers.resize(3, 0)
    workers.resize(0, 1)
    workers.resize(3, 2)
    assert len(workers.workers) == 3
    retiring = [process for process, stop in workers.workers if stop.is_set()]
    for process in retiring:
        process.alive = False
        process.exitcode = 0
    workers.resize(3, 3)
    assert len(workers.workers) == 3
    assert all(not stop.is_set() for _, stop in workers.workers)
    assert not any(process in retiring for process, _ in workers.workers)


def test_busy_period_resets_scale_down_delay_and_crashes_are_replaced():
    workers = pool(jobs_per_worker=1, max_workers=2, scale_down_delay=10)
    workers.resize(2, 0)
    workers.resize(0, 1)
    workers.resize(2, 8)
    workers.resize(0, 10)
    workers.resize(0, 15)
    assert not any(stop.is_set() for _, stop in workers.workers)
    crashed = workers.workers[0][0]
    crashed.alive = False
    crashed.exitcode = 1
    workers.resize(2, 16)
    assert len(workers.workers) == 2
    assert all(process is not crashed for process, _ in workers.workers)


@pytest.mark.parametrize('settings', [
    {'min_workers': 0}, {'min_workers': 3, 'max_workers': 2},
    {'jobs_per_worker': 0}, {'interval': 0}, {'scale_down_delay': -1},
])
def test_invalid_scaling_settings_fail_fast(settings):
    with pytest.raises(ValueError):
        autoscale.Settings(**settings)


def test_work_count_includes_busy_and_due_jobs_but_excludes_future_retries(monkeypatch):
    engine = create_engine('sqlite://')
    sessions = sessionmaker(engine)
    Base.metadata.create_all(engine)
    monkeypatch.setattr(autoscale, 'SessionLocal', sessions)
    now = datetime.now(timezone.utc)
    try:
        with sessions.begin() as db:
            for index, (status, deadline) in enumerate([
                ('queued', None), ('retry_queued', None), ('processing', None),
                ('failed', now - timedelta(seconds=30)),
                ('failed', now + timedelta(hours=1)), ('completed', None), ('dead', None),
            ]):
                db.add(Job(job_id=str(index), filename='x.csv', status=status, next_retry_at=deadline))
        assert autoscale.count_work() == 4
    finally:
        engine.dispose()


def test_retiring_worker_finishes_received_job_before_leaving_loop(monkeypatch):
    from worker import main

    stop = Event()
    client = Mock()
    client.xautoclaim.return_value = ('0-0', [])
    client.xreadgroup.return_value = [('jobs', [('1-0', {'job_id': 'job'})])]
    monkeypatch.setattr(main, 'redis_client', client)
    monkeypatch.setattr(main, 'schedule_retries', Mock())
    monkeypatch.setattr(main, 'reconcile_dead_jobs', Mock())
    processed = []

    def finish(message_id, data):
        stop.set()  # Simulate retirement being requested during this job.
        processed.append(data['job_id'])

    monkeypatch.setattr(main, 'process_job', finish)
    main.main(stop_event=stop)
    assert processed == ['job']
    client.xreadgroup.assert_called_once()


def test_stop_during_recovery_does_not_fetch_a_new_message(monkeypatch):
    from worker import main

    stop = Event()
    client = Mock()
    client.xautoclaim.return_value = ('0-0', [('1-0', {'job_id': 'job'})])
    monkeypatch.setattr(main, 'redis_client', client)
    monkeypatch.setattr(main, 'schedule_retries', Mock())
    monkeypatch.setattr(main, 'reconcile_dead_jobs', Mock())
    monkeypatch.setattr(main, 'process_job', lambda *args: stop.set())
    main.main(stop_event=stop)
    client.xreadgroup.assert_not_called()
