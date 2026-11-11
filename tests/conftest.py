import os
from pathlib import Path
from uuid import uuid4
from types import SimpleNamespace

import pytest
from dotenv import load_dotenv

load_dotenv()
os.environ.setdefault('DATABASE_URL', 'postgresql+psycopg://localhost:5433/postgres')


@pytest.fixture
def s3_inputs(monkeypatch):
    from botocore.exceptions import ClientError
    from worker import processor

    objects = {"input/good.csv": b"a,b\n1,2\n1,2\n"}
    downloads = []
    uploads = []

    def download_file(bucket, key, destination):
        assert bucket == "test-input-bucket"
        downloads.append(Path(destination))
        if key not in objects:
            raise ClientError(
                {"Error": {"Code": "404", "Message": "CSV file not found"}},
                "HeadObject",
            )
        Path(destination).write_bytes(objects[key])

    def upload_file(source, bucket, key):
        assert bucket == "test-input-bucket"
        objects[key] = Path(source).read_bytes()
        uploads.append(key)

    monkeypatch.setenv("S3_BUCKET", "test-input-bucket")
    monkeypatch.setattr(
        processor, "s3_client",
        SimpleNamespace(download_file=download_file, upload_file=upload_file),
    )
    return SimpleNamespace(objects=objects, downloads=downloads, uploads=uploads)


@pytest.fixture
def services(monkeypatch, tmp_path, s3_inputs):
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import sessionmaker
    from redis import Redis
    from db.models import Base, Job
    from worker import main, dlq
    from api import main as api

    # A private schema and key prefix isolate tests from application data.
    schema = 'test_' + uuid4().hex
    url = os.environ.get('TEST_DATABASE_URL', os.environ['DATABASE_URL'])
    admin = create_engine(url, connect_args={'connect_timeout': 3})
    with admin.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA {schema}'))
    engine = create_engine(url, connect_args={'options': f'-csearch_path={schema}'})
    prefix = schema + ':'

    class NamespacedRedis(Redis):
        def execute_command(self, *args, **kwargs):
            args = list(args)
            # All production stream names and the DLQ index are namespaced.
            for i, arg in enumerate(args):
                if isinstance(arg, str) and arg in ('jobs', 'jobs:dead', 'jobs:dead:events'):
                    args[i] = prefix + arg
            return super().execute_command(*args, **kwargs)

    client = NamespacedRedis.from_url(os.environ.get('TEST_REDIS_URL', 'redis://localhost:6379/0'), decode_responses=True, socket_timeout=3)
    sessions = sessionmaker(engine)
    try:
        client.ping()
        Base.metadata.create_all(engine)
        client.xgroup_create('jobs', 'csv-workers', id='0', mkstream=True)
        monkeypatch.setattr(main, 'engine', engine)
        monkeypatch.setattr(main, 'SessionLocal', sessions)
        for module in (main, dlq, api):
            monkeypatch.setattr(module, 'redis_client', client)
        def get_db():
            with sessions() as db:
                yield db
        api.app.dependency_overrides[api.get_db] = get_db
        monkeypatch.chdir(tmp_path)
        (tmp_path / 'tests').mkdir()
        (tmp_path / 'tests' / 'good.csv').write_text('a,b\n1,2\n1,2\n', encoding='utf-8')
        def add(**kwargs):
            job_id = str(uuid4())
            with sessions.begin() as db:
                db.add(Job(job_id=job_id, filename=kwargs.pop('filename', 'good.csv'), **kwargs))
            return job_id
        def read(consumer='one'):
            return client.xreadgroup('csv-workers', consumer, {'jobs': '>'}, count=1)[0][1][0]
        def state(job_id):
            with sessions() as db:
                job = db.get(Job, job_id)
                db.expunge(job)
                return job
        yield SimpleNamespace(redis=client, sessions=sessions, add=add, read=read, state=state, main=main, dlq=dlq, api=api)
    finally:
        api.app.dependency_overrides.clear()
        keys = list(client.scan_iter(match=prefix + '*'))
        if keys:
            client.delete(*keys)
        client.close()
        engine.dispose()
        with admin.begin() as conn:
            conn.execute(text(f'DROP SCHEMA {schema} CASCADE'))
        admin.dispose()
