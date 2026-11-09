# Distributed Job Queue

A Python job queue for asynchronous CSV processing. FastAPI accepts jobs, Redis Streams distributes work across workers, and PostgreSQL stores job state, attempts, and retry deadlines. Workers remove duplicate CSV rows and write one output file per job.

## Tech stack

- Python 3.13+, FastAPI, Pydantic, and Uvicorn
- Redis 7 Streams and consumer groups
- PostgreSQL 18, SQLAlchemy, and Psycopg
- Docker Compose for local services, uv for Python dependencies
- pytest for unit and integration tests

## Architecture

```mermaid
flowchart LR
    Client -->|Submit or check a job| API[FastAPI]
    API -->|Store and read job state| DB[(PostgreSQL)]
    API -->|Publish job| Queue[Redis jobs stream]
    Queue -->|csv-workers consumer group| Workers[Python workers]
    Workers -->|State, locks, retry deadlines| DB
    Workers -->|Publish due retries| Queue
    CSV[Local CSV files] --> Workers
    Workers --> Output[Cleaned CSV files]
    Workers -->|Exhausted attempts| DLQ[Redis jobs:dead stream]
```

The API and workers run on the host. Docker Compose runs PostgreSQL and Redis with persistent volumes; Redis has append-only persistence enabled. Each worker processes one job at a time and uses a unique consumer name.

## Implemented features

- Job submission and status lookup through HTTP.
- CSV row deduplication that preserves the header and first occurrence order.
- Counts for total, unique, and duplicate rows, plus missing values in retained rows. These results are logged by the worker.
- Persistent job state and exponential retry scheduling.
- Concurrent workers with per-job PostgreSQL locking.
- Recovery of unacknowledged Redis messages and idempotent file output.
- Dead-letter publication with deduplication and periodic repair.

## Local setup

Install Python 3.13 or newer, uv, and Docker with Compose. Run commands from the repository root.

Create a `.env` file:

```dotenv
POSTGRES_USER=jobqueue
POSTGRES_PASSWORD=localdev
POSTGRES_DB=jobqueue
DATABASE_URL=postgresql+psycopg://jobqueue:localdev@localhost:5433/jobqueue
```

These credentials are for local development. Existing PostgreSQL volumes retain their original database and credentials.

Install dependencies and start the services:

```sh
uv sync --group dev
docker compose up -d postgres redis
docker compose ps
```

Once PostgreSQL is ready, create the tables:

```sh
uv run python -m db.init_db
```

PostgreSQL is exposed on port `5433`; the application connects to Redis at `localhost:6379`.

Start the API:

```sh
uv run uvicorn api.main:app --reload
```

Start a worker in another terminal:

```sh
uv run python -m worker.main
```

Run the same worker command in additional terminals to process jobs concurrently. All workers must be able to access the same input and output files. Stop local services with `docker compose stop`.

## API

Interactive API documentation is available at `http://localhost:8000/docs`.

| Endpoint | Behavior |
| --- | --- |
| `GET /health` | Pings Redis and returns its connection status. |
| `POST /jobs` | Stores a job, publishes it to Redis, and returns HTTP 202. |
| `GET /jobs/{job_id}` | Returns the stored job record, or HTTP 404 if it does not exist. |

Submit one of the included CSV files:

```sh
curl -X POST http://localhost:8000/jobs -H 'Content-Type: application/json' -d '{"filename":"duplicate_rows.csv"}'
```

Response:

```json
{"job_id":"<generated-uuid>","status":"queued"}
```

Check the job and Redis health:

```sh
curl http://localhost:8000/jobs/<generated-uuid>
curl http://localhost:8000/health
```

The status response includes `job_id`, `filename`, `status`, `attempts`, `last_error`, `next_retry_at`, `created_at`, and `updated_at`. On Windows PowerShell, use `Invoke-RestMethod` for submission:

```powershell
Invoke-RestMethod -Method Post -Uri http://localhost:8000/jobs -ContentType 'application/json' -Body '{"filename":"duplicate_rows.csv"}'
```

The processor searches recursively under `tests/` for the submitted filename. Place additional CSV inputs there with unique filenames. Output is written to `output/cleaned_<job_id>.csv`. There is no file upload or download endpoint.

## Processing and recovery

### States and retries

A successful job moves from `queued` to `processing` to `completed`. An unsuccessful attempt becomes `failed` with a persisted error and retry deadline. When due, a worker publishes a retry and changes the state to `retry_queued`, then processes it again.

Attempts increment before CSV processing. The limit is three total attempts, including the initial attempt. Retry delay is `min(5 * 2 ** (attempts - 1), 60)` seconds: with the current limit, retries wait at least 5 and 10 seconds. An exhausted job becomes `dead`. Crashed attempts also count toward the limit.

Each worker schedules due retries using PostgreSQL row locks with `SKIP LOCKED`, allowing multiple schedulers to run concurrently. Retry messages carry an attempt token so stale deliveries cannot start a later scheduled retry. Timing depends on worker availability; there is no separate scheduler process.

### Advisory locking and XAUTOCLAIM

Before processing, a worker acquires a transaction-scoped PostgreSQL advisory lock derived from the job ID. A separate database connection holds that lock across job-state commits and file processing. Another worker that cannot acquire it leaves its message pending. PostgreSQL releases the lock when the transaction ends or the connection is lost.

Workers use `XAUTOCLAIM` to reclaim messages idle for at least 60 seconds. This handles a worker stopping after delivery but before acknowledgment. A reclaimed message still goes through the lock and state checks. Retry deadlines remain in PostgreSQL; reclaiming a failed job does not bypass its backoff.

### Idempotency

Completed jobs are acknowledged without reprocessing. CSV output uses a stable job-specific filename, a temporary file, and atomic replacement. Reprocessing the same job with unchanged input produces the same output rather than additional files.

Delivery can occur more than once; this is not an exactly-once system. A new HTTP submission always creates a new job ID, even for the same filename.

### Dead-letter queue

Workers commit the `dead` state before publishing to `jobs:dead`. Events include the job ID, filename, attempts, error, and failure timestamp. A Redis Lua script atomically checks `jobs:dead:events` and publishes once per stable event ID, `dead:<job_id>:<attempts>`.

At startup and approximately every 30 seconds between jobs, workers revisit dead records in PostgreSQL. This repairs failures between committing the dead state and publishing, and restores missing stream entries. Keep the deduplication index with the DLQ stream. Trimming the stream is not permanent retention while its dead records remain in PostgreSQL. Downstream consumers should deduplicate by event ID after Redis data loss. There is no DLQ replay endpoint.

## Tests

With the Compose services running and `.env` configured:

```sh
uv run pytest -q
uv run pytest -q -m integration
uv run pytest -q tests/test_processor.py
```

Tests cover submission and status, CSV output, retry delays and caps, exhaustion, publication failures, DLQ repair, consumer-group delivery, XAUTOCLAIM recovery, and concurrent workers.

Integration tests use real PostgreSQL transactions, advisory locks, Redis Streams, and Lua scripts. They create a unique PostgreSQL schema and Redis key prefix per test and clean them up afterward. The database account needs permission to create schemas. CSV tests use temporary directories.

Set `TEST_DATABASE_URL` or `TEST_REDIS_URL` to override integration service addresses. Unavailable services fail integration setup. The older standalone Redis DLQ test uses local Redis and skips if unavailable; `-m "not integration"` therefore does not guarantee an offline run.

Concurrency tests use threads with separate database connections and Redis consumer identities. Recovery tests recreate abandoned processing state and reclaim real pending messages; they do not forcibly kill worker processes.

## Known limitations

- PostgreSQL commits and Redis publication are not atomic. Initial submission can leave a queued record without a stream message if publication fails; there is no outbox or queued-job reconciliation. Redis data loss can also strand queued work.
- Input and output use local filesystem paths. Workers on different machines need shared storage. Matching filenames in multiple input directories are ambiguous, and changing an input between attempts can change the output.
- CSV processing keeps unique rows in memory. There is no job timeout, cancellation, or file-size limit.
- There is no authentication, rate limiting, metrics endpoint, or automatic stream retention. The health endpoint checks Redis only.

## To Be Added
- File uploads through FastAPI for asynchronous CSV processing
- S3-compatible object storage for uploaded files and processed results
- Downloadable processed files through the API
- Job monitoring and performance metrics
- Worker scalability and throughput benchmarks
- Support for additional job types beyond CSV processing

