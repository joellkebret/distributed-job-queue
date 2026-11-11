# Distributed Job Queue

A Python job queue for asynchronous CSV processing. S3 uploads trigger notifications to SQS; a local listener creates jobs in PostgreSQL and publishes them to Redis Streams. A local autoscaler adjusts the worker count as unfinished jobs accumulate. Workers remove duplicate CSV rows and upload cleaned outputs to S3. FastAPI provides job status and health endpoints.

## Tech stack

- Python 3.13+, FastAPI, Pydantic, and Uvicorn
- Redis 7 Streams and consumer groups
- PostgreSQL 18, SQLAlchemy, and Psycopg
- Amazon S3 and SQS, accessed through Boto3
- Docker Compose for local services, uv for Python dependencies
- pytest for unit and integration tests

## Architecture

```mermaid
flowchart LR
    Client -->|Upload CSV| S3[(S3 bucket)]
    S3 -->|Input upload notifications| SQS[SQS upload queue]
    SQS --> Listener[Local listener]
    Listener -->|Create job| DB[(PostgreSQL)]
    Listener -->|Publish job| Queue[Redis jobs stream]
    Client -->|Check job| API[FastAPI]
    API -->|Read job state| DB
    Queue -->|csv-workers consumer group| Workers[Python workers]
    Workers -->|State, locks, retry deadlines| DB
    DB -->|Unfinished job count| Autoscaler[Local worker autoscaler]
    Autoscaler -->|Start and gracefully retire processes| Workers
    Workers -->|Publish due retries| Queue
    S3 -->|Download input| Workers
    Workers -->|Upload cleaned CSV| S3
    Workers -->|Exhausted attempts| DLQ[Redis jobs:dead stream]
```

The API, listener, autoscaler, and workers run on the host. S3 and SQS run in AWS. Docker Compose runs PostgreSQL and Redis with persistent volumes; Redis has append-only persistence enabled. Each worker processes one job at a time and uses a unique consumer name.

## Implemented features

- Automatic job creation from S3 upload notifications, with status lookup through HTTP.
- CSV row deduplication that preserves the header and first occurrence order.
- Counts for total, unique, and duplicate rows, plus missing values in retained rows. These results are logged by the worker.
- Persistent job state and exponential retry scheduling.
- Concurrent workers with per-job PostgreSQL locking.
- Local worker autoscaling based on unfinished job counts.
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
AWS_PROFILE=job-queue
AWS_DEFAULT_REGION=us-east-2
S3_BUCKET=YOUR-BUCKET
SQS_QUEUE_URL=https://sqs.us-east-2.amazonaws.com/YOUR-ACCOUNT-ID/YOUR-QUEUE
```

These credentials are for local development. Existing PostgreSQL volumes retain their original database and credentials.

Use your own bucket, queue URL, and region. Create a Standard SQS queue in the bucket's region. Allow the bucket to send messages to the queue and configure S3 `ObjectCreated` notifications with prefix `input/` and suffix `.csv`. The local AWS identity needs S3 read/write access and SQS `ReceiveMessage` and `DeleteMessage` permissions. Authenticate it with `aws login --profile job-queue` before starting the listener and workers.

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

Start the worker autoscaler in another terminal:

```sh
uv run python -m worker.autoscale
```

The autoscaler manages 1 to 4 local workers by default. See the configuration and scaling rules below. Use `uv run python -m worker.main` instead if you want one manually managed worker.

Start the upload listener in another terminal:

```sh
uv run python -m listener.main
```

All workers need read/write access to the configured S3 bucket and a writable local output directory. Stop local services with `docker compose stop` after stopping the listener and workers.

## Worker autoscaling

The autoscaler checks PostgreSQL for unfinished jobs and chooses a worker count using:

```text
target = ceil(unfinished_jobs / JOBS_PER_WORKER)
target = bounded between MIN_WORKERS and MAX_WORKERS
```

Queued, retry-queued, processing, and failed jobs whose retry time has arrived count toward demand. Including processing jobs prevents the pool from shrinking merely because workers claimed their messages. Completed/dead jobs, future retries, and retained Redis stream history do not count. At least one worker stays running to schedule retries and recover pending messages.

Set these optional variables in `.env` to override the defaults:

| Variable | Default | Purpose |
| --- | --- | --- |
| `MIN_WORKERS` | `1` | Minimum running workers; must be at least 1. |
| `MAX_WORKERS` | `4` | Maximum managed worker processes. |
| `JOBS_PER_WORKER` | `5` | Target unfinished jobs per worker. |
| `WORKER_SCALE_INTERVAL` | `5` | Seconds between demand checks. |
| `WORKER_SCALE_DOWN_DELAY` | `30` | Seconds of lower demand before retiring extra workers. |

With the defaults, 1 to 5 unfinished jobs use one worker, 6 to 10 use two, 11 to 15 use three, and 16 or more use four. Scaling up happens on the next check. Extra workers finish their current job before retiring, and retiring processes still count toward the maximum until they exit. Crashed workers are replaced on a subsequent check. If PostgreSQL cannot be queried, the pool is left running and the query is retried.

Ctrl+C asks managed workers to finish and exit. Run only one autoscaler per local queue; its worker limit applies to its own children, not separately launched workers or other machines. This scales local processes, not AWS servers, and does not split one CSV across workers. Capacity is based on job count, not CSV file size or row count.

## API

Interactive API documentation is available at `http://localhost:8000/docs`.

| Endpoint | Behavior |
| --- | --- |
| `GET /health` | Pings Redis and returns its connection status. |
| `GET /jobs/{job_id}` | Returns the stored job record, or HTTP 404 if it does not exist. |

Create jobs by uploading CSVs to the bucket's `input/` prefix. The listener logs the job ID for each upload; no HTTP submission is needed. Previously uploaded objects do not generate new notifications retroactively, so upload a sample again after enabling notifications.

Check the job and Redis health:

```sh
curl http://localhost:8000/jobs/<generated-uuid>
curl http://localhost:8000/health
```

The status response includes `job_id`, `filename`, `status`, `attempts`, `last_error`, `next_retry_at`, `created_at`, and `updated_at`.

The processor downloads `input/<filename>` from the S3 bucket configured by `S3_BUCKET` into a temporary directory. With the listener and workers running, upload a sample:

```sh
aws s3 cp tests/duplicate_rows.csv s3://YOUR-BUCKET/input/duplicate_rows.csv --profile job-queue
```

The upload automatically creates a job. Downloaded inputs are cleaned up after reading, including on failure. The worker writes the cleaned CSV locally to `output/cleaned_<job_id>.csv`, then uploads it to the same key in S3. A job only completes after the upload succeeds; upload errors use the existing retry flow. Processor results include `output_s3_key`, which the worker logs. There is no file upload or download endpoint.

Download the result using the job ID:

```sh
aws s3 cp s3://YOUR-BUCKET/output/cleaned_<job_id>.csv cleaned.csv --profile job-queue
```

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

Delivery can occur more than once; this is not an exactly-once system. The listener derives a stable job ID from the bucket, object key, and upload sequencer, so redelivery of the same notification reuses its PostgreSQL job. A new upload to the same key creates a new job. The listener saves the job before publishing to Redis and deletes the SQS notification only after all its records have been handled. Redelivery repairs interrupted Redis publication; duplicate Redis messages remain possible and go through the worker's existing locks and state checks.

### Dead-letter queue

Workers commit the `dead` state before publishing to `jobs:dead`. Events include the job ID, filename, attempts, error, and failure timestamp. A Redis Lua script atomically checks `jobs:dead:events` and publishes once per stable event ID, `dead:<job_id>:<attempts>`.

At startup and approximately every 30 seconds between jobs, workers revisit dead records in PostgreSQL. This repairs failures between committing the dead state and publishing, and restores missing stream entries. Keep the deduplication index with the DLQ stream. Trimming the stream is not permanent retention while its dead records remain in PostgreSQL. Downstream consumers should deduplicate by event ID after Redis data loss. There is no DLQ replay endpoint.

## Tests

With the Compose services running and `.env` configured:

```sh
uv run pytest -q
uv run pytest -q -m integration
uv run pytest -q tests/test_processor.py
uv run pytest -q tests/test_listener.py
uv run pytest -q tests/test_autoscale.py
```

Tests cover upload notification parsing, duplicate notifications, interrupted job publication, acknowledgment after success, status lookup, CSV output, retry delays and caps, exhaustion, DLQ repair, consumer-group delivery, XAUTOCLAIM recovery, and concurrent workers. Autoscaler tests cover demand counting, worker limits, delayed scale-down, crash replacement, and finishing received jobs before retirement. Listener unit tests use SQLite and mock SQS and Redis; S3 downloads and uploads are mocked in processor and worker integration tests.

Integration tests use real PostgreSQL transactions, advisory locks, Redis Streams, and Lua scripts. They create a unique PostgreSQL schema and Redis key prefix per test and clean them up afterward. The database account needs permission to create schemas. CSV tests use temporary directories.

Set `TEST_DATABASE_URL` or `TEST_REDIS_URL` to override integration service addresses. Unavailable services fail integration setup. The older standalone Redis DLQ test uses local Redis and skips if unavailable; `-m "not integration"` therefore does not guarantee an offline run.

Concurrency tests use threads with separate database connections and Redis consumer identities. Recovery tests recreate abandoned processing state and reclaim real pending messages; they do not forcibly kill worker processes.

Local validation completed with 49 passing tests. Separate live checks verified an S3 upload traveling through SQS, PostgreSQL, Redis, and a worker to a cleaned S3 output, plus three real Windows worker processes starting and gracefully shrinking to one. These live checks are separate from the automated suite; running pytest does not create S3 output objects.

## Known limitations

- PostgreSQL commits and Redis publication are not atomic. SQS redelivery repairs listener publication failures while the notification remains available. There is no transactional outbox or queued-job reconciliation after acknowledgment; Redis data loss can still strand queued work. Malformed notifications remain in SQS for retry until retention expires; no SQS DLQ is required or configured by the application.
- Inputs use S3 keys based on filenames; uploading to the same key changes the input, and retries read the current object. Outputs are uploaded to S3, but local output copies are also retained without automatic cleanup.
- CSV processing keeps unique rows in memory. There is no job timeout, cancellation, or file-size limit.
- There is no authentication, rate limiting, metrics endpoint, or automatic stream retention. The health endpoint checks Redis only.
- Database initialization creates missing tables but does not migrate existing schemas. Redis connection settings are currently hardcoded for local use.

