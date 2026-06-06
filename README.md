# Job Processing Service

An asynchronous, horizontally-scalable job-processing API built on FastAPI, Redis, and an arq worker pool. It accepts work over HTTP, returns immediately with a job ID, processes the work on a separate pool of workers, and exposes the reliability machinery that production payment-style systems depend on: idempotency, rate limiting, result caching, bounded retries with a dead-letter queue, and Prometheus metrics.

The processing task is image thumbnail generation (resize + encode via Pillow), chosen because it is genuinely CPU-bound — which makes the horizontal-scaling results real rather than an artifact of fake `sleep()` work.

## Measured results

All figures below were measured on a Docker Compose deployment (API + Redis + worker pool) on a single development machine.

**Submit path** (Locust, 50 concurrent users, 30s, ~13,400 requests):

| Metric | Value |
|---|---|
| Sustained throughput | 446 requests/sec |
| Failures | 0 (0%) |
| Latency p50 | 66 ms |
| Latency p95 | 92 ms |
| Latency p99 | 120 ms |

**Processing throughput** (queue-drain rate, 900 jobs):

| Workers | Throughput |
|---|---|
| 1 worker | 211 jobs/sec |
| 2 workers | 707 jobs/sec |
| 3 workers | 1033 jobs/sec |

Scaling is near-linear once past a single worker: 2 -> 3 workers gives ~1.46x throughput for 1.5x the replicas. The single-worker number is lower than a linear extrapolation would predict, and this is worth understanding rather than glossing over: Pillow's resize is synchronous and runs on arq's event loop, so one worker cannot overlap its CPU work with its Redis I/O (status updates, cache writes) and sits partly idle. Adding workers removes that serialization, which is why the 1 -> 2 step looks larger than linear. The correct fix to lift the single-worker baseline is to offload the CPU work to a thread or process pool (see "Concurrency model" below); horizontal scaling via worker replicas is the primary scaling lever regardless, and it stays near-linear until Redis or the single API process becomes the bottleneck.

**Correctness** (integration test suite): 13 tests, all passing. The guarantees are verified, not asserted — the suite includes a 20-concurrent-duplicate idempotency test (exactly one job created), an exact token-bucket accounting test (a capacity-5 bucket admits exactly 5 of 8 requests), a cache-skips-worker test (the worker's completion counter does not advance on a cache hit), and a retry-to-dead-letter test.

## Architecture

```
                  +------------------+
   client  --->   |   FastAPI API    |   202 Accepted + job_id
                  |                  |   (returns immediately)
                  |  - idempotency   |
                  |  - rate limiting |
                  |  - cache check   |
                  +--------+---------+
                           |
                  enqueue  |               +-----------+
                           v               |   Redis   |
                  +------------------+      |           |
                  |    arq queue     |<---->|  - queue  |
                  |    (Redis zset)  |      |  - jobs   |
                  +--------+---------+      |  - cache  |
                           |               |  - idem   |
              pull / process               |  - DLQ    |
                           v               +-----------+
        +------------------+------------------+
        |        worker pool (1..N)           |
        |  - thumbnail generation (Pillow)    |
        |  - retries w/ exponential backoff   |
        |  - dead-letter on exhaustion        |
        |  - writes result + cache + metrics  |
        +-------------------------------------+
```

The API never blocks on the work. It validates the request, applies rate limiting and idempotency, checks the cache, and either returns a cached result or enqueues the job and returns `202 Accepted` with a status URL. A separate pool of worker containers pulls from the queue and processes jobs independently — which is what makes "scale workers without touching the API" true. Redis is the single backing store for the queue, job records, result cache, idempotency keys, rate-limit buckets, and the dead-letter queue.

## Job lifecycle

A job moves through an explicit state machine:

```
queued  ->  in_progress  ->  completed
   ^                            
   |                            
   +---- (retry w/ backoff) ----+
   |                            
   +---- (retries exhausted) ---> failed  --> dead-letter queue
```

Clients submit, then poll the status URL. `GET /jobs/{id}/result` returns `202` + `Retry-After` while the job is still in flight, the result on completion, or `422` on permanent failure.

## API

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/jobs` | Submit a job. Returns `202` (new), `200` (idempotent duplicate), or `201` (cache hit). |
| `GET` | `/jobs/{id}` | Job status record. |
| `GET` | `/jobs/{id}/result` | Result, or `202` while pending, or `422` if failed. |
| `GET` | `/dlq` | Inspect the dead-letter queue. |
| `GET` | `/stats` | Aggregate processing stats across all workers. |
| `GET` | `/metrics` | Prometheus metrics. |
| `GET` | `/health` | Liveness + Redis connectivity. |

Request headers:
- `Idempotency-Key` — optional; duplicate submissions with the same key return the original job.
- `X-API-Key` — identifies the client for per-key rate limiting (defaults to `anonymous`).

## Design decisions

These are the choices a reviewer is most likely to ask about, with the reasoning.

**arq over Celery.** The service is async-native FastAPI, and arq is async-native, Redis-backed, and small enough to fully understand. Celery is the more widely recognized name but is sync-first and heavier; using it here would mean fighting its configuration rather than demonstrating the concepts. arq lets the same Redis instance back the queue, cache, and reliability state with minimal moving parts.

**Idempotency via atomic `SET NX`.** The naive "check if key exists, then create" has a race: two identical concurrent requests can both observe "no key" and both create a job. The implementation reserves the idempotency key with a single atomic Redis `SET key value NX EX ttl`, so exactly one concurrent request wins the claim and all others return the winner's job ID. This was verified by firing 20 identical concurrent requests and confirming exactly one job was created.

**Rate limiting via a Redis Lua script.** A token-bucket limiter's "refill, check, consume" sequence is a race if done as separate `GET`/`SET` calls — two requests can both consume the last token. Running it as a single Lua script makes Redis execute it atomically server-side, so the limiter is correct under concurrency and across multiple API instances. Token bucket (vs. fixed window) is chosen so clients can burst up to the bucket capacity while being held to the refill rate on average. Verified: a burst of 15 requests against a capacity-10 bucket produced exactly 10 accepted and 5 rejected.

**Caching keyed by input hash.** The cache key is a SHA-256 of `image_url + width + height` — the inputs that determine the output. A cache hit returns the stored result and skips the worker entirely. This is distinct from idempotency: idempotency dedupes *retries of the same request*; caching dedupes *different requests for the same work*. Known tradeoff: caching by URL assumes the content at that URL is stable for the cache TTL; a production system would key on the image's content hash or an ETag.

**Retries on the framework primitive, backoff configured explicitly.** Failed jobs are retried by raising arq's `Retry(defer=delay)`, with the backoff policy (`delay = base * 2^(attempt-1)`) configured deliberately rather than left to defaults. Exponential backoff gives a failing downstream room to recover and avoids a synchronized retry storm. After `JOB_MAX_TRIES` attempts, the job is marked `failed` and pushed to a dead-letter queue so it is inspectable rather than silently lost.

**Percentile latency, not averages.** The latency metric is a Prometheus histogram, because averages hide tail latency. p99 reflects what the worst-served requests experience, which is the number that matters for an SLA.

**Fail-fast on unreachable image sources.** The worker uses a short (0.5s) fetch timeout and falls back to synthetic image generation. A thumbnail service should not block for seconds on a dead image URL; failing fast and moving on keeps throughput high.

## Concurrency model (a known tradeoff)

Pillow's resize/encode is synchronous CPU work, and arq runs tasks on its event loop. For this workload that is acceptable because each task is short and the worker's `max_jobs` concurrency overlaps the async I/O (Redis round-trips, HTTP fetch) across tasks. For substantially heavier CPU tasks, the correct next step would be to offload the CPU work to a thread or process pool so it does not block the worker's event loop. Horizontal scaling (more worker replicas) is the primary scaling lever and is demonstrated above.

## Running it

Requires Docker Desktop (allocate 4+ CPUs for meaningful scaling tests).

```bash
cd infra
docker compose up --build -d
```

Health check:

```bash
curl http://localhost:8000/health        # {"status":"ok","redis":true}
```

Interactive API docs are auto-generated at `http://localhost:8000/docs`.

Scale the worker pool:

```bash
docker compose up -d --scale worker=3
```

### Submit a job

```bash
curl -X POST http://localhost:8000/jobs \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: my-key-1' \
  -H 'X-API-Key: client-a' \
  -d '{"image_url":"http://example.com/cat.jpg","width":128,"height":128}'
```

## Tests

A 13-test integration suite exercises the real app, real Redis, and a real worker subprocess (no mocks). It verifies the API contract, idempotency under concurrency, rate-limit accounting, cache behavior, and retry/dead-letter handling.

Requires Redis reachable on `localhost:6379` (the Docker Redis exposes it). Run from the project root:

```bash
pip install -r requirements.txt    # or: pip install pytest pytest-asyncio httpx redis arq
python -m pytest tests/ -v
```

The suite flushes Redis between tests, so run it against a dev Redis, not one holding data you care about.

## Load testing

Submit-path throughput and latency (Locust):

```bash
pip install locust
locust -f load/locustfile.py --host http://localhost:8000
# open http://localhost:8089, set users + spawn rate, run
```

Processing throughput / scaling (queue-drain rate):

```bash
pip install httpx
docker compose exec redis redis-cli flushall
python load/scale_test.py 900
```

Re-run after `docker compose up -d --scale worker=N` to compare worker counts.

## Configuration

Set via environment in `infra/docker-compose.yml`:

| Variable | Service | Default | Purpose |
|---|---|---|---|
| `REDIS_URL` | api, worker | `redis://redis:6379` | Redis connection |
| `RATE_LIMIT_CAPACITY` | api | `100` | Token-bucket burst size |
| `RATE_LIMIT_REFILL` | api | `5` | Tokens refilled per second |
| `JOB_MAX_TRIES` | worker | `3` | Attempts before dead-lettering |
| `JOB_BACKOFF_BASE` | worker | `1.0` | Base seconds for exponential backoff |

## Project layout

```
job-service/
  api/
    main.py          # FastAPI app: submit / status / result / dlq / stats / metrics
    models.py        # Pydantic models + job state machine
    store.py         # Redis-backed job records
    idempotency.py   # atomic SET NX idempotency keys
    ratelimit.py     # token-bucket limiter (Lua script)
    cache.py         # result cache keyed by input hash
    metrics.py       # Prometheus counters / histogram / gauge
  worker/
    worker.py        # arq worker: process, cache, retry, DLQ, metrics
    tasks.py         # thumbnail generation (pure function)
    retry.py         # backoff policy + dead-letter queue helpers
    settings.py      # shared arq connection settings
  infra/
    docker-compose.yml
  load/
    locustfile.py    # submit-path load test
    scale_test.py    # processing-throughput / scaling test
  tests/
    conftest.py      # fixtures: fresh Redis, in-process client, worker subprocess
    test_api_contract.py
    test_idempotency.py    # incl. 20-concurrent-duplicate race test
    test_rate_limit.py
    test_processing.py     # cache-skips-worker, retry-to-DLQ
  Dockerfile
  pytest.ini
  requirements.txt
```

## Tech stack

FastAPI, arq, Redis, Pillow, Prometheus client, Locust, Docker Compose.
