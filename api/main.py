"""FastAPI service: submit / status / result with idempotency (Phase 4)."""
import os
import uuid
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Response, status, Header
from redis.asyncio import Redis
from arq import create_pool

from .models import SubmitJobRequest, SubmitJobResponse, JobRecord, JobStatus
from . import store
from . import idempotency
from worker.settings import redis_settings

from . import idempotency
from . import cache
from . import cache
from .ratelimit import RateLimiter

from .ratelimit import RateLimiter
from worker import retry as retrypolicy

from worker import retry as retrypolicy
from . import metrics
from fastapi.responses import PlainTextResponse
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
redis: Redis | None = None
arq_pool = None
limiter: RateLimiter | None = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis, arq_pool
    redis = Redis.from_url(redis_url, decode_responses=True)
    arq_pool = await create_pool(redis_settings())
    global limiter
    limiter = RateLimiter(redis)
    yield
    await redis.aclose()
    await arq_pool.aclose()


app = FastAPI(title="Job Processing Service", lifespan=lifespan)


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "redis": await redis.ping()}


@app.post("/jobs", response_model=SubmitJobResponse)
async def submit_job(
    req: SubmitJobRequest,
    response: Response,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    api_key: str = Header(default="anonymous", alias="X-API-Key"),
) -> SubmitJobResponse:
    allowed, tokens, retry_after = await limiter.allow(api_key)
    if not allowed:
        metrics.rate_limited.inc()
        raise HTTPException(
            status_code=429,
            detail="rate limit exceeded",
            headers={"Retry-After": str(max(1, round(retry_after)))},
        )
    
    job_id = uuid.uuid4().hex

    if idempotency_key:
        is_new, owning_id = await idempotency.claim(redis, idempotency_key, job_id)
        if not is_new:
            # Duplicate: return the original job. 200 signals "not freshly created".
            response.status_code = status.HTTP_200_OK
            existing = await store.get_job(redis, owning_id)
            existing_status = existing.status if existing else JobStatus.QUEUED
            response.headers["Location"] = f"/jobs/{owning_id}"
            return SubmitJobResponse(
                job_id=owning_id,
                status=existing_status,
                status_url=f"/jobs/{owning_id}",
            )
    # Cache check: if we've already computed this exact work, return it
    # immediately as a completed job and skip the worker entirely.
    cached = await cache.get_cached(redis, req.image_url, req.width, req.height)
    if cached is not None:
        metrics.cache_hits.inc()
        await store.create_job(redis, job_id)
        await store.update_job(
            redis, job_id, status=JobStatus.COMPLETED, result=cached
        )
        response.status_code = status.HTTP_201_CREATED
        response.headers["Location"] = f"/jobs/{job_id}"
        return SubmitJobResponse(
            job_id=job_id, status=JobStatus.COMPLETED, status_url=f"/jobs/{job_id}"
        )
    
    # Fresh job (either no key, or we won the claim).
    await store.create_job(redis, job_id)
    await arq_pool.enqueue_job(
        "process_job", job_id, req.image_url, req.width, req.height, _job_id=job_id
    )
    metrics.jobs_submitted.inc()
    response.status_code = status.HTTP_202_ACCEPTED
    response.headers["Location"] = f"/jobs/{job_id}"
    return SubmitJobResponse(
        job_id=job_id, status=JobStatus.QUEUED, status_url=f"/jobs/{job_id}"
    )

@app.get("/stats")
async def get_stats():
    """Aggregate processing stats across ALL workers (shared Redis counter)."""
    completed = await redis.get("stats:completed_total")
    # arq stores its queue as a sorted set (zset), so use ZCARD, not LLEN.
    depth = await redis.zcard("arq:queue")
    return {"completed_total": int(completed or 0), "queue_depth": depth}

@app.get("/metrics")
async def get_metrics():
    # Sample queue depth at scrape time from the arq queue list.
    try:
        depth = await redis.llen("arq:queue")
        metrics.queue_depth.set(depth)
    except Exception:
        pass
    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.get("/dlq")
async def get_dlq(limit: int = 100):
    """Inspect the dead-letter queue: jobs that exhausted their retries."""
    items = await retrypolicy.dlq_items(redis, limit)
    return {"size": await retrypolicy.dlq_size(redis), "items": items}

@app.get("/jobs/{job_id}", response_model=JobRecord)
async def get_status(job_id: str) -> JobRecord:
    record = await store.get_job(redis, job_id)
    if record is None:
        raise HTTPException(status_code=404, detail="job not found")
    return record


@app.get("/jobs/{job_id}/result")
async def get_result(job_id: str):
    record = await store.get_job(redis, job_id)
    if record is None:
        raise HTTPException(status_code=404, detail="job not found")
    if record.status == JobStatus.COMPLETED:
        return {"job_id": job_id, "status": record.status, "result": record.result}
    if record.status == JobStatus.FAILED:
        raise HTTPException(
            status_code=422, detail={"status": record.status, "error": record.error}
        )
    return Response(
        content=f'{{"job_id":"{job_id}","status":"{record.status.value}"}}',
        media_type="application/json",
        status_code=status.HTTP_202_ACCEPTED,
        headers={"Retry-After": "1"},
    )