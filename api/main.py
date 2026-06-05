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

redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
redis: Redis | None = None
arq_pool = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis, arq_pool
    redis = Redis.from_url(redis_url, decode_responses=True)
    arq_pool = await create_pool(redis_settings())
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
) -> SubmitJobResponse:
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

    # Fresh job (either no key, or we won the claim).
    await store.create_job(redis, job_id)
    await arq_pool.enqueue_job(
        "process_job", job_id, req.image_url, req.width, req.height, _job_id=job_id
    )
    response.status_code = status.HTTP_202_ACCEPTED
    response.headers["Location"] = f"/jobs/{job_id}"
    return SubmitJobResponse(
        job_id=job_id, status=JobStatus.QUEUED, status_url=f"/jobs/{job_id}"
    )


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