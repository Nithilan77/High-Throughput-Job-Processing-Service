"""Redis-backed job store. One JSON record per job, namespaced by key."""
import time
from typing import Optional
from redis.asyncio import Redis
from .models import JobRecord, JobStatus

JOB_KEY_PREFIX = "job:"
JOB_TTL_SECONDS = 60 * 60 * 24  # keep job records for 24h


def _key(job_id: str) -> str:
    return f"{JOB_KEY_PREFIX}{job_id}"


async def create_job(redis: Redis, job_id: str) -> JobRecord:
    now = time.time()
    record = JobRecord(
        job_id=job_id,
        status=JobStatus.QUEUED,
        created_at=now,
        updated_at=now,
    )
    await redis.set(_key(job_id), record.model_dump_json(), ex=JOB_TTL_SECONDS)
    return record


async def get_job(redis: Redis, job_id: str) -> Optional[JobRecord]:
    raw = await redis.get(_key(job_id))
    if raw is None:
        return None
    return JobRecord.model_validate_json(raw)


async def update_job(redis: Redis, job_id: str, **fields) -> Optional[JobRecord]:
    record = await get_job(redis, job_id)
    if record is None:
        return None
    data = record.model_dump()
    data.update(fields)
    data["updated_at"] = time.time()
    updated = JobRecord.model_validate(data)
    await redis.set(_key(job_id), updated.model_dump_json(), ex=JOB_TTL_SECONDS)
    return updated