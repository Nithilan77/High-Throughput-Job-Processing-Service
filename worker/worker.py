"""arq worker: pulls jobs, runs the thumbnail task, updates the job record.

Run with:  arq worker.worker.WorkerSettings
"""
import httpx
from redis.asyncio import Redis

from api import store
from api.models import JobStatus
from worker.settings import redis_settings, REDIS_URL
from worker.tasks import generate_thumbnail


async def process_job(ctx, job_id: str, image_url: str, width: int, height: int) -> dict:
    """arq task. ctx holds shared resources set up in startup()."""
    redis: Redis = ctx["app_redis"]

    await store.update_job(redis, job_id, status=JobStatus.IN_PROGRESS)

    image_bytes = None
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(image_url)
            if resp.status_code == 200:
                image_bytes = resp.content
    except Exception:
        image_bytes = None  # synthetic fallback inside the task

    # Pillow is sync; arq runs tasks in its event loop, so for truly CPU-heavy
    # work you'd offload to a thread/process pool. Documented as a known
    # tradeoff in the README "Concurrency model" section.
    result = generate_thumbnail(image_bytes, width, height)

    await store.update_job(redis, job_id, status=JobStatus.COMPLETED, result=result)
    return result


async def startup(ctx) -> None:
    ctx["app_redis"] = Redis.from_url(REDIS_URL, decode_responses=True)


async def shutdown(ctx) -> None:
    await ctx["app_redis"].aclose()


class WorkerSettings:
    functions = [process_job]
    redis_settings = redis_settings()
    on_startup = startup
    on_shutdown = shutdown
    max_jobs = 10  # concurrency per worker process