"""arq worker: pulls jobs, runs the thumbnail task, with retries + DLQ."""
import httpx
from arq import Retry
from redis.asyncio import Redis

from api import store, cache
from api.models import JobStatus
from worker.settings import redis_settings, REDIS_URL
from worker.tasks import generate_thumbnail
from worker import retry as retrypolicy
from worker import retry as retrypolicy
from api import metrics
from prometheus_client import start_http_server
import time as _time

class TransientError(Exception):
    """Raised to signal a retryable failure."""


async def process_job(ctx, job_id: str, image_url: str, width: int, height: int) -> dict:
    redis: Redis = ctx["app_redis"]
    attempt = ctx["job_try"]  # arq sets this: 1 on first run, 2 on retry, ...
    max_tries = retrypolicy.MAX_TRIES

    await store.update_job(redis, job_id, status=JobStatus.IN_PROGRESS)

    try:
        # Test affordance: an image_url containing "fail" forces a failure so
        # retry/DLQ behavior is deterministically testable. Documented in README.
        if "fail" in image_url:
            raise TransientError(f"forced failure (attempt {attempt})")

        image_bytes = None
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(image_url)
                if resp.status_code == 200:
                    image_bytes = resp.content
        except Exception:
            image_bytes = None

        result = generate_thumbnail(image_bytes, width, height)
        await cache.set_cached(redis, image_url, width, height, result)
        record = await store.update_job(
            redis, job_id, status=JobStatus.COMPLETED, result=result
        )
        # End-to-end latency: submission (created_at) -> now.
        if record is not None:
            metrics.job_latency.observe(max(0.0, _time.time() - record.created_at))
        metrics.jobs_completed.inc()
        return result

    except Exception as exc:
        if attempt < max_tries:
            # Re-enqueue with exponential backoff. arq's Retry defers the job.
            delay = retrypolicy.backoff_delay(attempt)
            await store.update_job(
                redis, job_id, status=JobStatus.QUEUED,
                error=f"attempt {attempt} failed: {exc}; retrying in {delay}s",
            )
            raise Retry(defer=delay)
        # Exhausted retries -> permanent failure -> DLQ.
        payload = {"image_url": image_url, "width": width, "height": height}
        await retrypolicy.send_to_dlq(redis, job_id, payload, str(exc))
        await store.update_job(
            redis, job_id, status=JobStatus.FAILED,
            error=f"failed after {attempt} attempts: {exc}",
        )
        metrics.jobs_failed.inc()
        return {"status": "failed", "error": str(exc)}


async def startup(ctx) -> None:
    ctx["app_redis"] = Redis.from_url(REDIS_URL, decode_responses=True)
    start_http_server(9100)


async def shutdown(ctx) -> None:
    await ctx["app_redis"].aclose()


class WorkerSettings:
    functions = [process_job]
    redis_settings = redis_settings()
    on_startup = startup
    on_shutdown = shutdown
    max_jobs = 10
    max_tries = retrypolicy.MAX_TRIES  # arq won't retry beyond this