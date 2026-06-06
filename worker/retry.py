"""Retry policy + dead-letter queue helpers.

Backoff: exponential, delay = BASE * 2^(attempt-1) seconds (1, 2, 4, ...).
Exponential (not fixed) backoff gives a failing downstream room to recover
and prevents a synchronized retry storm.

DLQ: jobs that exhaust MAX_TRIES are pushed to a Redis list so they're not
silently lost and can be inspected/replayed by an operator.
"""
import os
import json
import time
from redis.asyncio import Redis

MAX_TRIES = int(os.getenv("JOB_MAX_TRIES", "3"))
BACKOFF_BASE = float(os.getenv("JOB_BACKOFF_BASE", "1.0"))
DLQ_KEY = "dlq"


def backoff_delay(attempt: int) -> float:
    """attempt is 1-based: 1 -> BASE, 2 -> 2*BASE, 3 -> 4*BASE."""
    return BACKOFF_BASE * (2 ** (attempt - 1))


async def send_to_dlq(redis: Redis, job_id: str, payload: dict, error: str) -> None:
    entry = {
        "job_id": job_id,
        "payload": payload,
        "error": error,
        "failed_at": time.time(),
    }
    await redis.rpush(DLQ_KEY, json.dumps(entry))


async def dlq_items(redis: Redis, limit: int = 100) -> list[dict]:
    raw = await redis.lrange(DLQ_KEY, 0, limit - 1)
    return [json.loads(r) for r in raw]


async def dlq_size(redis: Redis) -> int:
    return await redis.llen(DLQ_KEY)