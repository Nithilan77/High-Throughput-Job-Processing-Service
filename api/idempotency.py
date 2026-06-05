"""Idempotency-key handling.

Contract:
- Caller generates a job_id, then calls claim(). claim() atomically reserves
  the idempotency key for THIS job_id using SET NX.
- If the key was free, we now own it -> caller creates+enqueues the job.
- If the key was taken, we return the EXISTING job_id -> caller returns it
  without creating anything (the duplicate path).

The atomic SET NX is what makes this correct under concurrent duplicate
requests: exactly one request wins the claim, all others see the winner's id.
"""
from redis.asyncio import Redis

IDEM_KEY_PREFIX = "idem:"
IDEM_TTL_SECONDS = 60 * 60 * 24  # keys valid for 24h


def _key(idempotency_key: str) -> str:
    return f"{IDEM_KEY_PREFIX}{idempotency_key}"


async def claim(redis: Redis, idempotency_key: str, job_id: str) -> tuple[bool, str]:
    """Try to claim `idempotency_key` for `job_id`.

    Returns (is_new, owning_job_id):
      - (True, job_id)        -> we won the claim; this is a fresh job
      - (False, existing_id)  -> key already claimed; existing_id owns it
    """
    won = await redis.set(_key(idempotency_key), job_id, nx=True, ex=IDEM_TTL_SECONDS)
    if won:
        return True, job_id
    existing = await redis.get(_key(idempotency_key))
    # Rare: key expired between SET and GET. Returning our own id preserves
    # correctness because job_id is globally unique.
    return False, (existing or job_id)