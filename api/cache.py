"""Result caching keyed by job inputs.

Cache key is a hash of the inputs that determine the output (image_url,
width, height). Same inputs -> same key -> reuse the prior result and skip
the worker.

Assumption: the content at image_url is stable for the cache TTL. A
production system would key on the image's content hash (or an ETag) to be
robust to the URL's content changing. Documented as a known tradeoff.
"""
import hashlib
import json
from typing import Optional, Any
from redis.asyncio import Redis

CACHE_KEY_PREFIX = "cache:"
CACHE_TTL_SECONDS = 60 * 60  # results cached for 1h


def cache_key(image_url: str, width: int, height: int) -> str:
    raw = f"{image_url}|{width}|{height}".encode()
    digest = hashlib.sha256(raw).hexdigest()
    return f"{CACHE_KEY_PREFIX}{digest}"


async def get_cached(redis: Redis, image_url: str, width: int, height: int) -> Optional[Any]:
    raw = await redis.get(cache_key(image_url, width, height))
    return json.loads(raw) if raw else None


async def set_cached(redis: Redis, image_url: str, width: int, height: int, result: Any) -> None:
    await redis.set(
        cache_key(image_url, width, height),
        json.dumps(result),
        ex=CACHE_TTL_SECONDS,
    )