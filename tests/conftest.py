"""Shared fixtures: a fresh Redis, an in-process API client, and a live worker.

Integration tests: they exercise the real FastAPI app, real Redis, and (where
needed) a real arq worker subprocess — not mocks. The point is to verify the
concurrency guarantees end to end, the same way they were validated by hand.
"""
import os
import subprocess
import time
import pytest
import pytest_asyncio
import httpx
from redis.asyncio import Redis

os.environ.setdefault("REDIS_URL", "redis://localhost:6379")
os.environ.setdefault("RATE_LIMIT_CAPACITY", "100000")
os.environ.setdefault("RATE_LIMIT_REFILL", "50000")
os.environ.setdefault("JOB_MAX_TRIES", "3")
os.environ.setdefault("JOB_BACKOFF_BASE", "0.2")


@pytest_asyncio.fixture
async def redis_client():
    r = Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
    await r.flushall()
    yield r
    await r.flushall()
    await r.aclose()


@pytest_asyncio.fixture
async def client(redis_client):
    from api.main import app
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        async with app.router.lifespan_context(app):
            yield c


@pytest.fixture
def worker():
    """Start a real arq worker subprocess for tests that need processing."""
    proc = subprocess.Popen(
        ["arq", "worker.worker.WorkerSettings"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(2.5)
    yield proc
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()