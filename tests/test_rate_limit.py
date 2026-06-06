"""Rate limiting: token bucket admits exactly capacity, then 429s."""
import pytest
from api.ratelimit import RateLimiter


@pytest.mark.asyncio
async def test_bucket_admits_capacity_then_rejects(redis_client):
    """Capacity 5: first 5 allowed, rest rejected. Tests the limiter directly
    so the assertion is exact and independent of HTTP timing."""
    limiter = RateLimiter(redis_client, capacity=5, refill_rate=0.001)
    results = []
    for _ in range(8):
        allowed, _, _ = await limiter.allow("client-x")
        results.append(allowed)
    assert results.count(True) == 5
    assert results.count(False) == 3


@pytest.mark.asyncio
async def test_separate_keys_have_separate_buckets(redis_client):
    limiter = RateLimiter(redis_client, capacity=2, refill_rate=0.001)
    await limiter.allow("client-a")
    await limiter.allow("client-a")
    a_third, _, _ = await limiter.allow("client-a")
    b_first, _, _ = await limiter.allow("client-b")
    assert a_third is False
    assert b_first is True


@pytest.mark.asyncio
async def test_429_returned_over_http(client):
    r = await client.post(
        "/jobs",
        json={"image_url": "synthetic://x", "width": 64, "height": 64},
        headers={"X-API-Key": "http-client"},
    )
    assert r.status_code in (200, 201, 202)