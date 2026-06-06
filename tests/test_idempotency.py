"""Idempotency: the fintech guarantee — same key processed exactly once."""
import asyncio
import pytest


@pytest.mark.asyncio
async def test_same_key_returns_same_job(client):
    headers = {"Idempotency-Key": "order-123"}
    body = {"image_url": "synthetic://x", "width": 64, "height": 64}
    first = await client.post("/jobs", json=body, headers=headers)
    dupe = await client.post("/jobs", json=body, headers=headers)
    assert first.json()["job_id"] == dupe.json()["job_id"]
    assert first.status_code == 202
    assert dupe.status_code == 200


@pytest.mark.asyncio
async def test_different_keys_create_different_jobs(client):
    body = {"image_url": "synthetic://x", "width": 64, "height": 64}
    a = await client.post("/jobs", json=body, headers={"Idempotency-Key": "A"})
    b = await client.post("/jobs", json=body, headers={"Idempotency-Key": "B"})
    assert a.json()["job_id"] != b.json()["job_id"]


@pytest.mark.asyncio
async def test_concurrent_duplicates_create_exactly_one_job(client, redis_client):
    """The race-condition test: 20 identical concurrent submissions must
    produce exactly one job. This is the atomic SET NX guarantee."""
    headers = {"Idempotency-Key": "concurrent-key"}
    body = {"image_url": "synthetic://x", "width": 64, "height": 64}

    async def submit():
        r = await client.post("/jobs", json=body, headers=headers)
        return r.json()["job_id"]

    ids = await asyncio.gather(*[submit() for _ in range(20)])
    assert len(set(ids)) == 1, f"expected 1 job, got {len(set(ids))}"

    job_keys = await redis_client.keys("job:*")
    assert len(job_keys) == 1