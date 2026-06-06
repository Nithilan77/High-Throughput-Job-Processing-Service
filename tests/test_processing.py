"""End-to-end processing: completion, cache-skips-worker, retry-to-DLQ.

These use a live worker subprocess (the `worker` fixture)."""
import asyncio
import time
import pytest


async def _wait_for_status(client, job_id, target, timeout=15.0):
    start = time.time()
    while time.time() - start < timeout:
        r = await client.get(f"/jobs/{job_id}")
        if r.json()["status"] == target:
            return r.json()
        await asyncio.sleep(0.2)
    raise AssertionError(f"job {job_id} did not reach {target} in {timeout}s")


@pytest.mark.asyncio
async def test_job_completes(client, worker):
    sub = await client.post("/jobs", json={"image_url": "synthetic://a", "width": 64, "height": 64})
    job_id = sub.json()["job_id"]
    rec = await _wait_for_status(client, job_id, "completed")
    assert rec["result"]["thumbnail_size"] == [64, 64]


@pytest.mark.asyncio
async def test_cache_hit_skips_worker(client, worker, redis_client):
    """Second identical submission served from cache (201, completed
    immediately) without the worker processing it again."""
    body = {"image_url": "synthetic://cached", "width": 100, "height": 100}
    cold = await client.post("/jobs", json=body)
    await _wait_for_status(client, cold.json()["job_id"], "completed")
    completed_after_cold = int(await redis_client.get("stats:completed_total") or 0)

    warm = await client.post("/jobs", json=body)
    assert warm.status_code == 201
    assert warm.json()["status"] == "completed"

    await asyncio.sleep(1.0)
    completed_after_warm = int(await redis_client.get("stats:completed_total") or 0)
    assert completed_after_warm == completed_after_cold


@pytest.mark.asyncio
async def test_failing_job_goes_to_dlq(client, worker):
    """A job whose URL contains 'fail' exhausts retries and lands in the DLQ."""
    sub = await client.post("/jobs", json={"image_url": "http://will-fail/x", "width": 64, "height": 64})
    job_id = sub.json()["job_id"]
    rec = await _wait_for_status(client, job_id, "failed", timeout=20.0)
    assert "failed after" in (rec["error"] or "")

    dlq = await client.get("/dlq")
    body = dlq.json()
    assert body["size"] >= 1
    assert any(item["job_id"] == job_id for item in body["items"])