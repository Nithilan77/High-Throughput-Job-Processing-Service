"""The async API contract: 202 on submit, status transitions, 404s, validation."""
import pytest


@pytest.mark.asyncio
async def test_submit_returns_202_and_job_id(client):
    r = await client.post("/jobs", json={"image_url": "synthetic://x", "width": 64, "height": 64})
    assert r.status_code == 202
    body = r.json()
    assert "job_id" in body
    assert body["status"] == "queued"
    assert body["status_url"] == f"/jobs/{body['job_id']}"


@pytest.mark.asyncio
async def test_status_of_unknown_job_is_404(client):
    r = await client.get("/jobs/does-not-exist")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_invalid_dimensions_rejected(client):
    r = await client.post("/jobs", json={"image_url": "synthetic://x", "width": 999999})
    assert r.status_code == 422


@pytest.mark.asyncio
async def test_result_pending_returns_202_with_retry_after(client):
    sub = await client.post("/jobs", json={"image_url": "synthetic://x", "width": 64, "height": 64})
    job_id = sub.json()["job_id"]
    r = await client.get(f"/jobs/{job_id}/result")
    assert r.status_code == 202
    assert "retry-after" in {k.lower() for k in r.headers}