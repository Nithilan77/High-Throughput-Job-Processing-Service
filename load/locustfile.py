"""Locust load test for the job-processing service.

Run (headless):
  locust -f load/locustfile.py --host http://localhost:8000 \
         --users 50 --spawn-rate 10 --run-time 60s --headless

Web UI:
  locust -f load/locustfile.py --host http://localhost:8000
  # then open http://localhost:8089

Measures submit-endpoint throughput (RPS) and latency p50/p95/p99 under
concurrent load. Dimensions/urls are randomized so the result cache doesn't
absorb the load -- we want to exercise the queue + workers.
"""
import random
from locust import HttpUser, task, between


class JobSubmitter(HttpUser):
    wait_time = between(0.0, 0.05)

    @task
    def submit_job(self):
        w = random.choice([64, 96, 128, 160, 192])
        n = random.randint(0, 1_000_000)
        payload = {"image_url": f"http://load-test/img-{n}.jpg", "width": w, "height": w}
        with self.client.post(
            "/jobs", json=payload,
            headers={"X-API-Key": "loadtest"},
            catch_response=True,
        ) as resp:
            if resp.status_code in (200, 201, 202):
                resp.success()
            else:
                resp.failure(f"unexpected status {resp.status_code}")