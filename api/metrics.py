"""Prometheus metrics shared across the codebase.

Latency is measured as percentiles via a Histogram (averages hide tail
latency; p99 reflects worst-case user experience). Counters are monotonic
so Prometheus derives rates (jobs/sec) with rate(). Queue depth is a gauge
sampled at scrape time.
"""
from prometheus_client import Counter, Histogram, Gauge

jobs_submitted = Counter("jobs_submitted_total", "Jobs accepted by the API")
jobs_completed = Counter("jobs_completed_total", "Jobs completed successfully")
jobs_failed = Counter("jobs_failed_total", "Jobs that exhausted retries")
cache_hits = Counter("cache_hits_total", "Submissions served from cache")
rate_limited = Counter("rate_limited_total", "Requests rejected by the limiter")

job_latency = Histogram(
    "job_latency_seconds",
    "End-to-end latency from submission to completion",
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

queue_depth = Gauge("queue_depth", "Jobs currently waiting in the queue")