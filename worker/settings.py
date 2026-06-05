"""Shared arq connection settings used by both the API and the worker."""
import os
from arq.connections import RedisSettings

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")


def redis_settings() -> RedisSettings:
    return RedisSettings.from_dsn(REDIS_URL)


QUEUE_NAME = "arq:queue"