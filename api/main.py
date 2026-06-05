from fastapi import FastAPI
from redis.asyncio import Redis
import os

app = FastAPI(title="Job Processing Service")

redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
redis: Redis | None = None


@app.on_event("startup")
async def startup() -> None:
    global redis
    redis = Redis.from_url(redis_url, decode_responses=True)


@app.on_event("shutdown")
async def shutdown() -> None:
    if redis is not None:
        await redis.aclose()


@app.get("/health")
async def health() -> dict:
    # Healthy only if we can actually reach Redis.
    pong = await redis.ping()
    return {"status": "ok", "redis": pong}