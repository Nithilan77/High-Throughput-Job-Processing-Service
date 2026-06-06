"""Token-bucket rate limiter, per API key, atomic via a Redis Lua script.

Token bucket (vs fixed window) is chosen so clients may burst up to
`capacity` requests but are held to `refill_rate` requests/sec on average.

Atomicity: the check-refill-consume sequence is a race if done as separate
GET/SET calls (two requests can both observe the last token). Running it as
a single Lua script makes Redis execute it atomically server-side, so the
limiter is correct under concurrency and across multiple API instances.
"""
import os
import time
from redis.asyncio import Redis

CAPACITY = int(os.getenv("RATE_LIMIT_CAPACITY", "10"))      # burst size
REFILL_RATE = float(os.getenv("RATE_LIMIT_REFILL", "5"))    # tokens per second

# KEYS[1] = bucket key
# ARGV[1] = capacity, ARGV[2] = refill_rate, ARGV[3] = now (sec), ARGV[4] = ttl
# Returns: {allowed (1/0), tokens_remaining, retry_after_seconds}
_LUA = """
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])

local data = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])

if tokens == nil then
  tokens = capacity
  ts = now
end

local elapsed = now - ts
tokens = math.min(capacity, tokens + elapsed * refill)
ts = now

local allowed = 0
local retry_after = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
else
  retry_after = (1 - tokens) / refill
end

redis.call('HMSET', key, 'tokens', tokens, 'ts', ts)
redis.call('EXPIRE', key, ttl)
return {allowed, tostring(tokens), tostring(retry_after)}
"""


class RateLimiter:
    def __init__(self, redis: Redis, capacity: int = CAPACITY, refill_rate: float = REFILL_RATE):
        self.redis = redis
        self.capacity = capacity
        self.refill_rate = refill_rate
        self._script = redis.register_script(_LUA)

    async def allow(self, api_key: str) -> tuple[bool, float, float]:
        """Returns (allowed, tokens_remaining, retry_after_seconds)."""
        now = time.time()
        ttl = int(self.capacity / self.refill_rate) + 60
        res = await self._script(
            keys=[f"ratelimit:{api_key}"],
            args=[self.capacity, self.refill_rate, now, ttl],
        )
        return bool(res[0]), float(res[1]), float(res[2])