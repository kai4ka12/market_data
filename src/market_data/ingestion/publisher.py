"""Publishes normalized events to Redis Streams.

Each symbol/event-type pair gets its own stream: md:trades:{symbol} and
md:depth:{symbol}. This keeps Phase 2's storage consumer simple later — it
can consume each stream independently rather than filtering one giant
merged stream by event type.

I/O-bound (network calls to Redis), so this uses redis.asyncio — the async
client, not the plain sync `redis.Redis`. Calling a sync client from
inside async code would block the whole event loop on every publish,
freezing the WebSocket consumer along with it — the same danger flagged in
client.py and orderbook_sync.py.
"""

from redis.asyncio import Redis

from market_data.config import settings
from market_data.models import DepthUpdate, TradeEvent


class Publisher:
    def __init__(self) -> None:
        self._redis: Redis = Redis.from_url(settings.redis_url)

    async def publish(self, event: TradeEvent | DepthUpdate) -> None:
        stream_key = self._stream_key(event)
        await self._redis.xadd(stream_key, {"data": event.model_dump_json()})

    @staticmethod
    def _stream_key(event: TradeEvent | DepthUpdate) -> str:
        kind = "trades" if isinstance(event, TradeEvent) else "depth"
        return f"md:{kind}:{event.symbol}"

    async def close(self) -> None:
        await self._redis.aclose()
