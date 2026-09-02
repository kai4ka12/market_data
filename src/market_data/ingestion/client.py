"""Binance WebSocket ingestion client.

Owns the raw connection to Binance's combined stream: connecting,
subscribing, receiving messages, and reconnecting on failure.

Deliberately does NOT parse/normalize messages into TradeEvent/DepthUpdate
here — this module's only job is "get raw JSON messages off the wire
reliably." Normalization belongs in normalizer.py, order-book sync belongs
in orderbook_sync.py.
"""

import asyncio
import json
from collections.abc import AsyncIterator

import websockets

from market_data.config import settings


class BinanceWebSocketClient:
    """Manages a single WebSocket connection to Binance's combined stream."""

    def __init__(self, symbols: list[str]) -> None:
        self.symbols = symbols
        self._backoff_seconds = 1
        self._max_backoff_seconds = 30
        self._closing = False

    def _build_stream_url(self) -> str:
        streams = []
        for symbol in self.symbols:
            lower = symbol.lower()
            streams.append(f"{lower}@trade")
            streams.append(f"{lower}@depth@100ms")
        return f"{settings.binance_ws_url}?streams={'/'.join(streams)}"

    async def connect(self) -> AsyncIterator[dict]:
        url = self._build_stream_url()
        while not self._closing:
            try:
                async with websockets.connect(url) as ws:
                    self._backoff_seconds = 1  # reset once a connection succeeds
                    async for raw_message in ws:
                        yield json.loads(raw_message)
            except websockets.ConnectionClosed:
                if self._closing:
                    return
                await self._reconnect_with_backoff()

    async def _reconnect_with_backoff(self) -> None:
        await asyncio.sleep(self._backoff_seconds)
        self._backoff_seconds = min(self._backoff_seconds * 2, self._max_backoff_seconds)

    async def close(self) -> None:
        self._closing = True
