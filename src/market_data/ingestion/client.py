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


class Reconnected:
    """Marker yielded by connect() right after a dropped connection has
    been replaced by a fresh one (never yielded for the initial
    connection). Binance's U/u update IDs don't reset on reconnect — they
    keep incrementing continuously on Binance's side regardless of which
    connection is watching. The problem is on OUR side: whatever updates
    happened during the drop were never observed, so any previously known
    "last applied update ID" can no longer be trusted to chain correctly
    with whatever arrives next. Anything tracking sequence continuity (see
    orderbook_sync.py's OrderBook) must treat this exactly like a cold
    start — fetch a fresh snapshot and resync — not just keep applying
    updates as if nothing happened.
    """


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

    async def connect(self) -> AsyncIterator[dict | Reconnected]:
        url = self._build_stream_url()
        first_connection = True
        while not self._closing:
            try:
                async with websockets.connect(url) as ws:
                    self._backoff_seconds = 1  # reset once a connection succeeds
                    if not first_connection:
                        yield Reconnected()
                    first_connection = False
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
