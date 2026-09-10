"""Orchestrates the full ingestion pipeline.

Wires client.py -> normalizer.py -> orderbook_sync.py -> publisher.py
together: connects to Binance, keeps one synced OrderBook per symbol, and
publishes every normalized event to Redis regardless of that symbol's sync
state (Redis holds the raw exchange history — that's valid and worth
storing even for the brief window a symbol's local book is resyncing;
OrderBook is a separate, in-memory concern for real-time feature work
later in Phase 3).
"""

import asyncio
import logging

from market_data.config import settings
from market_data.ingestion.client import BinanceWebSocketClient, Reconnected
from market_data.ingestion.normalizer import normalize
from market_data.ingestion.orderbook_sync import OrderBook, OrderBookOutOfSync
from market_data.ingestion.publisher import Publisher
from market_data.models import DepthUpdate

logger = logging.getLogger(__name__)


class IngestionService:
    def __init__(self) -> None:
        self.symbols = settings.symbols
        self.client = BinanceWebSocketClient(self.symbols)
        self.publisher = Publisher()
        self.books = {symbol: OrderBook(symbol) for symbol in self.symbols}
        self._resync_tasks: dict[str, asyncio.Task] = {}

    async def run(self) -> None:
        logger.info("ingestion started (symbols=%s)", self.symbols)
        for book in self.books.values():
            self._start_resync(book)

        async for msg in self.client.connect():
            if isinstance(msg, Reconnected):
                logger.warning("WebSocket reconnected — resyncing all order books")
                for book in self.books.values():
                    book.mark_stale()
                    self._start_resync(book)
                continue

            event = normalize(msg)
            if event is None:
                continue

            if isinstance(event, DepthUpdate):
                book = self.books[event.symbol]
                try:
                    book.apply_update(event)
                except OrderBookOutOfSync as exc:
                    if self._start_resync(book):
                        logger.warning("%s: %s", event.symbol, exc)

            await self.publisher.publish(event)

    def _start_resync(self, book: OrderBook) -> bool:
        """Fire-and-forget a resync, with its own retry loop — a single
        resync() attempt can legitimately fail (the REST snapshot can
        arrive already stale relative to the live stream; see
        orderbook_sync.py), so this keeps retrying until one succeeds
        rather than leaving the book permanently unsynced after one bad
        attempt.

        Idempotent per symbol: if a resync is already in flight for this
        book, does nothing and returns False (so callers know not to log
        it as a new event). This matters a lot — without it, the main
        loop's own apply_update() calls (for live messages arriving while
        a retry is sleeping) can independently raise OrderBookOutOfSync
        too, and each one would spawn a competing resync task for the same
        book. Two tasks concurrently clearing/reseeding/replaying into the
        same OrderBook is exactly what caused the repeated failures and
        stray "gap detected" warnings seen in testing.
        """
        existing = self._resync_tasks.get(book.symbol)
        if existing is not None and not existing.done():
            return False
        task = asyncio.create_task(self._resync_with_retry(book))
        self._resync_tasks[book.symbol] = task
        return True

    async def _resync_with_retry(self, book: OrderBook) -> None:
        # Re-check book.synced on every loop iteration, not just once:
        # while this task was asleep between retries, the main loop's own
        # direct apply_update() calls might have already found the bridge
        # and synced the book through a completely different path. Without
        # this check, waking up and blindly calling resync() again would
        # wastefully (and incorrectly) wipe a book that's already fine.
        while not book.synced:
            try:
                await book.resync()
            except OrderBookOutOfSync as exc:
                logger.warning("%s: resync failed (%s), retrying", book.symbol, exc)
                await asyncio.sleep(1)
        logger.info("%s: order book synced", book.symbol)

    async def close(self) -> None:
        """Tear down connections.

        Call order matters: cancel and await whatever task is running
        run() BEFORE calling this. client.close() only stops the receive
        loop between messages (see its own docstring caveat), so if
        run()'s task is still alive when this closes the Redis
        connection, a message already in flight can try to publish() on
        an already-closed connection and crash. This method only cleans
        up — stopping run() is the caller's responsibility.
        """
        await self.client.close()
        await self.publisher.close()
        for task in list(self._resync_tasks.values()):
            task.cancel()
