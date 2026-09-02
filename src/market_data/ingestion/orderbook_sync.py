"""Local order-book synchronization for one symbol.

Binance's diff-depth stream only sends *incremental* changes — to have a
correct local order book you must combine it with a REST snapshot and
handle out-of-order/dropped updates. This follows Binance's documented
algorithm:
https://binance-docs.github.io/apidocs/spot/en/#how-to-manage-a-local-order-book-correctly

The gap-detection/apply logic here is pure computation — no I/O — and is
deliberately kept separate from `_fetch_snapshot`, the one method that
actually touches the network, so the tricky part can be unit-tested
without a live connection.
"""

import asyncio
import json
import urllib.request
from decimal import Decimal

from market_data.config import settings
from market_data.models import DepthUpdate


class OrderBookOutOfSync(Exception):
    """Raised when a gap is detected, or a snapshot turns out too old to
    bridge to the buffered updates — either way, resync() must be called
    again (with a fresh snapshot) before the book can be trusted."""


class OrderBook:
    """Maintains one symbol's local order book, synced against Binance's
    diff-depth stream.

    Usage:
        book = OrderBook("BTCUSDT")
        resync_task = asyncio.create_task(book.resync())  # start the REST fetch
        async for update in depth_stream:
            book.apply_update(update)   # safe to call before resync() finishes —
                                         # updates are buffered/bridged automatically
        # apply_update raises OrderBookOutOfSync on a detected gap after
        # being synced — catch it, call resync() again, and keep going.
    """

    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        self.bids: dict[Decimal, Decimal] = {}
        self.asks: dict[Decimal, Decimal] = {}
        self._last_update_id: int | None = None
        self._synced = False
        self._buffer: list[DepthUpdate] = []

    @property
    def synced(self) -> bool:
        return self._synced

    def apply_update(self, update: DepthUpdate) -> None:
        """Apply one live DepthUpdate — safe to call at any point in the
        book's lifecycle; behaves differently depending on where it's up to:

        1. No snapshot fetched yet (_last_update_id is None): just buffer.
        2. Snapshot fetched but not yet bridged to the live stream: look
           for the bridging event (U <= lastUpdateId+1 <= u). Anything
           older than the snapshot is stale and safely dropped. A
           non-stale event that DOESN'T bridge means the snapshot was
           already too old — raise, caller must resync() with a fresh one.
        3. Fully synced: strict chain check (this update's U must equal
           the last applied update's u + 1) — any gap raises immediately.
        """
        if self._last_update_id is None:
            self._buffer.append(update)
            return

        if not self._synced:
            if update.final_update_id <= self._last_update_id:
                return  # stale: snapshot already reflects this update
            if not (
                update.first_update_id
                <= self._last_update_id + 1
                <= update.final_update_id
            ):
                raise OrderBookOutOfSync(
                    f"{self.symbol}: snapshot too stale to bridge to the "
                    f"live stream — resync() again with a fresh snapshot"
                )
            self._apply(update)
            self._synced = True
            return

        if update.first_update_id != self._last_update_id + 1:
            self._synced = False
            raise OrderBookOutOfSync(
                f"{self.symbol}: expected U={self._last_update_id + 1}, "
                f"got U={update.first_update_id} (gap detected)"
            )
        self._apply(update)

    async def resync(self) -> None:
        """(Re)synchronize from a fresh REST snapshot, then replay any
        updates buffered while the snapshot was in flight through the same
        apply_update() logic used for live updates.
        """
        self._synced = False
        self.bids.clear()
        self.asks.clear()

        snapshot = await self._fetch_snapshot()
        self._last_update_id = snapshot["lastUpdateId"]
        for price, qty in snapshot["bids"]:
            self._set_level(self.bids, Decimal(price), Decimal(qty))
        for price, qty in snapshot["asks"]:
            self._set_level(self.asks, Decimal(price), Decimal(qty))

        buffered, self._buffer = self._buffer, []
        for update in buffered:
            self.apply_update(update)

    def _apply(self, update: DepthUpdate) -> None:
        for level in update.bids:
            self._set_level(self.bids, level.price, level.quantity)
        for level in update.asks:
            self._set_level(self.asks, level.price, level.quantity)
        self._last_update_id = update.final_update_id

    @staticmethod
    def _set_level(
        side: dict[Decimal, Decimal], price: Decimal, quantity: Decimal
    ) -> None:
        if quantity == 0:
            side.pop(price, None)
        else:
            side[price] = quantity

    async def _fetch_snapshot(self) -> dict:
        url = f"{settings.binance_rest_url}/api/v3/depth?symbol={self.symbol}&limit=1000"

        def _get() -> dict:
            with urllib.request.urlopen(url) as response:
                return json.loads(response.read())

        # urllib is blocking (stdlib, no async HTTP client dependency) —
        # asyncio.to_thread runs it on a worker thread so it doesn't freeze
        # the event loop while waiting on the network.
        return await asyncio.to_thread(_get)
