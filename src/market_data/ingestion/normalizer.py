"""Converts raw Binance JSON messages into TradeEvent / DepthUpdate models.

Raw messages arrive from client.py already JSON-decoded, but still wrapped
in Binance's combined-stream envelope:
    {"stream": "btcusdt@trade", "data": {...actual Binance fields...}}

Pure data transformation — no I/O involved, so nothing in this file needs
to be async (see the "async only where you actually await something" rule
from client.py).
"""

from datetime import datetime, timezone
from decimal import Decimal

from market_data.models import DepthUpdate, PriceLevel, TradeEvent


def normalize_trade(data: dict) -> TradeEvent:
    return TradeEvent(
        symbol=data["s"],
        trade_id=data["t"],
        price=Decimal(data["p"]),
        quantity=Decimal(data["q"]),
        trade_time=datetime.fromtimestamp(data["T"] / 1000, tz=timezone.utc),
        aggressor_side="sell" if data["m"] else "buy",
    )


def normalize_depth(data: dict) -> DepthUpdate:
    return DepthUpdate(
        symbol=data["s"],
        first_update_id=data["U"],
        final_update_id=data["u"],
        bids=[PriceLevel(price=Decimal(p), quantity=Decimal(q)) for p, q in data["b"]],
        asks=[PriceLevel(price=Decimal(p), quantity=Decimal(q)) for p, q in data["a"]],
        exchange_ts=datetime.fromtimestamp(data["E"] / 1000, tz=timezone.utc),
    )


def normalize(raw_message: dict) -> TradeEvent | DepthUpdate | None:
    data = raw_message["data"]
    event_type = data.get("e")
    if event_type == "trade":
        return normalize_trade(data)
    if event_type == "depthUpdate":
        return normalize_depth(data)
    return None
