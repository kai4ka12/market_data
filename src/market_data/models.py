from datetime import datetime, timezone
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, Field


class TradeEvent(BaseModel):
    symbol: str
    trade_id: int
    price: Decimal
    quantity: Decimal
    trade_time: datetime
    aggressor_side: Literal["buy", "sell"]
    ingest_ts: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class PriceLevel(BaseModel):
    price: Decimal
    quantity: Decimal


class DepthUpdate(BaseModel):
    symbol: str
    first_update_id: int
    final_update_id: int
    bids: list[PriceLevel]
    asks: list[PriceLevel]
    exchange_ts: datetime
    ingest_ts: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
