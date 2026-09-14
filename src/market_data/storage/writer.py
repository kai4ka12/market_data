"""Consumes trade events from Redis Streams and writes them to TimescaleDB.

Uses a Redis *consumer group* (not a plain XREAD) so that:
  - progress is tracked server-side (each message is "pending" until XACK'd),
    which gives crash recovery — a writer that dies mid-batch re-reads its
    unacked messages on restart;
  - the work could be split across multiple writer processes later, each a
    named consumer in the same group, without any of them seeing the same
    message twice.

Delivery is at-least-once (a crash between "insert" and "XACK" means the
message is redelivered), so the DB write is made idempotent with
ON CONFLICT DO NOTHING on the (symbol, trade_id, trade_time) key.
"""

import asyncio
import logging
import os
import socket
from collections import defaultdict

import asyncpg
from pydantic import ValidationError
from redis.asyncio import Redis
from redis.exceptions import ResponseError

from market_data.config import settings
from market_data.models import TradeEvent

logger = logging.getLogger(__name__)

GROUP = "trades-storage"
BATCH_SIZE = 500
BLOCK_MS = 2000

_INSERT_SQL = """
    INSERT INTO trades (trade_time, symbol, trade_id, price, quantity, aggressor, ingest_ts)
    SELECT trade_time, symbol, trade_id, price, quantity, aggressor::aggressor_side, ingest_ts
    FROM unnest(
        $1::timestamptz[], $2::text[], $3::bigint[],
        $4::numeric[], $5::numeric[], $6::text[], $7::timestamptz[]
    ) AS t(trade_time, symbol, trade_id, price, quantity, aggressor, ingest_ts)
    ON CONFLICT (symbol, trade_id, trade_time) DO NOTHING
"""


class TradeWriter:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool
        self._redis: Redis = Redis.from_url(settings.redis_url, decode_responses=True)
        self._streams = [f"md:trades:{symbol}" for symbol in settings.symbols]
        # Unique per running process, so scaling to multiple writers later
        # just works — each is a distinct consumer in the group.
        self._consumer = f"writer-{socket.gethostname()}-{os.getpid()}"
        self._closing = False

    async def run(self) -> None:
        await self._ensure_groups()
        await self._drain_pending()
        logger.info(
            "trade writer consuming %s as %s", self._streams, self._consumer
        )
        while not self._closing:
            raw = await self._read(new_only=True)
            if raw:
                await self._process(raw)

    async def close(self) -> None:
        self._closing = True
        await self._redis.aclose()

    async def _ensure_groups(self) -> None:
        for stream in self._streams:
            try:
                # id="$" => the group only sees messages published from now
                # on. id="0" would instead replay the whole existing stream
                # — the "right" choice for a durable writer, but these
                # streams still contain incompatible entries from an older
                # implementation, so start fresh for now.
                await self._redis.xgroup_create(stream, GROUP, id="$", mkstream=True)
            except ResponseError as exc:
                if "BUSYGROUP" not in str(exc):
                    raise  # group already exists is fine; anything else isn't

    async def _drain_pending(self) -> None:
        """Reprocess anything this consumer was delivered but never ACK'd
        (i.e. we crashed mid-batch last time). Idempotent inserts make
        replaying them harmless.
        """
        recovered = 0
        while True:
            raw = await self._read(new_only=False)
            if not raw:
                break
            await self._process(raw)
            recovered += len(raw)
        if recovered:
            logger.info("recovered %d pending entries on startup", recovered)

    async def _read(self, *, new_only: bool) -> list[tuple[str, str, dict]]:
        # ">" = messages never delivered to any consumer in the group.
        # "0" = this consumer's already-delivered-but-unacked backlog.
        cursor = ">" if new_only else "0"
        resp = await self._redis.xreadgroup(
            GROUP,
            self._consumer,
            {stream: cursor for stream in self._streams},
            count=BATCH_SIZE,
            block=BLOCK_MS if new_only else None,
        )
        return [
            (stream, msg_id, fields)
            for stream, messages in (resp or [])
            for msg_id, fields in messages
        ]

    async def _process(self, raw: list[tuple[str, str, dict]]) -> None:
        events: list[TradeEvent] = []
        ack_ids: dict[str, list[str]] = defaultdict(list)
        for stream, msg_id, fields in raw:
            ack_ids[stream].append(msg_id)
            try:
                events.append(TradeEvent.model_validate_json(fields["data"]))
            except (KeyError, ValidationError) as exc:
                # Poison message — log and ACK anyway so it doesn't block
                # the pending list forever. A real system would dead-letter it.
                logger.warning("skipping unparseable %s/%s: %s", stream, msg_id, exc)

        if events:
            await self._insert(events)

        # Insert first, ACK second. If we crash in between, the messages
        # stay pending and get redelivered next run — and re-inserted
        # harmlessly, thanks to ON CONFLICT DO NOTHING.
        for stream, ids in ack_ids.items():
            await self._redis.xack(stream, GROUP, *ids)

    async def _insert(self, events: list[TradeEvent]) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                _INSERT_SQL,
                [e.trade_time for e in events],
                [e.symbol for e in events],
                [e.trade_id for e in events],
                [e.price for e in events],
                [e.quantity for e in events],
                [e.aggressor_side for e in events],
                [e.ingest_ts for e in events],
            )
