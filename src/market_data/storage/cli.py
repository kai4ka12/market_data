"""Entrypoint for the storage writer: `uv run market-data-writer`.

Separate process from the ingestion service — ingestion writes to Redis,
the writer drains Redis into TimescaleDB. They're decoupled on purpose:
the writer can be stopped, restarted, or fall behind without ingestion
losing anything (Redis buffers), and either can be scaled independently.
"""

import asyncio
import logging

from market_data.storage.db import apply_schema, create_pool
from market_data.storage.writer import TradeWriter

logger = logging.getLogger(__name__)


async def _run() -> None:
    pool = await create_pool()
    try:
        await apply_schema(pool)
        writer = TradeWriter(pool)
        try:
            await writer.run()
        finally:
            await writer.close()
    finally:
        await pool.close()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        logger.info("interrupted — shut down cleanly")
