"""Command-line entrypoint: `uv run market-data` or `python -m market_data`.

Starts IngestionService, configures logging (the application entrypoint is
the right place for that — library modules shouldn't touch global logging
config), and shuts down cleanly on Ctrl+C.
"""

import asyncio
import logging

from market_data.ingestion import IngestionService

logger = logging.getLogger(__name__)


async def _run() -> None:
    service = IngestionService()
    try:
        await service.run()
    finally:
        # By the time we reach here, service.run() has already stopped
        # (its own stack unwound on the exception/cancellation), so the
        # "stop the loop before tearing down connections" ordering that
        # service.close() documents is satisfied — nothing is still
        # trying to publish while we close the Redis connection.
        await service.close()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        logger.info("interrupted — shut down cleanly")
