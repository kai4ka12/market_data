"""Database connection pool and schema bootstrap for the storage layer.

asyncpg is the async Postgres driver (chosen back in Phase 0 over the
sync psycopg2 precisely so DB calls can be awaited without blocking the
event loop). A connection *pool* hands out a small set of reusable
connections rather than opening a new one per query — opening a Postgres
connection is expensive (TCP + TLS + auth + backend process fork), so you
want to pay that once and keep them.
"""

from pathlib import Path

import asyncpg

from market_data.config import settings

_SCHEMA_PATH = Path(__file__).parent / "schema.sql"


async def create_pool() -> asyncpg.Pool:
    return await asyncpg.create_pool(
        settings.database_url,
        min_size=1,
        max_size=5,
    )


async def apply_schema(pool: asyncpg.Pool) -> None:
    """Run schema.sql. Idempotent — safe to call on every startup."""
    sql = _SCHEMA_PATH.read_text()
    async with pool.acquire() as conn:
        await conn.execute(sql)
