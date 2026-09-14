-- Storage schema for the market-data pipeline.
--
-- Applied by storage/db.py:apply_schema() on writer startup. Every
-- statement is idempotent (IF NOT EXISTS / exception-swallowing) so it's
-- safe to run on every boot.

-- TimescaleDB is a Postgres *extension* — the timescale/timescaledb image
-- pre-installs it, but a database still has to enable it explicitly.
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Aggressor side as a real enum type, mirroring Literal["buy","sell"] in
-- models.py — the DB rejects anything else at write time. CREATE TYPE has
-- no IF NOT EXISTS, so swallow the duplicate error on re-runs.
DO $$ BEGIN
    CREATE TYPE aggressor_side AS ENUM ('buy', 'sell');
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS trades (
    trade_time  timestamptz     NOT NULL,
    symbol      text            NOT NULL,
    trade_id    bigint          NOT NULL,
    price       numeric         NOT NULL,
    quantity    numeric         NOT NULL,
    aggressor   aggressor_side  NOT NULL,
    ingest_ts   timestamptz     NOT NULL,
    -- Dedup key. Binance's (symbol, trade_id) uniquely identifies a trade;
    -- trade_time is included only because TimescaleDB requires the
    -- partitioning column to be part of every unique constraint. A given
    -- trade always carries the same trade_time, so this doesn't weaken
    -- the dedup — reprocessing the same Redis message hits the same row.
    PRIMARY KEY (symbol, trade_id, trade_time)
);

-- Turn `trades` into a hypertable: transparently partitioned into
-- per-time-range child tables ("chunks"). 1-day chunks — at a few
-- million rows/day that keeps the newest chunk + its indexes small
-- enough to stay hot in memory, which is where TimescaleDB's speed comes
-- from. if_not_exists keeps this safe to re-run.
SELECT create_hypertable(
    'trades', 'trade_time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- The hypertable auto-indexes trade_time. Almost every query also filters
-- by symbol ("BTCUSDT trades between 14:00 and 15:00"), so add a
-- composite index leading with symbol, then time descending (most recent
-- first — the common access pattern).
CREATE INDEX IF NOT EXISTS trades_symbol_time_idx
    ON trades (symbol, trade_time DESC);
