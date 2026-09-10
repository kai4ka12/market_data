# Docker reference

Quick reference for operating this project's containers (`redis` +
`timescaledb`, defined in `docker-compose.yml`). All commands assume you're
running them from the project root, so `docker compose` picks up
`docker-compose.yml` and `.env` automatically.

## Basics — starting, stopping, status

```bash
# Start both containers in the background (detached)
docker compose up -d

# Start just one service
docker compose up -d redis
docker compose up -d timescaledb

# Stop containers but keep them (and volumes) around — fast to resume
docker compose stop

# Stop AND remove the containers (volumes survive — data is safe)
docker compose down

# Stop and remove containers AND volumes — WIPES ALL DATA, use with care
docker compose down -v

# Restart (e.g. after editing docker-compose.yml or .env)
docker compose restart
docker compose up -d --force-recreate    # if just `restart` doesn't pick up changes

# Current status + health
docker compose ps

# Follow logs (both services)
docker compose logs -f

# Follow logs for one service
docker compose logs -f redis
docker compose logs -f timescaledb
```

## Health & resource usage

```bash
# Detailed health check status (redis/timescaledb both define healthchecks)
docker inspect --format='{{.State.Health.Status}}' market-data-redis
docker inspect --format='{{.State.Health.Status}}' market-data-timescaledb

# Live resource usage (CPU/memory/network) for both containers
docker stats market-data-redis market-data-timescaledb

# Full container details (env vars, mounts, network, etc.)
docker inspect market-data-redis
```

## Volumes — where the data actually lives

```bash
# List this project's volumes
docker volume ls | grep market_data
# -> market_data_redis_data
# -> market_data_timescale_data

# Inspect a volume (see its on-disk path, etc.)
docker volume inspect market_data_redis_data

# Back up a volume to a local tarball
docker run --rm -v market_data_redis_data:/data -v "$(pwd)":/backup \
  alpine tar czf /backup/redis_backup.tar.gz -C /data .

docker run --rm -v market_data_timescale_data:/data -v "$(pwd)":/backup \
  alpine tar czf /backup/timescale_backup.tar.gz -C /data .

# Wipe just one volume without touching the other (containers must be stopped first)
docker compose stop redis
docker volume rm market_data_redis_data
docker compose up -d redis    # recreates it empty
```

---

## Redis — `market-data-redis`

```bash
# Open an interactive redis-cli shell inside the container
docker exec -it market-data-redis redis-cli

# Run a single command without an interactive shell
docker exec market-data-redis redis-cli PING
docker exec market-data-redis redis-cli INFO server
docker exec market-data-redis redis-cli DBSIZE
```

### Streams (what this project actually publishes to)

```bash
# List all market-data streams
docker exec market-data-redis redis-cli KEYS 'md:*'

# Number of entries in a stream
docker exec market-data-redis redis-cli XLEN md:trades:BTCUSDT

# Most recent N entries (newest first)
docker exec market-data-redis redis-cli XREVRANGE md:trades:BTCUSDT + - COUNT 5

# Oldest N entries (oldest first)
docker exec market-data-redis redis-cli XRANGE md:trades:BTCUSDT - + COUNT 5

# Stream metadata (length, first/last entry, radix tree info)
docker exec market-data-redis redis-cli XINFO STREAM md:trades:BTCUSDT

# Watch new entries arrive live (blocks — Ctrl+C to stop)
docker exec market-data-redis redis-cli XREAD BLOCK 0 STREAMS md:trades:BTCUSDT '$'

# Delete one stream entirely
docker exec market-data-redis redis-cli DEL md:trades:BTCUSDT

# Delete ALL market-data streams (careful)
docker exec market-data-redis redis-cli --scan --pattern 'md:*' | xargs -r docker exec market-data-redis redis-cli DEL

# Wipe everything in Redis (all keys, all DBs) — careful
docker exec market-data-redis redis-cli FLUSHALL
```

### Monitoring / debugging

```bash
# See every command hitting Redis in real time (heavy — dev/debug only, Ctrl+C to stop)
docker exec -it market-data-redis redis-cli MONITOR

# Memory usage stats
docker exec market-data-redis redis-cli INFO memory

# Connected clients
docker exec market-data-redis redis-cli CLIENT LIST
```

---

## TimescaleDB / Postgres — `market-data-timescaledb`

Credentials come from `.env` (`POSTGRES_USER`, `POSTGRES_PASSWORD`,
`POSTGRES_DB`) — the commands below use `market_data` as the example
username/db; swap in whatever your `.env` actually has.

```bash
# Open an interactive psql shell inside the container
docker exec -it market-data-timescaledb psql -U market_data -d market_data

# Run a single query without an interactive shell
docker exec market-data-timescaledb psql -U market_data -d market_data -c "SELECT version();"

# Check the container is actually accepting connections
docker exec market-data-timescaledb pg_isready -U market_data
```

### Useful psql commands (once inside the interactive shell)

```sql
\l              -- list databases
\dt             -- list tables in the current database
\d table_name   -- describe a table's columns/indexes
\dx             -- list installed extensions (confirm timescaledb is there)
\q              -- quit
```

### TimescaleDB-specific checks

```sql
-- Confirm the timescaledb extension is enabled
SELECT * FROM pg_extension WHERE extname = 'timescaledb';

-- List hypertables once you've created any (Phase 2)
SELECT * FROM timescaledb_information.hypertables;
```

### Backup / restore (logical dump, separate from the volume-level backup above)

```bash
# Dump the database to a local .sql file
docker exec market-data-timescaledb pg_dump -U market_data market_data > backup.sql

# Restore from that dump into a running (empty) database
docker exec -i market-data-timescaledb psql -U market_data -d market_data < backup.sql
```

---

## Common gotchas

- **`.env` changes only apply on first container creation** for `POSTGRES_USER`/`POSTGRES_PASSWORD`/`POSTGRES_DB` — Postgres only runs its init scripts against an empty data volume. To pick up new credentials you need `docker compose down -v` (wipes data) then `docker compose up -d` again, or manually `ALTER USER ... WITH PASSWORD ...` inside the running container.
- **Redis has no auth configured** in this setup — anything that can reach port 6379 can connect. Fine for local dev only.
- **Both services publish ports to the host** (`6379`, `5432`), so `localhost` from your machine (or from a `uv run` process outside Docker) reaches them directly — no need to `docker exec` into anything just to connect from your own Python code.
