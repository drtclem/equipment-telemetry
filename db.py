"""Shared configuration and connectivity for the API and the worker."""

import os
from pathlib import Path

import asyncpg
import redis.asyncio as aioredis

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://telemetry:telemetry@localhost:5432/telemetry")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
STREAM = os.getenv("STREAM_NAME", "readings")
GROUP = os.getenv("CONSUMER_GROUP", "ingest")

# How long a gateway may go without a reading before the system stops assuming
# its data is merely in flight and says so: its devices are flagged "may be
# behind" and, when it speaks again, the gap is written to the outage log.
# Should be ~3x a site's normal reporting interval; 60 s is a default.
SILENCE_THRESHOLD_SECONDS = int(os.getenv("SILENCE_THRESHOLD_SECONDS", "60"))

MIGRATIONS_DIR = Path(__file__).parent / "migrations"


async def connect_postgres(min_size: int = 1, max_size: int = 10) -> asyncpg.Pool:
    """Open a pool and make sure the schema exists (idempotent)."""
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=min_size, max_size=max_size)
    await ensure_schema(pool)
    return pool


def connect_redis() -> aioredis.Redis:
    return aioredis.from_url(REDIS_URL, decode_responses=True)


async def ensure_schema(pool: asyncpg.Pool) -> None:
    """Apply migrations/*.sql in name order. Every migration is idempotent."""
    async with pool.acquire() as conn:
        for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
            await conn.execute(path.read_text())
