"""Shared configuration and connectivity for the API and the worker."""

import os
from pathlib import Path

import asyncpg
import redis.asyncio as aioredis

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://telemetry:telemetry@localhost:5432/telemetry")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
STREAM = os.getenv("STREAM_NAME", "readings")
GROUP = os.getenv("CONSUMER_GROUP", "ingest")

SCHEMA_PATH = Path(__file__).parent / "migrations" / "001_init.sql"


async def connect_postgres(min_size: int = 1, max_size: int = 10) -> asyncpg.Pool:
    """Open a pool and make sure the schema exists (idempotent)."""
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=min_size, max_size=max_size)
    await ensure_schema(pool)
    return pool


def connect_redis() -> aioredis.Redis:
    return aioredis.from_url(REDIS_URL, decode_responses=True)


async def ensure_schema(pool: asyncpg.Pool) -> None:
    """Apply migrations/001_init.sql. Every statement is IF NOT EXISTS-idempotent."""
    sql = SCHEMA_PATH.read_text()
    async with pool.acquire() as conn:
        await conn.execute(sql)
