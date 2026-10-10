"""Fixtures backed by REAL Postgres and Redis — the invariants are constraint
behavior, so tests must run against the actual engines, not fakes."""

import os

import asyncpg
import pytest
import redis.asyncio as aioredis
from httpx import ASGITransport, AsyncClient

os.environ.setdefault("DATABASE_URL", "postgresql://telemetry:telemetry@localhost:5432/telemetry")
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/1")

from api.main import app  # noqa: E402 - import after env is pinned
from db import ensure_schema  # noqa: E402
from worker.main import ensure_group  # noqa: E402


@pytest.fixture
async def pg():
    pool = await asyncpg.create_pool(os.environ["DATABASE_URL"])
    await ensure_schema(pool)
    yield pool
    await pool.close()


@pytest.fixture
async def r():
    client = aioredis.from_url(os.environ["REDIS_URL"], decode_responses=True)
    await client.flushdb()
    yield client
    await client.aclose()


@pytest.fixture
async def clean(pg, r):
    """Fresh slate per test: empty tables, fresh stream + consumer group."""
    await pg.execute(
        "TRUNCATE devices, readings, device_state, late_readings, gateway_state,"
        " reading_conflicts"
    )
    await r.flushdb()
    await ensure_group(r)


@pytest.fixture
async def client(pg, r):
    """In-process API client sharing the test pool/redis."""
    app.state.pg = pg
    app.state.redis = r
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
