"""Queue consumer: applies readings idempotently, acknowledges, recovers crashes.

Delivery is at-least-once (Redis Streams consumer group + XACK + XAUTOCLAIM);
the Postgres constraints make the effect exactly-once.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
from datetime import UTC, datetime

import redis.asyncio as aioredis
from redis.exceptions import ResponseError

from db import GROUP, STREAM, connect_postgres, connect_redis
from worker.apply import apply_reading_safe

log = logging.getLogger("worker")
BATCH = int(os.getenv("WORKER_BATCH", "100"))
BLOCK_MS = int(os.getenv("WORKER_BLOCK_MS", "1000"))
RECOVERY_MIN_IDLE_MS = int(os.getenv("WORKER_RECOVERY_MIN_IDLE_MS", "30000"))
CONSUMER = os.getenv("WORKER_CONSUMER", f"worker-{os.getpid()}")


def parse_fields(fields: dict) -> dict:
    """Stream fields (flat strings) -> typed reading."""
    et = datetime.fromisoformat(str(fields["event_time"]).replace("Z", "+00:00"))
    if et.tzinfo is None:
        et = et.replace(tzinfo=UTC)
    return {
        "gateway_id": str(fields["gateway_id"]),
        "device_id": str(fields["device_id"]),
        "metric": str(fields["metric"]),
        "value": float(fields["value"]),
        "event_time": et,
        "sequence": int(fields["sequence"]),
    }


async def ensure_group(redis: aioredis.Redis) -> None:
    try:
        await redis.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
    except ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


async def process_entry(pg_pool, redis: aioredis.Redis, entry_id, fields: dict) -> None:
    reading = parse_fields(fields)
    await apply_reading_safe(pg_pool, **reading)
    await redis.xack(STREAM, GROUP, entry_id)


async def process_batch(pg_pool, redis: aioredis.Redis, consumer: str) -> int:
    """Read new entries and apply them. A failed apply stays unacked and is
    picked up later by recovery — it is never lost."""
    rows = await redis.xreadgroup(GROUP, consumer, {STREAM: ">"}, count=BATCH, block=BLOCK_MS)
    processed = 0
    for _stream, entries in rows:
        for entry_id, fields in entries:
            try:
                await process_entry(pg_pool, redis, entry_id, fields)
            except Exception:
                log.exception("failed to apply %s; leaving unacked for recovery", entry_id)
            else:
                processed += 1
    return processed


async def recover_pending(pg_pool, redis: aioredis.Redis, consumer: str,
                          min_idle_ms: int = RECOVERY_MIN_IDLE_MS) -> int:
    """Claim entries left pending by a crashed worker and re-apply them.

    The idempotent apply + XACK turns redelivery into exactly-once effect.
    """
    result = await redis.xautoclaim(
        STREAM, GROUP, consumer, min_idle_time=min_idle_ms, count=BATCH
    )
    entries = result[1] if isinstance(result, (tuple, list)) else result
    recovered = 0
    for entry_id, fields in entries or []:
        if not fields:  # entry was deleted while pending
            continue
        await process_entry(pg_pool, redis, entry_id, fields)
        recovered += 1
    return recovered


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s"
    )
    pg_pool = await connect_postgres()
    redis = connect_redis()
    await ensure_group(redis)
    stop = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        asyncio.get_running_loop().add_signal_handler(sig, stop.set)
    log.info("worker %s consuming stream %r group %r", CONSUMER, STREAM, GROUP)
    iterations = 0
    try:
        while not stop.is_set():
            try:
                await process_batch(pg_pool, redis, CONSUMER)
            except ResponseError as exc:
                if "NOGROUP" in str(exc):
                    await ensure_group(redis)
                else:
                    raise
            iterations += 1
            if iterations % 10 == 0:
                recovered = await recover_pending(pg_pool, redis, CONSUMER)
                if recovered:
                    log.info("recovered %d pending entries", recovered)
    finally:
        await pg_pool.close()
        await redis.aclose()
        log.info("worker stopped")


if __name__ == "__main__":
    asyncio.run(main())
