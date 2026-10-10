"""Queue consumer: applies readings idempotently, acknowledges, recovers crashes.

Delivery is at-least-once (Redis Streams consumer group + XACK + XAUTOCLAIM);
the Postgres constraints make the effect exactly-once.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import socket
from datetime import UTC, datetime

import redis.asyncio as aioredis
from redis.exceptions import ResponseError

from db import GROUP, STREAM, connect_postgres, connect_redis
from worker.apply import apply_reading_safe

log = logging.getLogger("worker")
BATCH = int(os.getenv("WORKER_BATCH", "100"))
BLOCK_MS = int(os.getenv("WORKER_BLOCK_MS", "1000"))
RECOVERY_MIN_IDLE_MS = int(os.getenv("WORKER_RECOVERY_MIN_IDLE_MS", "30000"))
# Unique per worker: in a container the worker is always PID 1, so the PID
# alone would make every container the same Redis consumer. The hostname
# (container ID) tells them apart; the PID separates processes on one host.
MAX_ATTEMPTS = int(os.getenv("WORKER_MAX_ATTEMPTS", "5"))
CONSUMER = os.getenv("WORKER_CONSUMER", f"worker-{socket.gethostname()}-{os.getpid()}")


def parse_fields(fields: dict) -> dict:
    """Stream fields (flat strings) -> typed reading."""
    et = datetime.fromisoformat(str(fields["event_time"]).replace("Z", "+00:00"))
    if et.tzinfo is None:
        et = et.replace(tzinfo=UTC)
    received_at = None
    if fields.get("received_at"):
        received_at = datetime.fromisoformat(str(fields["received_at"]))
    return {
        "received_at": received_at,
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


async def _times_delivered(redis: aioredis.Redis, entry_id) -> int:
    info = await redis.xpending_range(STREAM, GROUP, min=entry_id, max=entry_id, count=1)
    return int(info[0]["times_delivered"]) if info else 0


async def dead_letter(pg_pool, redis: aioredis.Redis, entry_id, fields: dict,
                      error: BaseException, attempts: int) -> None:
    """Park an entry that can never be applied, then acknowledge it.

    Written to Postgres BEFORE the ack: if Postgres is down this raises, the
    entry stays pending in the durable queue, and nothing is lost.
    """
    async with pg_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO dead_letters (stream_entry_id, fields, error, attempts)"
            " VALUES ($1, $2::jsonb, $3, $4) ON CONFLICT (stream_entry_id) DO NOTHING",
            str(entry_id),
            json.dumps(fields),
            f"{type(error).__name__}: {error}"[:2000],
            attempts,
        )
    await redis.xack(STREAM, GROUP, entry_id)


async def recover_pending(pg_pool, redis: aioredis.Redis, consumer: str,
                          min_idle_ms: int = RECOVERY_MIN_IDLE_MS,
                          max_attempts: int = MAX_ATTEMPTS) -> int:
    """Claim entries left pending (crashed worker, or a failed apply) and retry.

    Always try to apply first. Only if that fails AND the entry has already
    been delivered max_attempts times is it dead-lettered, so a Postgres
    outage that fails everything for a few minutes dead-letters nothing: once
    the database is back, the retry simply succeeds. A failure here is logged,
    never raised, so one bad entry can't crash the worker.
    """
    result = await redis.xautoclaim(
        STREAM, GROUP, consumer, min_idle_time=min_idle_ms, count=BATCH
    )
    entries = result[1] if isinstance(result, (tuple, list)) else result
    recovered = 0
    for entry_id, fields in entries or []:
        if not fields:  # entry was deleted while pending
            continue
        try:
            await process_entry(pg_pool, redis, entry_id, fields)
            recovered += 1
        except Exception as exc:  # noqa: BLE001 - any failure is handled the same way
            try:
                attempts = await _times_delivered(redis, entry_id)
                if attempts >= max_attempts:
                    await dead_letter(pg_pool, redis, entry_id, fields, exc, attempts)
                    log.error("dead-lettered %s after %d attempts: %s", entry_id, attempts, exc)
                else:
                    log.warning("retry %d/%d failed for %s: %s",
                                attempts, max_attempts, entry_id, exc)
            except Exception:  # noqa: BLE001 - e.g. Postgres down: stays pending
                log.exception("could not dead-letter %s; leaving pending", entry_id)
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
                try:
                    recovered = await recover_pending(pg_pool, redis, CONSUMER)
                except Exception:  # noqa: BLE001 - keep consuming; retry next round
                    log.exception("recovery round failed")
                else:
                    if recovered:
                        log.info("recovered %d pending entries", recovered)
    finally:
        await pg_pool.close()
        await redis.aclose()
        log.info("worker stopped")


if __name__ == "__main__":
    asyncio.run(main())
