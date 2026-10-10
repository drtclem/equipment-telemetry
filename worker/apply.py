"""Idempotent application of a reading to Postgres — the three invariants live here.

Every path through this module is safe to re-run on the same input: a
redelivered queue entry (worker crashed before acknowledging) applies as a
no-op. Correctness comes from constraints, not from delivery semantics.
"""

from __future__ import annotations

import asyncio
import datetime as dt

import asyncpg

# Invariant 2 — only a strictly newer reading (event time, with the gateway
# sequence as tie-break) may overwrite current state. A replayed backlog
# therefore can never clobber newer data, no matter when it arrives.
_STATE_UPSERT = """
INSERT INTO device_state AS ds (device_id, metric, value, event_time, sequence, gateway_id)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (device_id, metric) DO UPDATE
SET value = EXCLUDED.value,
    event_time = EXCLUDED.event_time,
    sequence = EXCLUDED.sequence,
    gateway_id = EXCLUDED.gateway_id,
    updated_at = now()
WHERE (EXCLUDED.event_time, EXCLUDED.sequence) > (ds.event_time, ds.sequence)
"""

_QUIET_PERIOD = "60 seconds"


async def apply_reading(
    conn: asyncpg.Connection,
    *,
    gateway_id: str,
    device_id: str,
    metric: str,
    value: float,
    event_time: dt.datetime,
    sequence: int,
) -> None:
    """Apply one reading in a single transaction."""
    async with conn.transaction():
        # Invariant 3 — a device registers exactly once, even when two gateways
        # first-report it concurrently: the primary key arbitrates.
        await conn.execute(
            "INSERT INTO devices (device_id, first_gateway)"
            " VALUES ($1, $2) ON CONFLICT (device_id) DO NOTHING",
            device_id,
            gateway_id,
        )

        # Invariant 1 — history keeps each reading exactly once. A reading's
        # identity is (gateway, device, metric, sequence, event_time): a real
        # resend is byte-identical and collapses; a reused counter with a new
        # timestamp is a new reading and is kept. RETURNING tells us whether
        # THIS delivery stored a row.
        inserted = await conn.fetch(
            "INSERT INTO readings (gateway_id, device_id, metric, value, event_time, sequence)"
            " VALUES ($1, $2, $3, $4, $5, $6)"
            " ON CONFLICT ON CONSTRAINT readings_identity_key DO NOTHING RETURNING id",
            gateway_id,
            device_id,
            metric,
            value,
            event_time,
            sequence,
        )

        if not inserted:
            # Same identity already stored. Identical value = a resend: no-op.
            # Different value = not a resend: record it, never drop it silently.
            stored_value = await conn.fetchval(
                "SELECT value FROM readings WHERE gateway_id = $1 AND device_id = $2"
                " AND metric = $3 AND sequence = $4 AND event_time = $5",
                gateway_id,
                device_id,
                metric,
                sequence,
                event_time,
            )
            if stored_value is not None and stored_value != value:
                await conn.execute(
                    "INSERT INTO reading_conflicts (gateway_id, device_id, metric, sequence,"
                    " event_time, stored_value, incoming_value)"
                    " VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT DO NOTHING",
                    gateway_id,
                    device_id,
                    metric,
                    sequence,
                    event_time,
                    stored_value,
                    value,
                )
            # Current state may only ever point at a reading that is in history,
            # so a delivery that stored nothing changes nothing else.
            await _touch_gateway(conn, gateway_id, event_time)
            return

        prev_event_time = await conn.fetchval(
            "SELECT event_time FROM device_state WHERE device_id = $1 AND metric = $2",
            device_id,
            metric,
        )

        await conn.execute(
            _STATE_UPSERT, device_id, metric, value, event_time, sequence, gateway_id
        )

        if (
            prev_event_time is not None
            and event_time < prev_event_time
        ):
            # Arrival-time ledger: late data is recorded, never dropped and
            # never allowed to regress state. Only reached for a newly stored
            # reading, so a re-sent or re-delivered one never re-flags lateness —
            # lateness is a property of a reading, not of a delivery, so each
            # distinct reading is flagged at most once. Also flags the gateway
            # as replaying so the dashboard shows a reconciliation badge.
            await conn.execute(
                "INSERT INTO late_readings"
                " (device_id, metric, reading_event_time, state_event_time, gateway_id)"
                " VALUES ($1, $2, $3, $4, $5)",
                device_id,
                metric,
                event_time,
                prev_event_time,
                gateway_id,
            )
            await conn.execute(
                "INSERT INTO gateway_state (gateway_id, max_event_time, last_seen_at,"
                " replaying, last_late_at)"
                " VALUES ($1, $2, now(), true, now())"
                " ON CONFLICT (gateway_id) DO UPDATE SET replaying = true, last_late_at = now()",
                gateway_id,
                event_time,
            )

        await _touch_gateway(conn, gateway_id, event_time)


async def _touch_gateway(conn: asyncpg.Connection, gateway_id: str,
                         event_time: dt.datetime) -> None:
    """Gateway bookkeeping, and badge clearing once the replay looks drained:
    no new late detection for a full quiet period."""
    await conn.execute(
        "INSERT INTO gateway_state (gateway_id, max_event_time, last_seen_at)"
        " VALUES ($1, $2, now())"
        " ON CONFLICT (gateway_id) DO UPDATE SET"
        " max_event_time = GREATEST(COALESCE(gateway_state.max_event_time,"
        " '-infinity'::timestamptz), EXCLUDED.max_event_time),"
        " last_seen_at = now()",
        gateway_id,
        event_time,
    )
    await conn.execute(
        "UPDATE gateway_state SET replaying = false"
        " WHERE gateway_id = $1 AND replaying AND last_late_at IS NOT NULL"
        f" AND last_late_at < now() - interval '{_QUIET_PERIOD}'",
        gateway_id,
    )


async def apply_reading_safe(pool: asyncpg.Pool, **reading) -> None:
    """apply_reading with a bounded retry for rare serialization conflicts.

    Two workers racing on the same device can in principle deadlock or hit a
    serialization failure; the conflict-handling invariants make any retry a
    no-op, so retrying is always safe.
    """
    for attempt in range(3):
        try:
            async with pool.acquire() as conn:
                await apply_reading(conn, **reading)
            return
        except (asyncpg.DeadlockDetectedError, asyncpg.SerializationError):
            if attempt == 2:
                raise
            await asyncio.sleep(0.05 * (attempt + 1))
