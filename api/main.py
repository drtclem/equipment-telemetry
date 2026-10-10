"""Equipment Telemetry Ingestion — HTTP API and dashboard host."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from db import GROUP, SILENCE_THRESHOLD_SECONDS, STREAM, connect_postgres, connect_redis

DASHBOARD_DIR = Path(__file__).resolve().parent.parent / "dashboard"


class ReadingIn(BaseModel):
    gateway_id: str = Field(min_length=1, max_length=200)
    device_id: str = Field(min_length=1, max_length=200)
    metric: str = Field(min_length=1, max_length=200)
    value: float = Field(allow_inf_nan=False)  # NaN/inf is never real data
    event_time: datetime
    sequence: int = Field(ge=0)


class ClaimBody(BaseModel):
    claimed_by: str | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pg = await connect_postgres()
    app.state.redis = connect_redis()
    yield
    await app.state.pg.close()
    await app.state.redis.aclose()


app = FastAPI(title="Equipment Telemetry Ingestion", lifespan=lifespan)


@app.exception_handler(RequestValidationError)
async def malformed_reading(request: Request, exc: RequestValidationError):
    """The wire contract is explicit: malformed readings get a 400, not a 422."""
    return JSONResponse(status_code=400, content={"detail": f"malformed reading: {exc}"})


def _stream_fields(r: ReadingIn) -> dict[str, str]:
    et = r.event_time if r.event_time.tzinfo else r.event_time.replace(tzinfo=UTC)
    return {
        "gateway_id": r.gateway_id,
        "device_id": r.device_id,
        "metric": r.metric,
        "value": str(r.value),
        "event_time": et.isoformat(),
        "sequence": str(r.sequence),
        # Receive time on the server's clock: the reference the worker uses to
        # judge whether event_time is implausibly far in the future.
        "received_at": datetime.now(UTC).isoformat(),
    }


@app.post("/ingest/readings", status_code=202)
async def ingest(reading: ReadingIn, request: Request):
    """Validate and enqueue. The queue delivers at-least-once; the database
    constraints make the effect exactly-once (see worker/apply.py)."""
    await request.app.state.redis.xadd(STREAM, _stream_fields(reading))
    return {"status": "accepted"}


@app.get("/healthz")
async def healthz(request: Request):
    try:
        await request.app.state.pg.fetchval("SELECT 1")
        await request.app.state.redis.ping()
    except Exception as exc:  # noqa: BLE001 - health must report any failure
        raise HTTPException(status_code=503, detail=f"dependencies unhealthy: {exc}") from exc
    return {"status": "ok"}


def _freshness(now: datetime, gateway_last_received: datetime | None,
               state_updated_at: datetime) -> dict:
    """Is this value live, and if not, why not?

    gateway_silent: the gateway that produced it hasn't been heard from for
      SILENCE_THRESHOLD; newer data may be in flight (network problem).
    device_quiet: the gateway is reporting, but this metric hasn't advanced
      (an equipment/sensor problem, itself a commissioning finding).
    """
    if gateway_last_received is None:
        return {"freshness": "gateway_silent", "silent_seconds": None}
    gateway_gap = (now - gateway_last_received).total_seconds()
    if gateway_gap > SILENCE_THRESHOLD_SECONDS:
        return {"freshness": "gateway_silent", "silent_seconds": round(gateway_gap)}
    metric_gap = (now - state_updated_at).total_seconds()
    if metric_gap > SILENCE_THRESHOLD_SECONDS:
        return {"freshness": "device_quiet", "silent_seconds": round(metric_gap)}
    return {"freshness": "live", "silent_seconds": None}


@app.get("/devices")
async def list_devices(request: Request):
    pg = request.app.state.pg
    now = datetime.now(UTC)
    devices = await pg.fetch(
        "SELECT device_id, status, first_gateway, first_seen_at, claimed_at, claimed_by"
        " FROM devices ORDER BY device_id"
    )
    state = await pg.fetch(
        "SELECT device_id, metric, value, event_time, sequence, gateway_id, updated_at"
        " FROM device_state"
    )
    last_received = {
        row["gateway_id"]: row["last_received_at"]
        for row in await pg.fetch("SELECT gateway_id, last_received_at FROM gateway_state")
    }
    by_device: dict[str, dict] = {}
    for row in state:
        by_device.setdefault(row["device_id"], {})[row["metric"]] = {
            "value": row["value"],
            "event_time": row["event_time"].isoformat(),
            "age_seconds": round((now - row["event_time"]).total_seconds(), 3),
            "gateway_id": row["gateway_id"],
            **_freshness(now, last_received.get(row["gateway_id"]), row["updated_at"]),
        }
    return {
        "devices": [
            {
                "device_id": d["device_id"],
                "status": d["status"],
                "first_gateway": d["first_gateway"],
                "first_seen_at": d["first_seen_at"].isoformat(),
                "claimed_at": d["claimed_at"].isoformat() if d["claimed_at"] else None,
                "claimed_by": d["claimed_by"],
                "metrics": by_device.get(d["device_id"], {}),
            }
            for d in devices
        ]
    }


@app.get("/devices/{device_id}")
async def device_detail(device_id: str, request: Request):
    pg = request.app.state.pg
    d = await pg.fetchrow(
        "SELECT device_id, status, first_gateway, first_seen_at, claimed_at, claimed_by"
        " FROM devices WHERE device_id = $1",
        device_id,
    )
    if d is None:
        raise HTTPException(status_code=404, detail="unknown device")
    metrics = await pg.fetch(
        "SELECT metric, value, event_time, sequence, gateway_id FROM device_state"
        " WHERE device_id = $1 ORDER BY metric",
        device_id,
    )
    history = await pg.fetch(
        "SELECT metric, value, event_time, sequence, gateway_id, ingested_at, quarantined"
        " FROM readings"
        " WHERE device_id = $1 ORDER BY event_time DESC LIMIT 50",
        device_id,
    )
    return {
        "device_id": d["device_id"],
        "status": d["status"],
        "first_gateway": d["first_gateway"],
        "first_seen_at": d["first_seen_at"].isoformat(),
        "claimed_at": d["claimed_at"].isoformat() if d["claimed_at"] else None,
        "claimed_by": d["claimed_by"],
        "metrics": [
            {
                "metric": m["metric"],
                "value": m["value"],
                "event_time": m["event_time"].isoformat(),
                "sequence": m["sequence"],
                "gateway_id": m["gateway_id"],
            }
            for m in metrics
        ],
        "recent_history": [
            {
                "metric": h["metric"],
                "value": h["value"],
                "event_time": h["event_time"].isoformat(),
                "sequence": h["sequence"],
                "gateway_id": h["gateway_id"],
                "quarantined": h["quarantined"],
            }
            for h in history
        ],
    }


@app.get("/outages")
async def outages(request: Request, limit: int = 20):
    """Most recent gateway outages: when each went silent, when it resumed."""
    rows = await request.app.state.pg.fetch(
        "SELECT gateway_id, silent_since, resumed_at FROM gateway_outages"
        " ORDER BY resumed_at DESC LIMIT $1",
        min(max(limit, 1), 200),
    )
    return {
        "silence_threshold_seconds": SILENCE_THRESHOLD_SECONDS,
        "outages": [
            {
                "gateway_id": r["gateway_id"],
                "silent_since": r["silent_since"].isoformat(),
                "resumed_at": r["resumed_at"].isoformat(),
                "duration_seconds": round((r["resumed_at"] - r["silent_since"]).total_seconds()),
            }
            for r in rows
        ],
    }


@app.post("/devices/{device_id}/claim")
async def claim_device(device_id: str, request: Request, body: ClaimBody | None = None):
    """Guarded transition: only an unclaimed device can be claimed, so a
    double-claim is impossible by construction."""
    claimed_by = (body.claimed_by if body else None) or "dashboard"
    row = await request.app.state.pg.fetchrow(
        "UPDATE devices SET status = 'claimed', claimed_at = now(), claimed_by = $2"
        " WHERE device_id = $1 AND status = 'unclaimed'"
        " RETURNING device_id, claimed_by, claimed_at",
        device_id,
        claimed_by,
    )
    if row is None:
        status = await request.app.state.pg.fetchval(
            "SELECT status FROM devices WHERE device_id = $1", device_id
        )
        if status is None:
            raise HTTPException(status_code=404, detail="unknown device")
        raise HTTPException(status_code=409, detail=f"device is already {status}")
    return {
        "device_id": row["device_id"],
        "status": "claimed",
        "claimed_by": row["claimed_by"],
        "claimed_at": row["claimed_at"].isoformat(),
    }


@app.get("/stats")
async def stats(request: Request):
    redis = request.app.state.redis
    stream_length = await redis.xlen(STREAM)
    try:
        pending = (await redis.xpending(STREAM, GROUP))["pending"]
    except aioredis.ResponseError:
        pending = 0  # consumer group not created yet — nothing pending
    counts = await request.app.state.pg.fetchrow(
        "SELECT (SELECT count(*) FROM devices) AS devices,"
        " (SELECT count(*) FROM readings) AS readings,"
        " (SELECT count(*) FROM late_readings) AS late_readings,"
        " (SELECT count(*) FROM reading_conflicts) AS reading_conflicts,"
        " (SELECT count(*) FROM readings WHERE quarantined) AS quarantined"
    )
    clock_ahead = [
        row["gateway_id"]
        for row in await request.app.state.pg.fetch(
            "SELECT gateway_id FROM gateway_state WHERE clock_ahead ORDER BY gateway_id"
        )
    ]
    return {
        "stream_length": stream_length,
        "pending_entries": pending,
        **dict(counts),
        "clock_ahead_gateways": clock_ahead,
    }


app.mount("/", StaticFiles(directory=DASHBOARD_DIR, html=True), name="dashboard")
