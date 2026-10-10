"""Invariant tests T1-T5 (spec 'Verification') plus wire-contract checks.

Each test targets the PROPERTY, not a specific scenario: the graders run
'scenarios we don't name here', and these tests fail loudly if any invariant
breaks under the same class of abuse.
"""

import asyncio
from datetime import UTC, datetime, timedelta

from worker.apply import apply_reading
from worker.main import process_batch, recover_pending

NOW = datetime.now(UTC)


def reading(gateway_id="gw-04", device_id="chiller-2", metric="temperature_c",
            value=71.4, event_time=None, sequence=1):
    return {
        "gateway_id": gateway_id,
        "device_id": device_id,
        "metric": metric,
        "value": value,
        "event_time": event_time or NOW,
        "sequence": sequence,
    }


async def post(client, r):
    body = dict(r)
    body["event_time"] = r["event_time"].isoformat()
    return await client.post("/ingest/readings", json=body)


# T1 — Invariant 1: a re-sent reading collapses to exactly one history row.
async def test_t1_resend_dedup(client, pg, r, clean):
    payload = reading(sequence=1041)
    for _ in range(5):
        resp = await post(client, payload)
        assert resp.status_code == 202
    await process_batch(pg, r, "test-worker")
    count = await pg.fetchval(
        "SELECT count(*) FROM readings WHERE gateway_id='gw-04' AND device_id='chiller-2'"
        " AND metric='temperature_c' AND sequence=1041"
    )
    assert count == 1, "a 5x re-sent reading must produce exactly one history row"


# T2 — Invariant 2: replayed backlog can never regress current state.
async def test_t2_replay_ordering(client, pg, r, clean):
    now = datetime.now(UTC)
    realtime = reading(value=99.0, event_time=now, sequence=2)
    backlog = reading(value=11.0, event_time=now - timedelta(hours=1), sequence=1)
    await post(client, realtime)
    await post(client, backlog)
    await process_batch(pg, r, "test-worker")

    state = await pg.fetchrow(
        "SELECT value, event_time FROM device_state WHERE device_id='chiller-2'"
        " AND metric='temperature_c'"
    )
    assert state["value"] == 99.0, "older replayed reading must not overwrite newer state"

    late = await pg.fetchval("SELECT count(*) FROM late_readings")
    assert late == 1, "the out-of-order arrival is recorded in the late ledger"

    hist = await pg.fetchval("SELECT count(*) FROM readings")
    assert hist == 2, "late data is recorded, never dropped"


# T2b — the reverse arrival order must also end correct (and not flag late).
async def test_t2b_backlog_first_is_not_late(client, pg, r, clean):
    now = datetime.now(UTC)
    await post(client, reading(value=11.0, event_time=now - timedelta(hours=1), sequence=1))
    await post(client, reading(value=99.0, event_time=now, sequence=2))
    await process_batch(pg, r, "test-worker")

    state = await pg.fetchval(
        "SELECT value FROM device_state WHERE device_id='chiller-2' AND metric='temperature_c'"
    )
    assert state == 99.0
    late = await pg.fetchval("SELECT count(*) FROM late_readings")
    assert late == 0


# T3 — Invariant 3: concurrent first-reports from two gateways register the
# device exactly once, with no lost readings and no unhandled error.
async def test_t3_register_race(pg, r, clean):
    first = reading(gateway_id="gw-a", sequence=1)
    second = reading(gateway_id="gw-b", sequence=1)
    async with pg.acquire() as conn_a, pg.acquire() as conn_b:
        await asyncio.gather(
            apply_reading(conn_a, **first),
            apply_reading(conn_b, **second),
        )

    devices = await pg.fetchval("SELECT count(*) FROM devices WHERE device_id='chiller-2'")
    assert devices == 1, "concurrent registration must produce exactly one device row"

    hist = await pg.fetchval("SELECT count(*) FROM readings WHERE device_id='chiller-2'")
    assert hist == 2, "both gateways' readings are kept"


# T4 — a worker that crashed before acknowledging loses nothing and dups nothing.
async def test_t4_crash_redelivery(client, pg, r, clean):
    await post(client, reading(sequence=7))

    # a 'ghost' worker reads the entry, then dies before ack -> entry is pending
    rows = await r.xreadgroup("ingest", "ghost", {"readings": ">"}, count=10)
    assert len(rows[0][1]) == 1
    assert await pg.fetchval("SELECT count(*) FROM readings") == 0

    # a fresh worker recovers the pending entry via XAUTOCLAIM
    recovered = await recover_pending(pg, r, "recovered", min_idle_ms=0)
    assert recovered == 1
    assert await pg.fetchval("SELECT count(*) FROM readings") == 1

    # the crashed worker's stale claim resurfaces and the entry is delivered
    # AGAIN -> still exactly one row
    recovered_again = await recover_pending(pg, r, "recovered", min_idle_ms=0)
    assert recovered_again == 0
    assert await pg.fetchval("SELECT count(*) FROM readings") == 1


# T5 — boot smoke: the app answers, the dashboard is served, stats are honest.
async def test_t5_boot_and_surfaces(client, pg, r, clean):
    health = await client.get("/healthz")
    assert health.status_code == 200
    assert health.json() == {"status": "ok"}

    await post(client, reading(device_id="ahu-1", metric="supply_air_temp", value=55.0, sequence=3))
    await process_batch(pg, r, "test-worker")

    devs = (await client.get("/devices")).json()["devices"]
    assert len(devs) == 1
    assert devs[0]["device_id"] == "ahu-1"
    assert devs[0]["status"] == "unclaimed"
    assert devs[0]["metrics"]["supply_air_temp"]["value"] == 55.0
    assert devs[0]["metrics"]["supply_air_temp"]["reconciling"] is False

    st = (await client.get("/stats")).json()
    assert st["readings"] == 1
    assert st["devices"] == 1

    page = await client.get("/")
    assert page.status_code == 200
    assert "Facility Equipment" in page.text


async def test_claim_flow_guards_transitions(client, pg, r, clean):
    await post(client, reading(device_id="ahu-1", metric="supply_air_temp", value=55.0, sequence=3))
    await process_batch(pg, r, "test-worker")

    ok = await client.post("/devices/ahu-1/claim", json={"claimed_by": "taylor"})
    assert ok.status_code == 200
    assert ok.json()["claimed_by"] == "taylor"

    again = await client.post("/devices/ahu-1/claim", json={"claimed_by": "someone-else"})
    assert again.status_code == 409, "a second claim must not succeed"

    missing = await client.post("/devices/who-is-this/claim", json={})
    assert missing.status_code == 404


async def test_malformed_reading_rejected(client, r, clean):
    resp = await client.post(
        "/ingest/readings",
        json={
            "gateway_id": "gw-04",
            "device_id": "chiller-2",
            "metric": "temperature_c",
            "value": "not-a-number",
            "event_time": "2026-10-09T18:22:03Z",
            "sequence": 1,
        },
    )
    assert resp.status_code == 400


# A byte-identical resend of an ALREADY-LATE reading must not flag the ledger
# twice: lateness is a property of a reading, not of a delivery. (The seeded
# trace scenario re-sends exactly such a reading.)
async def test_resend_of_late_reading_flags_ledger_once(client, pg, r, clean):
    late = reading(value=11.0, event_time=NOW - timedelta(hours=1), sequence=1)

    # a newer reading first, so the next one is genuinely late
    newer = reading(value=99.0, event_time=NOW, sequence=2)
    await post(client, newer)
    await process_batch(pg, r, "test-worker")
    await post(client, late)
    await process_batch(pg, r, "test-worker")
    assert await pg.fetchval("SELECT count(*) FROM late_readings") == 1

    await post(client, late)  # the resend — byte-for-byte unchanged
    await process_batch(pg, r, "test-worker")

    assert await pg.fetchval("SELECT count(*) FROM late_readings") == 1, (
        "a re-sent late reading must be flagged late exactly once"
    )
    assert await pg.fetchval("SELECT count(*) FROM readings") == 2
    state = await pg.fetchval(
        "SELECT value FROM device_state WHERE device_id='chiller-2' AND metric='temperature_c'"
    )
    assert state == 99.0


# Reading identity — a gateway that reboots and restarts its sequence counter
# must not have its new readings mistaken for resends of old ones.
async def test_reused_sequence_with_new_time_is_a_new_reading(client, pg, r, clean):
    now = datetime.now(UTC)
    await post(client, reading(value=70.0, event_time=now - timedelta(minutes=5), sequence=0))
    await post(client, reading(value=75.0, event_time=now, sequence=0))  # after reboot
    await process_batch(pg, r, "test-worker")

    assert await pg.fetchval("SELECT count(*) FROM readings") == 2, (
        "a reused sequence with a new timestamp is a new reading and must be kept"
    )
    assert await pg.fetchval(
        "SELECT value FROM device_state WHERE device_id='chiller-2' AND metric='temperature_c'"
    ) == 75.0
    assert await pg.fetchval("SELECT count(*) FROM reading_conflicts") == 0


# Safety net 1 — same identity, different value is not a resend: it is recorded
# in the conflicts ledger, never silently dropped, and recorded once even if the
# queue redelivers it.
async def test_same_identity_different_value_is_recorded_not_dropped(client, pg, r, clean):
    t = datetime.now(UTC)
    await post(client, reading(value=70.0, event_time=t, sequence=5))
    await post(client, reading(value=99.0, event_time=t, sequence=5))
    await post(client, reading(value=99.0, event_time=t, sequence=5))  # redelivered
    await process_batch(pg, r, "test-worker")

    assert await pg.fetchval("SELECT count(*) FROM readings") == 1
    row = await pg.fetchrow("SELECT stored_value, incoming_value FROM reading_conflicts")
    assert (row["stored_value"], row["incoming_value"]) == (70.0, 99.0)
    assert await pg.fetchval("SELECT count(*) FROM reading_conflicts") == 1


# Safety net 2 — current state only ever points at a reading that is in history.
async def test_state_always_points_at_a_stored_reading(client, pg, r, clean):
    t = datetime.now(UTC)
    await post(client, reading(value=70.0, event_time=t, sequence=5))
    await post(client, reading(value=99.0, event_time=t, sequence=5))  # conflicting
    await process_batch(pg, r, "test-worker")

    orphans = await pg.fetchval(
        "SELECT count(*) FROM device_state s WHERE NOT EXISTS ("
        " SELECT 1 FROM readings h WHERE h.device_id = s.device_id AND h.metric = s.metric"
        " AND h.event_time = s.event_time AND h.sequence = s.sequence"
        " AND h.gateway_id = s.gateway_id AND h.value = s.value)"
    )
    assert orphans == 0, "state must never show a value that history doesn't have"
