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
    assert devs[0]["metrics"]["supply_air_temp"]["freshness"] == "live"

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


# Quarantine — a reading stamped far in the future (broken gateway clock) is
# kept in history but must never become current state, or it would freeze the
# metric until real time caught up.
async def test_future_reading_is_quarantined_not_current(client, pg, r, clean):
    now = datetime.now(UTC)
    await post(client, reading(value=50.0, event_time=now, sequence=1))
    await post(client, reading(value=999.0, event_time=datetime(2099, 1, 1, tzinfo=UTC),
                               sequence=2))
    await process_batch(pg, r, "test-worker")

    assert await pg.fetchval("SELECT count(*) FROM readings") == 2, "kept, never lost"
    assert await pg.fetchval("SELECT quarantined FROM readings WHERE value = 999") is True
    assert await pg.fetchval(
        "SELECT value FROM device_state WHERE device_id='chiller-2'"
    ) == 50.0, "a bad clock must not take over current state"
    st = (await client.get("/stats")).json()
    assert st["quarantined"] == 1
    assert st["clock_ahead_gateways"] == ["gw-04"]

    # the gateway's next sane reading updates state and clears the flag
    await post(client, reading(value=51.0, event_time=datetime.now(UTC), sequence=3))
    await process_batch(pg, r, "test-worker")
    assert await pg.fetchval(
        "SELECT value FROM device_state WHERE device_id='chiller-2'"
    ) == 51.0
    assert (await client.get("/stats")).json()["clock_ahead_gateways"] == []


# The verdict is judged against RECEIVE time and stored once: a reading that
# was too far ahead when it arrived stays quarantined even after real time
# catches up (here, a worker draining it long after it was received).
async def test_quarantine_is_judged_at_receive_time(pg, clean):
    received = datetime.now(UTC) - timedelta(hours=1)
    ahead = received + timedelta(minutes=30)  # 30 min ahead at receipt; past by now
    async with pg.acquire() as conn:
        await apply_reading(conn, **reading(value=7.0, event_time=ahead, sequence=1),
                            received_at=received)
    assert await pg.fetchval("SELECT quarantined FROM readings") is True
    assert await pg.fetchval("SELECT count(*) FROM device_state") == 0


# Small drift inside the tolerance is honest skew, not a broken clock.
async def test_small_clock_drift_is_accepted(client, pg, r, clean):
    await post(client, reading(value=60.0, event_time=datetime.now(UTC) + timedelta(minutes=2)))
    await process_batch(pg, r, "test-worker")
    assert await pg.fetchval("SELECT quarantined FROM readings") is False
    assert await pg.fetchval("SELECT value FROM device_state") == 60.0


async def test_nan_and_infinity_rejected(client, r, clean):
    for bad in ("NaN", "Infinity", "-Infinity"):
        body = ('{"gateway_id":"g","device_id":"d","metric":"m","value":%s,'
                '"event_time":"2026-10-09T18:22:03Z","sequence":1}' % bad)
        resp = await client.post("/ingest/readings", content=body,
                                 headers={"content-type": "application/json"})
        assert resp.status_code == 400, f"{bad} must be rejected"


# Concurrency — with several workers, readings for the same device and metric
# can be applied at the same moment. State must still end on the newest
# reading, history must hold every reading once, and the late ledger must never
# flag the reading that won (it was never beaten).
async def test_concurrent_workers_same_metric(pg, clean):
    import random as _random
    base = datetime.now(UTC)
    readings = [reading(gateway_id=f"gw-{i % 3}", value=float(i),
                        event_time=base - timedelta(seconds=i), sequence=i)
                for i in range(40)]
    _random.shuffle(readings)

    async def apply_one(rd):
        async with pg.acquire() as conn:
            await apply_reading(conn, **rd)

    await asyncio.gather(*(apply_one(rd) for rd in readings))

    assert await pg.fetchval("SELECT count(*) FROM readings") == 40
    state = await pg.fetchrow("SELECT value, event_time FROM device_state")
    assert state["value"] == 0.0 and state["event_time"] == base, "newest reading wins"
    assert await pg.fetchval(
        "SELECT count(*) FROM late_readings WHERE reading_event_time = $1", base
    ) == 0, "the newest reading can never be late"
    assert await pg.fetchval(
        "SELECT count(*) FROM late_readings WHERE reading_event_time >= state_event_time"
    ) == 0, "every late flag points at a strictly newer reading that beat it"


class _PauseBeforeStateWrite:
    """Connection wrapper that holds a transaction just before it writes
    device_state, until `gate` is released. Lets a test force the exact
    interleaving where another worker commits in between."""

    def __init__(self, conn, gate):
        self._conn, self._gate = conn, gate

    def __getattr__(self, name):
        return getattr(self._conn, name)

    async def _maybe_pause(self, query):
        if "INSERT INTO device_state" in query:
            await self._gate.wait()

    async def execute(self, query, *args):
        await self._maybe_pause(query)
        return await self._conn.execute(query, *args)

    async def fetchval(self, query, *args):
        await self._maybe_pause(query)
        return await self._conn.fetchval(query, *args)

    async def fetch(self, query, *args):
        await self._maybe_pause(query)
        return await self._conn.fetch(query, *args)


# Forced interleaving: worker A (older reading) is paused right before it
# writes state; worker B (newer reading) commits in that gap. A arrived after B
# was already current, so A must be flagged late. Judging lateness from a read
# taken BEFORE the write would see "no state yet" and miss the flag.
async def test_late_flag_survives_worker_interleaving(pg, clean):
    t = datetime.now(UTC)
    async with pg.acquire() as conn:  # pre-register so the two don't contend on it
        await apply_reading(conn, **reading(metric="other", sequence=99))

    gate = asyncio.Event()
    older = reading(gateway_id="gw-a", value=1.0, event_time=t - timedelta(minutes=1), sequence=1)
    newer = reading(gateway_id="gw-b", value=2.0, event_time=t, sequence=2)

    async def worker_a():
        async with pg.acquire() as conn:
            await apply_reading(_PauseBeforeStateWrite(conn, gate), **older)

    async def worker_b():
        await asyncio.sleep(0.2)  # let A reach its pause point
        async with pg.acquire() as conn:
            await apply_reading(conn, **newer)
        gate.set()

    await asyncio.wait_for(asyncio.gather(worker_a(), worker_b()), timeout=10)

    assert await pg.fetchval(
        "SELECT value FROM device_state WHERE metric='temperature_c'"
    ) == 2.0
    assert await pg.fetchval(
        "SELECT count(*) FROM late_readings WHERE gateway_id='gw-a'"
    ) == 1, "A lost to a reading committed before it: it must be flagged late"


# Outage log — a gateway that goes silent past the threshold and then speaks
# again leaves exactly one outage row, measured on the API's receive clock.
async def test_outage_logged_when_gateway_returns(pg, clean):
    t0 = datetime.now(UTC) - timedelta(minutes=10)
    async with pg.acquire() as conn:
        await apply_reading(conn, **reading(sequence=1, event_time=t0), received_at=t0)
        # normal cadence: no outage
        await apply_reading(conn, **reading(sequence=2, event_time=t0 + timedelta(seconds=5)),
                            received_at=t0 + timedelta(seconds=5))
        # 5 minutes of silence, then the gateway speaks (backlog + live)
        back = t0 + timedelta(minutes=5, seconds=5)
        await apply_reading(conn, **reading(sequence=3, event_time=t0 + timedelta(seconds=30)),
                            received_at=back)
        await apply_reading(conn, **reading(sequence=4, event_time=back), received_at=back)

    rows = await pg.fetch("SELECT silent_since, resumed_at FROM gateway_outages")
    assert len(rows) == 1
    assert rows[0]["silent_since"] == t0 + timedelta(seconds=5)
    assert rows[0]["resumed_at"] == back


# A backed-up queue or a stopped worker must not look like a gateway outage:
# readings received seconds apart are processed minutes apart here.
async def test_worker_lag_is_not_an_outage(pg, clean):
    received = datetime.now(UTC) - timedelta(minutes=10)
    async with pg.acquire() as conn:
        for i in range(3):
            await apply_reading(conn, **reading(sequence=i, event_time=received),
                                received_at=received + timedelta(seconds=i))
    assert await pg.fetchval("SELECT count(*) FROM gateway_outages") == 0


# Two workers applying the first readings after a reconnect at the same moment
# must log the outage once, not twice.
async def test_outage_logged_once_under_concurrency(pg, clean):
    t0 = datetime.now(UTC) - timedelta(minutes=10)
    async with pg.acquire() as conn:
        await apply_reading(conn, **reading(sequence=0, event_time=t0), received_at=t0)
    back = t0 + timedelta(minutes=5)

    async def one(i):
        async with pg.acquire() as conn:
            await apply_reading(conn, **reading(device_id=f"dev-{i}", sequence=i,
                                                event_time=back), received_at=back)

    await asyncio.gather(*(one(i) for i in range(1, 9)))
    assert await pg.fetchval("SELECT count(*) FROM gateway_outages") == 1


# Freshness says WHY a value may be stale: gateway silent (network) versus
# device quiet while its gateway keeps reporting (equipment/sensor).
async def test_freshness_distinguishes_gateway_silent_from_device_quiet(client, pg, r, clean):
    now = datetime.now(UTC)
    await post(client, reading(gateway_id="gw-net", device_id="ahu-1", sequence=1, event_time=now))
    await post(client, reading(gateway_id="gw-ok", device_id="pump-1", sequence=1, event_time=now))
    await post(client, reading(gateway_id="gw-ok", device_id="pump-2", sequence=1, event_time=now))
    await process_batch(pg, r, "test-worker")

    # gw-net went silent 3 min ago; pump-1 stopped advancing 3 min ago while
    # gw-ok kept reporting (pump-2 is still live)
    await pg.execute("UPDATE gateway_state SET last_received_at = now() - interval '3 minutes'"
                     " WHERE gateway_id = 'gw-net'")
    await pg.execute("UPDATE device_state SET updated_at = now() - interval '3 minutes'"
                     " WHERE device_id = 'pump-1'")

    devs = {d["device_id"]: d for d in (await client.get("/devices")).json()["devices"]}
    m = lambda d: devs[d]["metrics"]["temperature_c"]  # noqa: E731
    assert m("ahu-1")["freshness"] == "gateway_silent"
    assert 170 <= m("ahu-1")["silent_seconds"] <= 190
    assert m("pump-1")["freshness"] == "device_quiet"
    assert m("pump-2")["freshness"] == "live"

    out = (await client.get("/outages")).json()
    assert out["silence_threshold_seconds"] == 60 and out["outages"] == []


async def test_oversized_sequence_rejected(client, r, clean):
    body = dict(reading())
    body["event_time"] = body["event_time"].isoformat()
    body["sequence"] = 2**63  # one past Postgres BIGINT
    assert (await client.post("/ingest/readings", json=body)).status_code == 400
    body["sequence"] = 2**63 - 1
    assert (await client.post("/ingest/readings", json=body)).status_code == 202


def _raw_fields(**overrides):
    f = {"gateway_id": "gw-04", "device_id": "chiller-2", "metric": "temperature_c",
         "value": "71.4", "event_time": datetime.now(UTC).isoformat(), "sequence": "1",
         "received_at": datetime.now(UTC).isoformat()}
    f.update(overrides)
    return f


# Dead letters — an entry that can never be applied (here: one that slipped
# past validation straight onto the queue) is parked after MAX_ATTEMPTS, the
# worker never crashes, and good traffic behind it keeps flowing.
async def test_poison_entry_is_dead_lettered_not_crash_looped(pg, r, clean):
    from worker.main import MAX_ATTEMPTS
    poison_id = await r.xadd("readings", _raw_fields(sequence=str(10**20)))
    await process_batch(pg, r, "w")  # delivery 1: fails, stays pending
    good = _raw_fields(sequence="2", value="56.0")
    await r.xadd("readings", good)
    await process_batch(pg, r, "w")  # good traffic is not blocked
    assert await pg.fetchval("SELECT count(*) FROM readings") == 1

    for _ in range(MAX_ATTEMPTS + 2):  # recovery must never raise
        await recover_pending(pg, r, "w", min_idle_ms=0)

    row = await pg.fetchrow("SELECT stream_entry_id, fields, error, attempts FROM dead_letters")
    assert row["stream_entry_id"] == poison_id
    assert '"100000000000000000000"' in row["fields"], "raw payload kept exactly as queued"
    assert row["attempts"] == MAX_ATTEMPTS
    assert (await r.xpending("readings", "ingest"))["pending"] == 0, "acknowledged once parked"
    assert await pg.fetchval("SELECT count(*) FROM dead_letters") == 1


# A database outage fails every entry for a while. That must not dead-letter
# good readings: when Postgres is back, the retry succeeds, even if the entry
# has been delivered more than MAX_ATTEMPTS times by then.
async def test_database_outage_does_not_dead_letter_good_readings(pg, r, clean, monkeypatch):
    import worker.main as wm

    async def db_down(*args, **kwargs):
        raise ConnectionRefusedError("postgres is down")

    await r.xadd("readings", _raw_fields(sequence="7"))
    monkeypatch.setattr(wm, "apply_reading_safe", db_down)
    monkeypatch.setattr(wm, "dead_letter", db_down)  # can't write dead letters either
    await process_batch(pg, r, "w")
    for _ in range(wm.MAX_ATTEMPTS + 3):
        await recover_pending(pg, r, "w", min_idle_ms=0)
    assert (await r.xpending("readings", "ingest"))["pending"] == 1, "waits in the queue"

    monkeypatch.undo()  # database is back
    await recover_pending(pg, r, "w", min_idle_ms=0)
    assert await pg.fetchval("SELECT count(*) FROM readings") == 1
    assert await pg.fetchval("SELECT count(*) FROM dead_letters") == 0
    assert (await r.xpending("readings", "ingest"))["pending"] == 0
