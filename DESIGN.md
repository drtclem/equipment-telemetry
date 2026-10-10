# Design — Equipment Telemetry Ingestion

## The shape

Gateways POST readings to one service; the service validates and *only*
enqueues; a worker applies them to Postgres; a dashboard reads state.

```
gateway ──POST /ingest/readings──▶ FastAPI ──XADD──▶ Redis Stream "readings"
                                                          │ consumer group "ingest"
                            ┌─────────────────────────────┘
                            ▼
                  worker (XREADGROUP → apply in one tx → XACK)
                            │                ▲ re-entry via XAUTOCLAIM
                            ▼                │ (crashed worker's pending entries)
                        PostgreSQL ◀─────────┘
                            ▲
                     GET /devices · /stats          dashboard (polls, 2 s)
```

The queue exists because gateways WILL re-send and connections WILL drop. The
decision that matters: **the queue delivers at-least-once, and the database
makes the effect exactly-once.** The API never touches Postgres on the ingest
path, so the slowest thing a gateway can observe is Redis.

## How each invariant is enforced

**1. No duplicated or lost readings.** The queue can deliver an entry twice
(worker crash before `XACK`); Postgres makes that harmless:
`UNIQUE (gateway_id, device_id, metric, sequence)` collapses re-sends to one
history row. Loss cannot happen through the queue path: an entry that fails to
apply stays unacknowledged and is re-claimed by the next worker's
`XAUTOCLAIM`. Worker identity doesn't matter because *any* consumer can
recover *any* pending entry.

**2. Current state is the true newest reading.** `device_state` is written by
a single guarded upsert:

```sql
INSERT INTO device_state AS ds (...)
VALUES ($1, ..., $event_time, $sequence, ...)
ON CONFLICT (device_id, metric) DO UPDATE SET value = EXCLUDED.value, ...
WHERE (EXCLUDED.event_time, EXCLUDED.sequence) > (ds.event_time, ds.sequence)
```

A replayed backlog — every reading an hour old, arriving after today's
readings — fails that `WHERE` for each stale row and cannot regress state,
whatever order the entries come in. Ties are broken by the gateway sequence:
deterministic, and stable when event times repeat.

**3. Registration happens exactly once.** `devices.device_id` is the primary
key; first-report is `INSERT ... ON CONFLICT DO NOTHING`. Two gateways
first-reporting the same device in the same millisecond serialize on the key:
one insert wins, the other is a no-op, and both gateways' readings still land
in history (they dedupe independently). "First gateway" is whoever won the
race — honest, and as stable as a race winner can be.

## Late data: the call

The tempting design is a hold-back window: keep each device's state "open"
for N minutes and resolve it once the window closes. That buys zero late
readings — at the cost of a live dashboard that is always N minutes behind,
an idle-timer state machine, and an answer I'd still owe for readings that
miss the window anyway.

I chose **immediate commits**: state is the event-time maximum the instant a
reading arrives, and lateness is an *observation* layered on top, not a
filter. A reading that arrives after a newer one was stored still goes into
history (nothing is dropped), into a `late_readings` ledger with the state
time it lost to, and flags its gateway as "reconciling" so the dashboard shows
a badge instead of silently pretending the world is monotonic. The badge
clears when the flagged gateway's next reading arrives after a full quiet
period with no new late detection — a gateway that has gone silent may still
have backlog in flight, so its badge waits for it to speak again. The brief's late
requirement is about *delivered* readings — holding them back to filter
lateness is a design choice, and I think the wrong one; recording and
surfacing is both more honest and strictly more available.

## What breaks first if a gateway goes offline for an hour

The quiet-period heuristic degrades first: the dashboard shows stale
state ("2h ago") with no explicit "gateway is down" verdict — replay draining
and true offline are hard to tell apart from the data alone. Everything else
holds: the stream absorbs the outage (bounded by memory; see cuts), the
worker drains it idempotently when the gateway returns, and any readings
mid-hour, state never regresses.

## At 10× gateways

The consumer group is the seam: workers scale horizontally out of the box
(each consumer gets distinct entries; `XAUTOCLAIM` recovers dead ones). The
next bottleneck is per-device apply contention; sharding readings by
`device_id` across partitions — device keys are natural shard boundaries —
plus a bounded stream (`XTRIM MAXLEN`) and Postgres table partitioning by
`event_time` are the first three moves. None of them change the invariants;
they're all below the constraint layer.

## Seeded trace (`--scenario trace --seed drtclem`)

The generator prints this sequence for our seed — device `trace-device-6462`,
gateways `gw-a-drtclem` / `gw-b-drtclem`, all event times relative to a base
moment T. Arrival order is the row order; the last line is a byte-identical
resend of event #2:

| #  | gateway      | metric      | value | event time |
|----|--------------|-------------|-------|------------|
| 0  | gw-a-drtclem | vibration   | 91.38 | T + 5s     |
| 1  | gw-a-drtclem | temperature | 98.03 | T + 2s     |
| 2  | gw-b-drtclem | temperature | 43.27 | T + 1s     |
| 3  | gw-b-drtclem | vibration   | 77.4  | T + 4s     |
| 4  | gw-a-drtclem | status      | 11.98 | T + 0s     |
| 5  | gw-a-drtclem | vibration   | 89.76 | T + 3s     |
| 2′ | gw-b-drtclem | temperature | 43.27 | T + 1s     |

Tracing each arrival through the worker (store → guard state → late check):

- **#0** — first report: device registers (first gateway `gw-a-drtclem`),
  history row stored, `vibration = 91.38 @ T+5`. No prior state → not late.
- **#1** — new metric: history stored, `temperature = 98.03 @ T+2`. Not late.
- **#2** — new history row; `temperature` state is at T+2 and T+1 < T+2 →
  **late**. Ledger row: reading 43.27 @ T+1, current state 98.03 @ T+2,
  gateway `gw-b-drtclem` flagged reconciling. Guard rejects: `(T+1, 2) >
  (T+2, 1)` is false → state stays **98.03**.
- **#3** — new row; `vibration` is at T+5 and T+4 < T+5 → **late** (ledger
  row 2, `gw-b-drtclem`). State stays **91.38**.
- **#4** — new metric: `status = 11.98 @ T+0`. Not late.
- **#5** — new row; T+3 < T+5 → **late** (ledger row 3, `gw-a-drtclem` now
  also reconciling). State stays **91.38**.
- **#2′** — byte-identical resend: the unique constraint dedupes history, and
  the late check is gated on the insert actually storing a row, so this is a
  **pure no-op** — no duplicate history row, no second late flag.

**Predicted end state:** 6 history rows from 7 deliveries; current state
`vibration 91.38 @ T+5` · `temperature 98.03 @ T+2` · `status 11.98 @ T+0`
(the event-time maximum of each metric, i.e. the first arrivals win here);
late ledger 3 rows (#2, #3, #5); both gateways reconciling, badges clearing
when each gateway's next reading lands after 60 quiet seconds.

**Verification (actual run):** 6 history rows · late ledger 3 · device
`trace-device-6462` registered once, first gateway `gw-a-drtclem` · state
vibration 91.38, temperature 98.03, status 11.98 · reconciling badges shown
for both gateways — matches the trace on every point.

