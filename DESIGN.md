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
clears after a quiet period with no new late arrivals. The brief's late
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

## Seeded trace

The generator's trace scenario (run with `--seed drtclem` once the provided
script is available — the build environment cannot reach the private
reference) replays a randomized-yet-deterministic session of re-sends,
backlog floods, and a registration race. The dashboard during that run:
late-arrival counter climbing while a "reconciling — backlog may be in
flight" badge shows on the affected device, state values never regressing,
and the session ends with the same device count the scenario began with.
This section is filled from the actual run in the demo video.
