# Equipment Telemetry Ingestion

Queue-backed ingestion for building-commissioning gateways, with a live
dashboard. Gateways POST readings; the API validates and enqueues to a durable
Redis stream; two workers apply them to Postgres, where constraints enforce:

1. **No reading duplicated or lost**: resends collapse to one history row;
   anything odd (conflicting resends, unappliable entries) goes to a ledger,
   never silently dropped.
2. **Current state = true newest reading by event time**: a replayed backlog
   can never overwrite newer data; broken-clock readings are quarantined.
3. **A new device registers exactly once**, even when two gateways race.

Design and the late-data decision: [DESIGN.md](DESIGN.md) · what's left out:
[CUTS.md](CUTS.md) · AI disclosure: [AI_USAGE.md](AI_USAGE.md)

**Demo video:** _link added on submission_

## Run it

From a clean checkout (Docker with Compose v1 or v2):

    docker-compose up --build        # or: docker compose up --build

- Dashboard: http://localhost:8000/
- Health: http://localhost:8000/healthz

Starts Postgres, Redis (append-only, fsync on every write), the API, and two
workers. Schema migrations apply automatically on startup.

## Run the generator scenarios

The provided simulator uses only the Python standard library:

    python3 telemetry_generator.py --target http://localhost:8000 --scenario clean
    python3 telemetry_generator.py --target http://localhost:8000 --scenario replay
    python3 telemetry_generator.py --target http://localhost:8000 --scenario race
    python3 telemetry_generator.py --target http://localhost:8000 --scenario trace --seed drtclem

`trace` prints the personalized sequence first (traced step by step in DESIGN.md);
press enter, or add `--yes`, to send it.

## Watch a gateway go silent and come back

The `replay` scenario's gateway has no prior contact, so it never shows an
outage. This script does: a gateway drops off while another keeps reporting,
then reconnects and replays its backlog with a resend.

    SILENCE_THRESHOLD_SECONDS=10 docker-compose up --build
    python3 scripts/outage_demo.py --target http://localhost:8000 --silence 10

On the dashboard: the silent gateway's device is flagged "may be behind" while
the other stays live; after reconnect the flag clears, state shows the newest
reading, and the outage appears under **Gateway outages**.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `SILENCE_THRESHOLD_SECONDS` | 60 | No reading from a gateway for this long → its devices are flagged, and the gap is logged as an outage when it returns. Set to ~3× a site's reporting interval. |
| `FUTURE_TOLERANCE_SECONDS` | 300 | A reading this far ahead of its receive time is quarantined (broken clock). |
| `WORKER_MAX_ATTEMPTS` | 5 | Failed deliveries before an entry is dead-lettered. |

## API

| Endpoint | Purpose |
|---|---|
| `POST /ingest/readings` | `{gateway_id, device_id, metric, value, event_time, sequence}` → 202 queued, 400 malformed (incl. NaN/∞, oversized sequence) |
| `GET /devices` | all devices with current state per metric and its freshness (`live` / `gateway_silent` / `device_quiet`) |
| `GET /devices/{id}` | one device with recent history (quarantined readings flagged) |
| `POST /devices/{id}/claim` | claim an unregistered device (409 if already claimed) |
| `GET /outages` | recent gateway outages |
| `GET /stats` | counts: readings, devices, late, quarantined, conflicts, dead letters, queue pending |
| `GET /healthz` | Postgres + Redis reachable |

## Tests

Tests run against real Postgres and Redis (CI starts both as services):

    pip install -r requirements.txt
    docker-compose up -d postgres redis   # or local instances; see .env.example
    pytest -q

25 tests, targeting the invariants as properties rather than the named
scenarios: resend dedup, counter resets, conflicting resends, replay ordering
both ways, quarantine, concurrent registration, crash redelivery, dead-lettering
(including a simulated database outage), concurrent workers on one metric, a
forced worker interleaving, the outage log, and freshness causes.

## Layout

    api/         FastAPI: ingest, devices, claim, outages, stats, health; serves the dashboard
    worker/      Redis Streams consumer: idempotent apply, ack, recovery, dead letters
    dashboard/   static live view, vanilla JS, polls every 2 s
    migrations/  SQL schema, applied in order on startup
    scripts/     outage_demo.py
    tests/       invariant tests against real Postgres + Redis
