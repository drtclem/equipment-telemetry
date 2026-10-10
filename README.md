# Equipment Telemetry Ingestion

Queue-backed ingestion for building-commissioning gateways. Gateways (and the
provided `telemetry_generator.py`) POST readings to the API; a Redis Streams
consumer applies them to Postgres under three hard invariants:

1. **No reading is duplicated or lost.** Re-sent readings collapse to exactly
   one history row via `UNIQUE (gateway_id, device_id, metric, sequence)`.
   The queue is at-least-once; the database is exactly-once.
2. **Current state is the true newest reading by event time.** A guarded
   upsert overwrites only on strictly newer `(event_time, sequence)`, so a
   replayed backlog can never clobber newer data. Late arrivals are recorded
   in a `late_readings` ledger and surfaced as dashboard badges — never
   dropped, never blocking.
3. **A device registers exactly once**, even when two gateways first-report it
   concurrently — the primary key arbitrates.

## Demo video

**[docs/demo-video.mp4](docs/demo-video.mp4)** — a 5-minute walkthrough:
clean compose boot from a fresh checkout, all four generator scenarios run
live (clean / replay / registration race with a dashboard claim / the seeded
`drtclem` trace), the late-arrival badge lifecycle, the raw API contract
(claim idempotency, validation rejection), and the hand-trace in DESIGN.md.

## Run it

    docker compose up --build
    # dashboard  ->  http://localhost:8000/
    # health     ->  http://localhost:8000/healthz

Feed a reading:

    curl -X POST http://localhost:8000/ingest/readings \
      -H 'content-type: application/json' \
      -d '{"gateway_id":"gw-04","device_id":"chiller-2","metric":"temperature_c","value":71.4,"event_time":"2026-10-09T18:22:03Z","sequence":1041}'

## Generator scenarios (provided script)

    docker compose up -d postgres redis api worker
    python -m pip install requests   # only if requests is not already present
    python telemetry_generator.py --target http://localhost:8000 --scenario clean
    python telemetry_generator.py --target http://localhost:8000 --scenario replay
    python telemetry_generator.py --target http://localhost:8000 --scenario race
    python telemetry_generator.py --target http://localhost:8000 --scenario trace --seed drtclem
    #   ^ prints the personalized sequence first (traced in DESIGN.md);
    #     press enter (or add --yes) to send it

`replay` is the one to watch for correctness: one gateway replays a 20-reading
backlog with byte-identical resends while another streams live data. After it,
the device's history has every reading exactly once and its current state is
not stuck on stale data.

## Tests

    python -m pip install -r requirements.txt
    pytest -q    # needs Postgres + Redis (see .env.example); CI runs both as services

The suite (tests/test_invariants.py) targets the invariants as properties:
re-send deduplication, replay-vs-realtime ordering in both arrival orders,
concurrent registration, crash-redelivery recovery, and a boot smoke test
covering the API surfaces and the dashboard page.

## Layout

    api/         FastAPI service — ingest, devices, claim, stats, health + dashboard host
    worker/      Redis Streams consumer — idempotent apply, XACK, XAUTOCLAIM recovery
    dashboard/   static live view — vanilla JS, polls every 2 s
    migrations/  SQL schema — each invariant enforced by a constraint
    tests/       invariant tests against real Postgres + Redis

More docs: DESIGN.md (design and the late-data call), CUTS.md (what was cut
and why), AI_USAGE.md (what was AI-built and what was human).
