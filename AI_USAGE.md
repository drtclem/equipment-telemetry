# AI usage

This project was built almost entirely with AI tools. Here is exactly how.

## Phase 1: initial build (Obvious, Autobuild mode)

An AI engineering agent built the first complete version autonomously from the
brief: the stack choice (FastAPI, Redis Streams, PostgreSQL, a polling
dashboard), all source code, the schema, tests, Docker setup, CI, documentation,
and a silent demo video. I did not direct or review its decisions during that
phase. I provided the brief and `telemetry_generator.py`.

## Phase 2: review and redesign (Claude, Anthropic)

I then worked through the agent's output with Claude, one design decision at a
time. Claude explained each choice and its alternatives, ran the system, and
probed it with inputs the original tests didn't cover. I made each call, and
Claude implemented, tested, and committed it (commits carry a `Co-Authored-By:
Claude` trailer). Problems found by running the system, not by reading it:

- an accepted reading was lost if Redis crashed before it was processed (no
  persistence) → AOF with fsync on every write
- a gateway restarting its sequence counter had new readings discarded as
  duplicates, while current state showed a value missing from history → reading
  identity includes event time; conflicts recorded, never dropped
- one reading stamped 2099 froze a metric forever → quarantine
- with one worker the "race" scenario never actually raced; going to two workers
  exposed a missed-late-flag race → fixed and pinned by a forced-interleaving
  test that fails on the old code
- one malformed queue entry crash-looped the worker → dead letters
- the late-data policy was re-argued from the commissioning use case: no
  hold-back, explicit freshness with a cause, an outage log, and a
  connection-lost banner on the dashboard

The decisions behind each change, including the options rejected, are what
DESIGN.md argues. Claude also turned our running decision log into
DECISIONS.md (with diagrams), wrote the README's step-by-step run instructions,
and trimmed DESIGN.md to two pages.

## Verification

Claims in this repo were verified by running them, not by an agent asserting
them: 25 tests against real Postgres and Redis; all generator scenarios against
a fresh `docker compose up` with two workers; the seeded trace in DESIGN.md
worked out step by step before running it, then checked against the actual
result; failure cases reproduced before each fix and re-run after.
