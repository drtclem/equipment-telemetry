# AI usage disclosure

This project was built with an AI engineering agent (Obvious, Autobuild mode)
at the direction of Taylor Clements.

**AI-built:** all source code (`api/`, `worker/`, `dashboard/`, `db.py`,
`migrations/`), the test suite, `Dockerfile`/`docker-compose.yml`, the CI
workflow, and this documentation set. The Blueprint spec (architecture,
invariant enforcement strategy, the late-data trade-off, verification plan)
was also drafted by the agent and approved by Taylor before implementation.

**Human decisions:**
- the overall approach and stack (FastAPI + Redis Streams + PostgreSQL +
  lightweight polling dashboard), approved in the build plan;
- the late-data policy (immediate event-time maximum with reconciliation
  indicators, rather than a hold-back window) — reviewed and approved as
  part of the plan;
- the deliverable target (GitHub repository, demo video, scenario runs).

**Human-provided inputs:** the take-home brief, the design reference link,
and (pending) the `telemetry_generator.py` script — the build environment
cannot access the private reference, so generator-scenario runs and the
personalized `--seed drtclem` trace validation are performed once that script
is provided.

**Verification:** every claim in this repo about correctness is backed by a
test in `tests/test_invariants.py` (8 tests, real Postgres + Redis) and a
GitHub Actions workflow running them on every push. No scenario results are
claimed that were not run.
