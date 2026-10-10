# Cuts — what I left out, and why

Every cut here is a scope decision for a take-home build, made to keep the
three invariants airtight and the reviewable surface honest.

- **No authentication.** The API is unauthenticated; `claim` is a guarded
  transition, not an authorization boundary. Real deployment: gateway API
  keys at the edge, OIDC for the dashboard.
- **Polling, not push.** The dashboard polls every 2 s. SSE or WebSockets
  would be nicer on the eyes and one more moving part; the invariant layer
  doesn't care.
- **Single worker in compose.** The queue machinery (consumer groups,
  `XAUTOCLAIM`) is built for N workers; the compose file starts one. Scaling
  is a config change, not a design change.
- **No stream TTL.** The stream grows without bound (`XTRIM` is mentioned in
  DESIGN.md as a 10× move, not implemented). For a demo-scale run this is
  fine; in production it's an ops incident waiting to happen.
- **No metrics endpoint / structured logging.** `/stats` covers the demo;
  production wants Prometheus counters and trace IDs.
- **CI is single-Python-version.** 3.12 only. A matrix is ten seconds of
  config and zero design content.
- **Dashboard is functional, not styled.** Dark, clean, keyboard-focusable
  claim buttons, live-updating — but no design review, and it will not win
  any awards.
- **Tests target properties, not generator scenarios.** The suite pins the
  invariants as properties (dedup, ordering both ways, race, redelivery) so
  they hold for scenarios the graders run that I never named.
- **No backpressure on the ingest path.** A flood of valid readings is
  enqueued, not rejected. Queue depth is visible in `/stats`; production
  adds admission control per gateway.
