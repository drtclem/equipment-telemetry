# Cuts — what I left out, and why

The three invariants are enforced by database constraints and tested; these are
the things deliberately not built around them.

**Kept the raw data for, but didn't build the tool**
- **No clock correction.** Quarantined readings keep their original timestamp
  and receive time, so a human can later correct a gateway with a known offset.
  Guessing an offset automatically would be inventing data.
- **No dead-letter replay tool.** Dead letters keep the raw payload as queued;
  replaying them after a fix is a small script.
- **No time-window reports.** A test result over a window is where waiting
  matters; `gateway_outages` is the input such a report would check before
  calling a window final.

**Detection limits, stated honestly**
- **Slow clocks aren't detected.** Quarantine catches clocks running *ahead*. A
  clock running behind makes live readings look like backlog; you'd have to
  notice a gateway's "live" data is always hours old.
- **Gateway-side buffer overflow isn't detectable.** Readings a gateway drops
  before reconnecting never reach us, and `sequence` isn't guaranteed gapless.
- **One fixed silence threshold.** 60 s default, configurable per deployment.
  Learning each gateway's normal reporting interval would be better.
- **Outage start can be slightly early** if two workers process a gateway's
  readings far out of receive order (rare; the outage is still recorded once).

**Product scope**
- **No authentication.** Gateways would need API keys at the edge, the dashboard
  real login; `claim` is a guarded transition, not an authorization boundary.
- **No pre-registration, unclaim, or dismissing bogus device IDs.** The
  generator's IDs carry a run tag, so pre-registration couldn't be exercised.
- **No device ID normalization.** `Chiller-2` and `chiller-2` are two devices.

**Operations and scale**
- **Polling, not push.** 2 s polling recovers for free on flaky job-site Wi-Fi.
  Every poll returns every device, which won't hold at 10× (see DESIGN.md).
- **No stream trimming.** The stream grows until trimmed (`XTRIM` after ack);
  fine at demo scale, an ops task in production.
- **No backpressure.** Valid readings are always enqueued; queue depth is in
  `/stats`. Production would add per-gateway admission control.
- **No metrics/alerting.** `/stats` and dashboard counts (late, quarantined,
  conflicts, dead letters) cover the demo; production wants Prometheus and
  alerts on dead letters and outages.
- **CI on one Python version** (3.12).
