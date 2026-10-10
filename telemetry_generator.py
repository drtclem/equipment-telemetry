#!/usr/bin/env python3
"""Candidate-facing gateway simulator for the Equipment Telemetry Ingestion take-home.

Scenarios:
  clean   - a few gateways, a few known devices, no faults
  replay  - one gateway disconnects, buffers, and replays its backlog interleaved
            with another gateway's live traffic, resending some readings
  race    - two gateways report the same brand-new device at nearly the same moment
  trace   - prints your personalized scenario (from --seed) for you to trace by hand
            in DESIGN.md, then sends it to --target so you can check your own
            system's result against your trace before submitting

Usage:
  python3 telemetry_generator.py --target http://localhost:8080 --scenario clean
  python3 telemetry_generator.py --target http://localhost:8080 --scenario replay
  python3 telemetry_generator.py --target http://localhost:8080 --scenario race
  python3 telemetry_generator.py --target http://localhost:8080 --scenario trace --seed <your GitHub username>
"""
import argparse
import json
import random
import threading
import time
import urllib.request
import urllib.error
from datetime import datetime, timedelta, timezone


def now():
    return datetime.now(timezone.utc)


def iso(dt):
    return dt.isoformat()


def post_reading(target, gateway_id, device_id, metric, value, event_time, sequence):
    body = json.dumps({
        "gateway_id": gateway_id,
        "device_id": device_id,
        "metric": metric,
        "value": value,
        "event_time": iso(event_time),
        "sequence": sequence,
    }).encode()
    req = urllib.request.Request(
        f"{target.rstrip('/')}/ingest/readings",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status
    except urllib.error.HTTPError as e:
        return e.code
    except urllib.error.URLError as e:
        print(f"  [warn] delivery failed: {e}")
        return None


def run_tag():
    return f"{int(time.time() * 1000) % 1000000}"


def scenario_clean(target, **_):
    print("clean: three gateways, five known devices, no faults")
    tag = run_tag()
    base = now()
    for gw in range(3):
        for dev in range(5):
            for i in range(10):
                post_reading(
                    target, f"gw-clean-{gw}-{tag}", f"known-device-clean-{dev}-{tag}",
                    random.choice(["temperature", "vibration", "power_draw", "status"]),
                    round(random.uniform(0, 100), 2),
                    base + timedelta(seconds=i * 0.2), i,
                )
                time.sleep(0.02)


def scenario_replay(target, **_):
    print("replay: gw-1 goes dark, buffers, replays interleaved with gw-0 live traffic, resends some readings")
    device = f"known-device-replay-{run_tag()}"
    backlog_start = now()
    buffered = [(backlog_start - timedelta(seconds=(20 - i)), i) for i in range(20)]

    def live():
        base = now()
        for i in range(20):
            post_reading(target, "gw-0", device, "temperature", round(random.uniform(60, 90), 1),
                         base + timedelta(seconds=i * 0.1), 1000 + i)
            time.sleep(0.1)

    t = threading.Thread(target=live)
    t.start()
    time.sleep(0.5)
    for event_time, seq in buffered:
        val = round(random.uniform(0, 5), 3)
        post_reading(target, "gw-1", device, "vibration", val, event_time, seq)
        if seq % 4 == 0:  # resend a few, byte-for-byte unchanged, like a real resend
            post_reading(target, "gw-1", device, "vibration", val, event_time, seq)
    t.join()
    print(f"  check device '{device}': history has no gaps, current state isn't stuck on stale data")


def scenario_race(target, **_):
    print("race: two gateways report the same brand-new device at nearly the same moment")
    device = f"new-device-{random.randint(10000, 99999)}"
    barrier = threading.Barrier(2)

    def report(gw):
        barrier.wait()
        post_reading(target, gw, device, "status", 1, now(), 0)

    threads = [threading.Thread(target=report, args=(f"gw-race-{i}",)) for i in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print(f"  check: exactly one device record for {device}")


def scenario_trace(target, seed, assume_yes=False, **_):
    if not seed:
        raise SystemExit("--seed is required for the trace scenario (use your GitHub username)")
    rnd = random.Random(seed)
    device = f"trace-device-{rnd.randint(1000, 9999)}"
    gateways = [f"gw-a-{seed}", f"gw-b-{seed}"]
    base = now()

    # Deterministic from seed: a short event list mixing arrival order, a real
    # duplicate, and out-of-order event times, small enough to trace by hand.
    offsets = [0, 1, 2, 3, 4, 5]
    rnd.shuffle(offsets)
    events = []
    for i, off in enumerate(offsets):
        gw = gateways[rnd.randint(0, 1)]
        metric = rnd.choice(["temperature", "vibration", "power_draw", "status"])
        value = round(rnd.uniform(0, 100), 2)
        events.append({"seq": i, "gateway_id": gw, "metric": metric, "value": value,
                       "event_time_offset_s": off})
    dup = dict(rnd.choice(events))  # exact resend of one earlier event, unchanged
    events.append(dup)

    print(f"your personalized trace scenario (seed={seed}), device={device}:")
    print(f"{'#':>2}  {'gateway':<14} {'metric':<12} {'value':>8}  {'event_time offset (s)':>22}")
    for e in events:
        print(f"{e['seq']:>2}  {e['gateway_id']:<14} {e['metric']:<12} {e['value']:>8}  {e['event_time_offset_s']:>22}")
    print()
    print("Before running this against your system: in DESIGN.md, trace what your system")
    print("stores, what it shows as current per metric, and what (if anything) it flags")
    print("late, in the order above. Then run this for real and compare.")
    if not assume_yes:
        input("press enter to actually send these events to --target now... ")

    for e in events:
        post_reading(target, e["gateway_id"], device, e["metric"], e["value"],
                     base + timedelta(seconds=e["event_time_offset_s"]), e["seq"])
        time.sleep(0.05)
    print(f"sent. inspect device '{device}' in your own system now and compare to your trace.")


SCENARIOS = {
    "clean": scenario_clean,
    "replay": scenario_replay,
    "race": scenario_race,
    "trace": scenario_trace,
}


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--target", required=True, help="base URL of your running system")
    p.add_argument("--scenario", required=True, choices=sorted(SCENARIOS))
    p.add_argument("--seed", default=None, help="required for --scenario trace; use your GitHub username")
    p.add_argument("--yes", dest="assume_yes", action="store_true", help="skip the confirmation prompt in --scenario trace")
    args = p.parse_args()
    SCENARIOS[args.scenario](args.target, seed=args.seed, assume_yes=args.assume_yes)


if __name__ == "__main__":
    main()
