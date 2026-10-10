#!/usr/bin/env python3
"""Stage a gateway outage end to end, for the demo and for manual checking.

The provided generator's `replay` scenario sends from a gateway with no prior
contact, so it never shows a gateway going silent and coming back. This does:

  1. two gateways report live (gw-demo-1 -> ahu-demo, gw-demo-2 -> pump-demo)
  2. gw-demo-1 drops off and buffers locally; gw-demo-2 keeps reporting
     -> dashboard: ahu-demo flagged "gateway gw-demo-1 silent", pump-demo live
  3. gw-demo-1 reconnects: replays its backlog oldest-first (with one
     byte-identical resend), then a live reading
     -> history fills in, current state is the newest reading, one outage row

Standard library only. Set --silence to the server's SILENCE_THRESHOLD_SECONDS
(default 60; start the stack with SILENCE_THRESHOLD_SECONDS=10 for a quick demo).

  python3 scripts/outage_demo.py --target http://localhost:8000 --silence 10
"""
import argparse
import json
import time
import urllib.request
from datetime import UTC, datetime


def post(target, gateway_id, device_id, value, event_time, sequence):
    body = json.dumps({
        "gateway_id": gateway_id, "device_id": device_id, "metric": "temperature",
        "value": value, "event_time": event_time.isoformat(), "sequence": sequence,
    }).encode()
    req = urllib.request.Request(f"{target.rstrip('/')}/ingest/readings", data=body,
                                 headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=5) as resp:
        return resp.status


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--target", default="http://localhost:8000")
    p.add_argument("--silence", type=int, default=60,
                   help="server's SILENCE_THRESHOLD_SECONDS (default 60)")
    args = p.parse_args()
    run = int(time.time()) % 100000
    gw1, gw2 = f"gw-demo-1-{run}", f"gw-demo-2-{run}"
    ahu, pump = f"ahu-demo-{run}", f"pump-demo-{run}"
    seq1 = seq2 = 0

    print(f"1) both gateways live ({gw1} -> {ahu}, {gw2} -> {pump})")
    for i in range(5):
        seq1 += 1
        seq2 += 1
        post(args.target, gw1, ahu, 55.0 + i * 0.1, datetime.now(UTC), seq1)
        post(args.target, gw2, pump, 40.0 + i, datetime.now(UTC), seq2)
        time.sleep(1)

    dark = args.silence + 5
    print(f"2) {gw1} drops off for {dark}s and buffers; {gw2} keeps reporting")
    print(f"   watch: {ahu} flagged 'gateway silent' after {args.silence}s, {pump} stays live")
    backlog = []
    for i in range(dark):
        seq1 += 1
        seq2 += 1
        backlog.append((60.0 + i * 0.1, datetime.now(UTC), seq1))  # taken, not sent
        post(args.target, gw2, pump, 45.0 + i % 5, datetime.now(UTC), seq2)
        time.sleep(1)

    print(f"3) {gw1} reconnects: {len(backlog)} buffered readings oldest-first, one resend, "
          "then live")
    for value, taken_at, seq in backlog:
        post(args.target, gw1, ahu, value, taken_at, seq)
    post(args.target, gw1, ahu, *backlog[0])  # byte-identical resend: must dedupe
    seq1 += 1
    live = 99.9
    post(args.target, gw1, ahu, live, datetime.now(UTC), seq1)

    print("\nExpect:")
    print(f"  {ahu}: current temperature {live} (newest), flag cleared")
    print(f"  {ahu} history: {5 + len(backlog) + 1} readings (the resend deduped)")
    print(f"  Gateway outages panel: {gw1} silent ~{dark}s")


if __name__ == "__main__":
    main()
