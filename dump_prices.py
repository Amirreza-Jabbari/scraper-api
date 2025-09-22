#!/usr/bin/env python3
"""
Connect to Redis, list keys matching "prices:*", and pretty-print JSON values
(using proper UTF-8 decoding so Persian text appears correctly).
Usage:
  python dump_prices.py            # uses redis://localhost:6379/0
  python dump_prices.py redis://host:6379/1
  python dump_prices.py --pattern "prices:mohammadrahimi:*"
"""
import sys
import json
import argparse
from redis import Redis

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("redis_url", nargs="?", default="redis://localhost:6379/0",
                   help="Redis URL (default redis://localhost:6379/0)")
    p.add_argument("--pattern", default="prices:*", help="SCAN pattern for keys")
    p.add_argument("--show-raw", action="store_true", help="Show raw repr of the value (debug)")
    return p.parse_args()

def main():
    args = parse_args()
    r = Redis.from_url(args.redis_url, decode_responses=True)  # decode_responses=True returns python str
    # Use SCAN to avoid blocking on large DBs
    print(f"Connecting to {args.redis_url!s} and scanning for pattern {args.pattern!r}...\n")
    keys = list(r.scan_iter(args.pattern))
    if not keys:
        print("No keys found.")
        return
    print(f"Found {len(keys)} keys.\n")
    for k in keys:
        try:
            v = r.get(k)
        except Exception as exc:
            print(f"ERROR reading key {k!r}: {exc}")
            continue
        print("=" * 80)
        print("KEY:", k)
        if v is None:
            print("<nil>\n")
            continue
        if args.show_raw:
            print("RAW REPR:", repr(v))
        # Attempt to parse as JSON
        try:
            obj = json.loads(v)
            pretty = json.dumps(obj, indent=2, ensure_ascii=False)
            print(pretty)
        except Exception:
            # Not JSON? just print as text (safe for Persian)
            print(v)
    print("\nDone.")

if __name__ == "__main__":
    main()
