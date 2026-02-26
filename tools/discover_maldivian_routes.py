"""
Discover Maldivian (Q2) routes.

Current behavior:
- Returns HAR-seeded routes (DAC <-> MLE) by default.
- Optional live airport-list probe exists, but the provided HAR captured an error/minimal shell
  response, so route graph discovery is not implemented yet.

Examples:
  python tools/discover_maldivian_routes.py
  python tools/discover_maldivian_routes.py --origin DAC
  python tools/discover_maldivian_routes.py --use-live-probe --output output/maldivian_routes.json
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from modules import maldivian


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--origin", action="append", default=[], help="Filter to one or more origin airports")
    parser.add_argument("--use-live-probe", action="store_true", help="Attempt live PLNext airport-list probe (best-effort)")
    parser.add_argument("--cookies-path", help=f"Cookie JSON path or use {maldivian.ENV_COOKIES_PATH}")
    parser.add_argument("--proxy-url", help=f"Proxy URL or use {maldivian.ENV_PROXY_URL}")
    parser.add_argument("--output", help="Write JSON to file instead of stdout")
    args = parser.parse_args()

    entries = maldivian.discover_route_entries(
        allowed_origins=args.origin,
        use_live_probe=args.use_live_probe,
        cookies_path=args.cookies_path,
        proxy_url=args.proxy_url,
    )

    payload = json.dumps(entries, indent=2, ensure_ascii=False)
    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(payload, encoding="utf-8")
        print(f"Wrote {len(entries)} Q2 routes to {out_path}")
        return

    print(payload)


if __name__ == "__main__":
    main()
