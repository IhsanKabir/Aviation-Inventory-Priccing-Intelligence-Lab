from __future__ import annotations

import argparse
import hashlib
import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


REPO_ROOT = Path(__file__).resolve().parents[1]
RUNS_DIR = REPO_ROOT / "output" / "manual_sessions" / "runs"
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _load_json(path: Path) -> Dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(raw, dict):
        raise ValueError(f"Expected JSON object in {path}")
    return raw


def _find_latest_q2_summary() -> Path:
    if not RUNS_DIR.exists():
        raise FileNotFoundError(f"Run directory not found: {RUNS_DIR}")
    candidates = sorted(
        (p for p in RUNS_DIR.glob("q2_*/*q2_probe_response.json") if p.is_file()),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        raise FileNotFoundError(f"No Q2 run summaries found under {RUNS_DIR}")
    return candidates[0]


def _parse_iso_naive_utc(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _default_scraped_at(path: Path) -> datetime:
    return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).replace(tzinfo=None)


def _deterministic_scrape_id(summary_path: Path, summary: Dict[str, Any]) -> uuid.UUID:
    digest = hashlib.sha256(
        json.dumps(summary, sort_keys=True, ensure_ascii=False, default=str).encode("utf-8")
    ).hexdigest()
    return uuid.uuid5(uuid.NAMESPACE_URL, f"q2-manual-ingest|{summary_path.resolve()}|{digest}")


def _dedupe_core_rows(rows: List[dict]) -> List[dict]:
    seen = set()
    out: List[dict] = []
    for r in rows:
        key = (
            str(r.get("scrape_id")),
            r.get("airline"),
            r.get("origin"),
            r.get("destination"),
            r.get("departure"),
            r.get("flight_number"),
            r.get("cabin"),
            r.get("fare_basis"),
            r.get("brand"),
            r.get("total_amount"),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def _dedupe_parsed_rows(rows: List[dict]) -> List[dict]:
    seen = set()
    out: List[dict] = []
    for r in rows:
        key = (
            r.get("airline"),
            r.get("origin"),
            r.get("destination"),
            r.get("departure"),
            r.get("flight_number"),
            r.get("cabin"),
            r.get("fare_basis"),
            r.get("brand"),
            r.get("total_amount"),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def _check_route_date_mismatch(rows: List[dict], summary: Dict[str, Any]) -> Optional[dict]:
    if not rows:
        return None
    expected_origin = str(summary.get("origin") or "").upper()
    expected_destination = str(summary.get("destination") or "").upper()
    expected_date = str(summary.get("date") or "")[:10]
    parsed_routes = sorted(
        {
            (str(r.get("origin") or "").upper(), str(r.get("destination") or "").upper())
            for r in rows
            if r.get("origin") and r.get("destination")
        }
    )
    parsed_dates = sorted(
        {
            (str(r.get("search_date") or "")[:10]) or (str(r.get("departure") or "")[:10])
            for r in rows
            if (r.get("search_date") or r.get("departure"))
        }
    )
    route_match = (expected_origin, expected_destination) in parsed_routes if (expected_origin and expected_destination) else True
    date_match = expected_date in parsed_dates if expected_date else True
    if route_match and date_match:
        return None
    return {
        "expected_route": [expected_origin, expected_destination],
        "expected_date": expected_date,
        "parsed_routes": [list(x) for x in parsed_routes],
        "parsed_dates": parsed_dates,
        "route_match": route_match,
        "date_match": date_match,
    }


def _resolve_summary_path(args: argparse.Namespace) -> Path:
    if args.summary:
        p = Path(args.summary)
        if not p.exists():
            raise SystemExit(f"Summary not found: {p}")
        return p
    if args.run_dir:
        p = Path(args.run_dir) / "q2_probe_response.json"
        if not p.exists():
            raise SystemExit(f"q2_probe_response.json not found under run dir: {args.run_dir}")
        return p
    if args.latest:
        return _find_latest_q2_summary()
    raise SystemExit("Provide one of --summary, --run-dir, or --latest")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Ingest Maldivian (Q2) manual-captured UID=FARE JSON into flight_offers.",
    )
    parser.add_argument("--summary", help="Path to q2_probe_response.json")
    parser.add_argument("--run-dir", help="Path to a Q2 run dir containing q2_probe_response.json")
    parser.add_argument("--latest", action="store_true", help="Use latest Q2 run summary under output/manual_sessions/runs")
    parser.add_argument("--fare-json", help="Override path to q2_fare_uid_response.json (defaults from summary/run dir)")
    parser.add_argument("--scrape-id", help="Override scrape_id UUID (default: deterministic UUID5 from summary)")
    parser.add_argument("--scraped-at", help="Override scraped_at UTC timestamp (ISO8601)")
    parser.add_argument("--cabin", help="Override cabin passed to parser (default summary/sample or Economy)")
    parser.add_argument("--adt", type=int, help="Override ADT count")
    parser.add_argument("--chd", type=int, help="Override CHD count")
    parser.add_argument("--inf", type=int, help="Override INF count")
    parser.add_argument("--allow-mismatch", action="store_true", help="Allow ingest even if parsed route/date differs from summary inputs")
    parser.add_argument("--dry-run", action="store_true", help="Parse/validate only; no DB writes")
    parser.add_argument("--result-out", help="Optional output path for q2_manual_ingest_result.json")
    args = parser.parse_args()

    if sum(1 for x in [args.summary, args.run_dir, args.latest] if x) != 1:
        parser.error("Provide exactly one of --summary, --run-dir, or --latest")

    summary_path = _resolve_summary_path(args)
    run_dir = summary_path.parent
    summary = _load_json(summary_path)

    if str(summary.get("carrier") or "").upper() not in {"Q2", ""}:
        raise SystemExit(f"Summary carrier is not Q2: {summary.get('carrier')!r}")

    fare_json_path: Path
    if args.fare_json:
        fare_json_path = Path(args.fare_json)
    else:
        summary_fare = summary.get("fare_uid_response_path")
        if isinstance(summary_fare, str) and summary_fare.strip():
            fare_json_path = Path(summary_fare)
        else:
            fare_json_path = run_dir / "q2_fare_uid_response.json"
    if not fare_json_path.exists():
        raise SystemExit(f"Fare JSON file not found: {fare_json_path}")

    import modules.maldivian as q2

    payload = _load_json(fare_json_path)
    cabin = args.cabin or str(summary.get("cabin") or "Economy")
    adt = int(args.adt if args.adt is not None else (summary.get("adt") or 1))
    chd = int(args.chd if args.chd is not None else (summary.get("chd") or 0))
    inf = int(args.inf if args.inf is not None else (summary.get("inf") or 0))

    rows = q2._extract_rows_from_fare_ajax(
        payload,
        requested_cabin=cabin,
        adt=adt,
        chd=chd,
        inf=inf,
    )
    if not rows:
        raise SystemExit("No rows parsed from q2_fare_uid_response.json")

    mismatch = _check_route_date_mismatch(rows, summary)
    if mismatch:
        print("[q2-ingest][warn] Parsed fare rows do not match summary route/date inputs:")
        print(json.dumps(mismatch, indent=2))
        if not args.allow_mismatch:
            raise SystemExit("Refusing ingest because of route/date mismatch. Re-run with --allow-mismatch if intentional.")

    scraped_at = _parse_iso_naive_utc(args.scraped_at) if args.scraped_at else _default_scraped_at(summary_path)
    scrape_id = uuid.UUID(args.scrape_id) if args.scrape_id else _deterministic_scrape_id(summary_path, summary)

    deduped_rows: List[dict]
    normalized: List[dict] | None = None
    if args.dry_run:
        deduped_rows = _dedupe_parsed_rows(rows)
    else:
        from db import normalize_for_db  # lazy import only when DB-capable path is used

        normalized = normalize_for_db(rows, scraped_at=scraped_at, scrape_id=scrape_id)
        deduped_rows = _dedupe_core_rows(normalized)

    result_manifest: Dict[str, Any] = {
        "summary_path": str(summary_path.resolve()),
        "run_dir": str(run_dir.resolve()),
        "fare_json_path": str(fare_json_path.resolve()),
        "carrier": "Q2",
        "scrape_id": str(scrape_id),
        "scraped_at_utc": scraped_at.replace(tzinfo=timezone.utc).isoformat(),
        "dry_run": bool(args.dry_run),
        "parser_inputs": {"cabin": cabin, "adt": adt, "chd": chd, "inf": inf},
        "summary_inputs": {
            "origin": summary.get("origin"),
            "destination": summary.get("destination"),
            "date": summary.get("date"),
        },
        "rows_parsed_total": len(rows),
        "rows_deduped_for_core": len(deduped_rows),
        "rows_inserted": 0,
        "parsed_sample_rows": rows[:3],
    }
    if mismatch:
        result_manifest["parsed_input_mismatch"] = mismatch

    print(f"[q2-ingest] summary={summary_path}")
    print(f"[q2-ingest] fare_json={fare_json_path}")
    print(f"[q2-ingest] parsed={len(rows)} deduped={len(deduped_rows)}")
    print(f"[q2-ingest] scrape_id={scrape_id} scraped_at_utc={result_manifest['scraped_at_utc']}")

    if not args.dry_run:
        from db import bulk_insert_offers, init_db  # lazy import

        init_db(create_tables=True)
        inserted = bulk_insert_offers(deduped_rows)
        result_manifest["rows_inserted"] = int(inserted or 0)
        print(f"[q2-ingest] inserted into flight_offers: {inserted}")
    else:
        print("[q2-ingest] dry-run; no DB writes performed")

    result_out = Path(args.result_out) if args.result_out else (run_dir / "q2_manual_ingest_result.json")
    result_out.parent.mkdir(parents=True, exist_ok=True)
    result_out.write_text(json.dumps(result_manifest, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    print(f"[q2-ingest] wrote result manifest: {result_out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
