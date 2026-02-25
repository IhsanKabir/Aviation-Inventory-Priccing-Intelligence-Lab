from __future__ import annotations

import argparse
import hashlib
import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
RUNS_DIR = REPO_ROOT / "output" / "manual_sessions" / "runs"
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _load_json(path: Path) -> Dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"Expected JSON object in {path}")
    return raw


def _load_carrier_module(carrier: str):
    code = (carrier or "").strip().upper()
    if code == "BS":
        import modules.bs as module

        return module
    if code == "2A":
        import modules.airastra as module

        return module
    raise ValueError(f"Unsupported carrier: {carrier}")


def _find_latest_run_summary(carrier: str) -> Path:
    prefix = f"{carrier.lower()}_"
    if not RUNS_DIR.exists():
        raise FileNotFoundError(f"Run directory not found: {RUNS_DIR}")
    candidates: list[Path] = []
    for d in RUNS_DIR.iterdir():
        if not d.is_dir():
            continue
        if not d.name.lower().startswith(prefix):
            continue
        summary = d / f"{carrier.lower()}_probe_response.json"
        if summary.exists():
            candidates.append(summary)
    if not candidates:
        raise FileNotFoundError(f"No manual run summaries found for {carrier} under {RUNS_DIR}")
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]


def _parse_iso_naive_utc(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _default_scraped_at(summary_path: Path) -> datetime:
    ts = summary_path.stat().st_mtime
    return datetime.fromtimestamp(ts, tz=timezone.utc).replace(tzinfo=None)


def _deterministic_scrape_id(summary_path: Path, summary: Dict[str, Any]) -> uuid.UUID:
    digest = hashlib.sha256(json.dumps(summary, sort_keys=True, ensure_ascii=False, default=str).encode("utf-8")).hexdigest()
    key = f"tti-manual-ingest|{summary_path.resolve()}|{digest}"
    return uuid.uuid5(uuid.NAMESPACE_URL, key)


def _is_valid_core_offer(o: dict) -> bool:
    required = [
        "airline",
        "flight_number",
        "origin",
        "destination",
        "departure",
        "cabin",
        "brand",
        "price_total_bdt",
    ]
    return all(o.get(k) is not None for k in required)


def _dedupe_core_rows(rows: List[dict]) -> List[dict]:
    seen = set()
    out = []
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
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def _infer_counts_and_cabin(summary: Dict[str, Any], args: argparse.Namespace) -> Tuple[str, int, int, int]:
    sample = (summary.get("parsed_selected_days_sample_rows") or [])
    first = sample[0] if sample and isinstance(sample[0], dict) else {}
    cabin = args.cabin or first.get("cabin") or "Economy"
    adt = int(args.adt if args.adt is not None else (first.get("adt_count") or 1))
    chd = int(args.chd if args.chd is not None else (first.get("chd_count") or 0))
    inf = int(args.inf if args.inf is not None else (first.get("inf_count") or 0))
    return cabin, adt, chd, inf


def _load_best_parsed_rows(
    *,
    module,
    summary: Dict[str, Any],
    bootstrap_cfg: Dict[str, Any],
    cabin: str,
    adt: int,
    chd: int,
    inf: int,
) -> tuple[List[dict], Optional[Path], list[tuple[Path, int]]]:
    html_files = [Path(p) for p in (summary.get("captured_network_html_files") or []) if isinstance(p, str)]
    existing = [p for p in html_files if p.exists()]
    if not existing:
        return [], None, []

    best_rows: List[dict] = []
    best_path: Optional[Path] = None
    stats: list[tuple[Path, int]] = []

    for path in existing:
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue
        try:
            rows = module._extract_rows_if_known(
                text,
                cfg=bootstrap_cfg,
                cabin=cabin,
                adt=adt,
                chd=chd,
                inf=inf,
            )
        except Exception:
            rows = []
        stats.append((path, len(rows)))
        if len(rows) > len(best_rows):
            best_rows = rows
            best_path = path
    return best_rows, best_path, stats


def _check_route_date_mismatch(
    rows: List[dict],
    *,
    expected_origin: Optional[str],
    expected_destination: Optional[str],
    expected_date: Optional[str],
) -> Optional[dict]:
    if not rows:
        return None
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
    exp_route = (str(expected_origin or "").upper(), str(expected_destination or "").upper())
    exp_date = str(expected_date or "")[:10]
    route_match = exp_route in parsed_routes if all(exp_route) else True
    date_match = exp_date in parsed_dates if exp_date else True
    if route_match and date_match:
        return None
    return {
        "expected_route": list(exp_route),
        "expected_date": exp_date,
        "parsed_routes": [list(x) for x in parsed_routes[:10]],
        "parsed_dates": parsed_dates[:10],
        "route_match": route_match,
        "date_match": date_match,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Ingest manual TTInteractive BS/2A capture artifacts into flight_offers.",
    )
    parser.add_argument("--carrier", choices=["BS", "2A"], help="Required when using --latest")
    parser.add_argument("--summary", help="Path to *_probe_response.json from a manual capture run")
    parser.add_argument("--run-dir", help="Path to a manual run directory (contains summary/bootstrap/network files)")
    parser.add_argument("--latest", action="store_true", help="Use the latest run for --carrier")
    parser.add_argument("--bootstrap-config", help="Override path to *_bootstrap_config.json")
    parser.add_argument("--scrape-id", help="Override scrape_id UUID (defaults to deterministic UUID5 from summary)")
    parser.add_argument("--scraped-at", help="Override scraped_at UTC timestamp (ISO8601). Default: summary file mtime")
    parser.add_argument("--cabin", help="Requested cabin passed to parser (defaults to inferred/sample or Economy)")
    parser.add_argument("--adt", type=int, help="Adult count (defaults to inferred/sample or 1)")
    parser.add_argument("--chd", type=int, help="Child count (defaults to inferred/sample or 0)")
    parser.add_argument("--inf", type=int, help="Infant count (defaults to inferred/sample or 0)")
    parser.add_argument("--allow-mismatch", action="store_true", help="Allow ingest even if parsed route/date differs from summary inputs")
    parser.add_argument("--dry-run", action="store_true", help="Parse and validate only; do not insert into DB")
    parser.add_argument("--result-out", help="Optional output JSON path for ingest result manifest")
    args = parser.parse_args()

    if not any([args.summary, args.run_dir, args.latest]):
        parser.error("Provide one of --summary, --run-dir, or --latest")
    if args.latest and not args.carrier:
        parser.error("--carrier is required with --latest")

    if args.summary:
        summary_path = Path(args.summary)
    elif args.run_dir:
        run_dir_p = Path(args.run_dir)
        # try explicit carrier if given, otherwise detect by files
        if args.carrier:
            summary_path = run_dir_p / f"{args.carrier.lower()}_probe_response.json"
        else:
            matches = list(run_dir_p.glob("*_probe_response.json"))
            if len(matches) != 1:
                raise SystemExit(f"Could not uniquely detect summary in {run_dir_p}; use --summary or --carrier")
            summary_path = matches[0]
    else:
        summary_path = _find_latest_run_summary(args.carrier)

    if not summary_path.exists():
        raise SystemExit(f"Summary file not found: {summary_path}")

    summary = _load_json(summary_path)
    carrier = str(summary.get("carrier") or args.carrier or "").upper().strip()
    if carrier not in {"BS", "2A"}:
        raise SystemExit(f"Unsupported/missing carrier in summary: {carrier!r}")

    module = _load_carrier_module(carrier)
    run_dir = summary_path.parent
    bootstrap_path = Path(args.bootstrap_config) if args.bootstrap_config else (run_dir / f"{carrier.lower()}_bootstrap_config.json")
    if not bootstrap_path.exists():
        raise SystemExit(
            f"Bootstrap config not found: {bootstrap_path}. "
            "Use the runner-generated run dir or pass --bootstrap-config."
        )
    bootstrap_cfg = _load_json(bootstrap_path)

    cabin, adt, chd, inf = _infer_counts_and_cabin(summary, args)
    rows, selected_html_path, per_file_counts = _load_best_parsed_rows(
        module=module,
        summary=summary,
        bootstrap_cfg=bootstrap_cfg,
        cabin=cabin,
        adt=adt,
        chd=chd,
        inf=inf,
    )
    if not rows:
        raise SystemExit("No fare rows could be parsed from captured_network_html_files in the summary")

    mismatch = _check_route_date_mismatch(
        rows,
        expected_origin=summary.get("origin"),
        expected_destination=summary.get("destination"),
        expected_date=summary.get("date"),
    )
    if mismatch:
        print("[warn] Parsed fare rows do not match summary route/date inputs:")
        print(json.dumps(mismatch, indent=2))
        if not args.allow_mismatch:
            raise SystemExit("Refusing ingest because of route/date mismatch. Re-run with --allow-mismatch if intentional.")

    scraped_at = _parse_iso_naive_utc(args.scraped_at) if args.scraped_at else _default_scraped_at(summary_path)
    scrape_id = uuid.UUID(args.scrape_id) if args.scrape_id else _deterministic_scrape_id(summary_path, summary)

    from db import normalize_for_db  # lazy import so argparse/help still works without DB deps

    normalized = normalize_for_db(rows, scraped_at=scraped_at, scrape_id=scrape_id)
    skipped_invalid = 0
    valid_rows: List[dict] = []
    for r in normalized:
        if _is_valid_core_offer(r):
            valid_rows.append(r)
        else:
            skipped_invalid += 1
    deduped_rows = _dedupe_core_rows(valid_rows)

    result_manifest = {
        "summary_path": str(summary_path.resolve()),
        "run_dir": str(run_dir.resolve()),
        "carrier": carrier,
        "scrape_id": str(scrape_id),
        "scraped_at_utc": scraped_at.replace(tzinfo=timezone.utc).isoformat(),
        "dry_run": bool(args.dry_run),
        "parser_inputs": {
            "cabin": cabin,
            "adt": adt,
            "chd": chd,
            "inf": inf,
        },
        "summary_inputs": {
            "origin": summary.get("origin"),
            "destination": summary.get("destination"),
            "date": summary.get("date"),
        },
        "selected_html_path": str(selected_html_path.resolve()) if selected_html_path else None,
        "per_file_parsed_counts": [
            {"path": str(p.resolve()), "rows": n} for p, n in per_file_counts
        ],
        "rows_parsed_total": len(rows),
        "rows_valid_for_core": len(valid_rows),
        "rows_deduped_for_core": len(deduped_rows),
        "rows_skipped_invalid": skipped_invalid,
        "rows_inserted": 0,
        "parsed_sample_rows": rows[:3],
    }
    if mismatch:
        result_manifest["parsed_input_mismatch"] = mismatch

    print(f"[manual-ingest] carrier={carrier} summary={summary_path}")
    print(f"[manual-ingest] selected_html={selected_html_path}")
    print(
        f"[manual-ingest] parsed={len(rows)} valid={len(valid_rows)} deduped={len(deduped_rows)} "
        f"skipped_invalid={skipped_invalid}"
    )
    print(f"[manual-ingest] scrape_id={scrape_id} scraped_at_utc={result_manifest['scraped_at_utc']}")

    if not args.dry_run:
        from db import bulk_insert_offers, init_db  # lazy import so --help works without DB deps

        init_db()
        inserted = bulk_insert_offers(deduped_rows)
        result_manifest["rows_inserted"] = int(inserted or 0)
        print(f"[manual-ingest] inserted into flight_offers: {inserted}")
    else:
        print("[manual-ingest] dry-run; no DB writes performed")

    result_out = Path(args.result_out) if args.result_out else (run_dir / f"{carrier.lower()}_manual_ingest_result.json")
    result_out.parent.mkdir(parents=True, exist_ok=True)
    result_out.write_text(json.dumps(result_manifest, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    print(f"[manual-ingest] wrote result manifest: {result_out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
