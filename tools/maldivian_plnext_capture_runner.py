from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse
from urllib.request import urlopen


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

DEFAULT_CDP_URL = "http://127.0.0.1:9222"
HOME_URL = "https://book.maldivian.aero/"
INDEX_URL = "https://book.maldivian.aero/plnext/MaldivianAero/Override.action"


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_UTC")


def _utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _json_dump(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")


def _load_json(path: Path) -> dict[str, Any] | None:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else None
    except Exception:
        return None


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0


def _parse_cdp_port(cdp_url: str) -> int:
    parsed = urlparse(cdp_url)
    if parsed.scheme not in {"http", "ws"} or not parsed.hostname or not parsed.port:
        raise SystemExit(f"Invalid --cdp-url: {cdp_url}")
    if parsed.hostname not in {"127.0.0.1", "localhost"}:
        raise SystemExit("--launch-cdp-browser only supports localhost/127.0.0.1 CDP endpoints")
    return int(parsed.port)


def _cdp_ready(cdp_url: str, timeout_s: float = 2.0) -> bool:
    try:
        with urlopen(cdp_url.rstrip("/") + "/json/version", timeout=timeout_s) as resp:
            return 200 <= int(resp.status) < 300
    except Exception:
        return False


def _wait_for_cdp(cdp_url: str, timeout_s: float = 20.0) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if _cdp_ready(cdp_url):
            return True
        time.sleep(0.5)
    return False


def _default_browser_candidates() -> list[Path]:
    candidates = [
        Path(r"C:\Program Files\Google\Chrome\Application\chrome.exe"),
        Path(r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"),
        Path(r"C:\Program Files\Microsoft\Edge\Application\msedge.exe"),
        Path(r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"),
    ]
    return [p for p in candidates if p.exists()]


def _pick_browser_exe(explicit: str | None) -> Path:
    if explicit:
        p = Path(explicit)
        if not p.exists():
            raise SystemExit(f"--chrome-path not found: {p}")
        return p
    candidates = _default_browser_candidates()
    if not candidates:
        raise SystemExit("Could not find Chrome/Edge automatically. Pass --chrome-path.")
    return candidates[0]


def _launch_cdp_browser(cdp_url: str, chrome_path: str | None, user_data_dir: Path, proxy_server: str | None) -> None:
    if _cdp_ready(cdp_url):
        print(f"[q2-runner] CDP endpoint already available at {cdp_url}; reusing existing browser.")
        return
    port = _parse_cdp_port(cdp_url)
    exe = _pick_browser_exe(chrome_path)
    user_data_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        str(exe),
        f"--remote-debugging-port={port}",
        f"--user-data-dir={user_data_dir}",
        "--no-first-run",
        "--no-default-browser-check",
        "about:blank",
    ]
    if proxy_server:
        cmd.insert(-1, f"--proxy-server={proxy_server}")
    print(f"[q2-runner] Launching browser for CDP attach: {exe}")
    subprocess.Popen(cmd, cwd=str(REPO_ROOT))
    if not _wait_for_cdp(cdp_url, timeout_s=20):
        raise SystemExit(f"CDP endpoint did not become ready: {cdp_url}")
    print(f"[q2-runner] CDP endpoint is ready: {cdp_url}")


def _pick_page(contexts):
    mald_pages = []
    for ctx in contexts:
        for p in ctx.pages:
            if "book.maldivian.aero" in (p.url or ""):
                mald_pages.append(p)
    if mald_pages:
        return mald_pages[-1]
    if not contexts:
        raise RuntimeError("No browser contexts found in attached browser")
    ctx = contexts[0]
    p = ctx.new_page()
    return p


def _page_looks_bad_request(page) -> bool:
    try:
        title = (page.title() or "").strip().lower()
    except Exception:
        title = ""
    if "bad request" in title:
        return True
    try:
        body_text = (page.text_content("body") or "").strip().lower()
    except Exception:
        body_text = ""
    return "bad request" in body_text


def _reset_to_url(page, target_url: str) -> None:
    # Fresh navigation helps when PLNext/Imperva leaves a stale/broken tab state.
    try:
        page.goto("about:blank", wait_until="domcontentloaded", timeout=15000)
    except Exception:
        pass
    page.goto(target_url, wait_until="domcontentloaded", timeout=60000)


def _is_q2_fare_response(url: str) -> bool:
    if not url or "book.maldivian.aero" not in url or "AjaxCall.action" not in url:
        return False
    parsed = urlparse(url)
    q = parse_qs(parsed.query)
    return (q.get("UID", [""])[0].upper() == "FARE") and (q.get("UI_ACTION", [""])[0].lower() == "ajax")


def _dedupe_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen = set()
    out = []
    for r in rows:
        key = (
            r.get("airline"),
            r.get("origin"),
            r.get("destination"),
            r.get("departure"),
            r.get("flight_number"),
            r.get("cabin"),
            r.get("brand"),
            r.get("fare_basis"),
            r.get("total_amount"),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def _deterministic_scrape_id(payload_path: Path, summary: dict[str, Any]) -> uuid.UUID:
    digest = hashlib.sha256(json.dumps(summary, sort_keys=True, ensure_ascii=False, default=str).encode("utf-8")).hexdigest()
    return uuid.uuid5(uuid.NAMESPACE_URL, f"q2-manual-capture|{payload_path.resolve()}|{digest}")


def _auto_ingest(rows: list[dict[str, Any]], summary_path: Path, summary: dict[str, Any], dry_run: bool) -> dict[str, Any]:
    from db import bulk_insert_offers, init_db, normalize_for_db  # type: ignore

    scraped_at = _utc_now_naive()
    scrape_id = _deterministic_scrape_id(summary_path, summary)
    normalized = normalize_for_db(rows, scraped_at=scraped_at, scrape_id=scrape_id)
    deduped = _dedupe_rows(normalized)

    result = {
        "scrape_id": str(scrape_id),
        "scraped_at_utc": scraped_at.isoformat(),
        "rows_parsed_total": len(rows),
        "rows_deduped_for_core": len(deduped),
        "rows_inserted": 0,
        "dry_run": bool(dry_run),
    }
    if dry_run:
        return result

    init_db(create_tables=True)
    inserted = bulk_insert_offers(deduped)
    result["rows_inserted"] = int(inserted or 0)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Automation-first Maldivian PLNext FARE capture runner (CDP attach + auto-capture + parse + optional ingest).",
    )
    parser.add_argument("--origin", required=True)
    parser.add_argument("--destination", required=True)
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--cabin", default="Economy")
    parser.add_argument("--adt", type=int, default=1)
    parser.add_argument("--chd", type=int, default=0)
    parser.add_argument("--inf", type=int, default=0)
    parser.add_argument("--cdp-url", default=DEFAULT_CDP_URL)
    parser.add_argument("--launch-cdp-browser", action="store_true")
    parser.add_argument("--chrome-path")
    parser.add_argument("--proxy-server")
    parser.add_argument("--user-data-dir")
    parser.add_argument("--session-root", default=str(REPO_ROOT / "output" / "manual_sessions"))
    parser.add_argument("--timeout-s", type=int, default=300, help="Time to wait for UID=FARE capture after instructions")
    parser.add_argument("--poll-ms", type=int, default=500)
    parser.add_argument("--open-home", action="store_true", help="Navigate to Maldivian home page on attach if no tab is open/current tab is off-domain")
    parser.add_argument("--open-index", action="store_true", help="Navigate directly to PLNext Override.action (less safe; can trigger Bad Request)")
    parser.add_argument("--keep-browser-open", action="store_true", help="No-op for CDP attach; browser is left open")
    parser.add_argument("--ingest", action="store_true", help="After successful capture+parse, insert into flight_offers")
    parser.add_argument("--ingest-dry-run", action="store_true", help="With --ingest, normalize/validate only (no DB writes)")
    args = parser.parse_args()

    if args.ingest_dry_run and not args.ingest:
        parser.error("--ingest-dry-run requires --ingest")

    session_root = Path(args.session_root)
    session_root.mkdir(parents=True, exist_ok=True)
    run_dir = session_root / "runs" / f"q2_{args.origin.upper()}_{args.destination.upper()}_{args.date}_{_now_tag()}"
    run_dir.mkdir(parents=True, exist_ok=True)

    fare_json_path = run_dir / "q2_fare_uid_response.json"
    summary_path = run_dir / "q2_probe_response.json"
    ingest_path = run_dir / "q2_manual_ingest_result.json"

    user_data_dir = Path(args.user_data_dir) if args.user_data_dir else (session_root / "q2_cdp_profile")
    if args.launch_cdp_browser:
        _launch_cdp_browser(args.cdp_url, args.chrome_path, user_data_dir, args.proxy_server)

    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        print(f"[q2-runner] Playwright is required: {exc}")
        print("[q2-runner] Install in your current Python: python -m pip install playwright")
        return 2

    import modules.maldivian as q2

    captured: dict[str, Any] = {
        "seen_fare_calls": [],
        "ok": False,
        "status": None,
        "fare_url": None,
        "fare_payload": None,
        "fare_response_path": None,
        "error": None,
    }

    print(f"[q2-runner] Run directory: {run_dir}")

    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp(args.cdp_url)
        contexts = browser.contexts
        if not contexts:
            raise SystemExit("[q2-runner] No browser contexts found via CDP. Open Chrome/Edge with remote debugging first.")
        page = _pick_page(contexts)
        ctx = page.context

        def on_response(resp):
            url = resp.url
            if not _is_q2_fare_response(url):
                return
            entry = {"url": url, "status": resp.status}
            captured["seen_fare_calls"].append(entry)
            try:
                headers = {str(k).lower(): str(v) for k, v in (resp.headers or {}).items()}
            except Exception:
                headers = {}
            ct = headers.get("content-type", "")
            if int(resp.status or 0) != 200 or "json" not in ct.lower():
                return
            try:
                text = resp.text()
            except Exception as exc:  # page closed/navigation race
                entry["read_error"] = str(exc)
                return
            try:
                payload = json.loads(text)
            except Exception as exc:
                entry["json_error"] = str(exc)
                return
            if not isinstance(payload, dict):
                entry["json_error"] = "response is not a JSON object"
                return
            captured["ok"] = True
            captured["status"] = int(resp.status or 0)
            captured["fare_url"] = url
            captured["fare_payload"] = payload
            entry["captured"] = True

        ctx.on("response", on_response)

        try:
            if "book.maldivian.aero" not in (page.url or ""):
                if args.open_index:
                    page.goto(INDEX_URL, wait_until="domcontentloaded", timeout=60000)
                elif args.open_home:
                    page.goto(HOME_URL, wait_until="domcontentloaded", timeout=60000)
                else:
                    print("[q2-runner] Attached page is not on Maldivian. Open https://book.maldivian.aero/ in the attached browser, then continue.")
        except Exception as exc:
            print(f"[q2-runner][warn] Could not auto-open Maldivian page: {exc}")

        # Common failure mode: runner reuses a stale Maldivian tab already showing "Bad Request".
        if _page_looks_bad_request(page):
            print("[q2-runner][warn] Current Maldivian tab is on a 'Bad Request' page. Resetting to home page...")
            try:
                _reset_to_url(page, HOME_URL)
            except Exception as exc:
                print(f"[q2-runner][warn] Auto-reset failed: {exc}")
                print("[q2-runner][hint] Manually open https://book.maldivian.aero/ in the attached browser, then continue to the booking flow from the site UI.")

        print("")
        print("[q2-runner] Automation-first capture armed for Maldivian UID=FARE.")
        print("Manual steps are only for Imperva/reCAPTCHA and the PLNext UI search (if needed).")
        print(f"1. In the browser, search {args.origin.upper()} -> {args.destination.upper()} for {args.date}")
        print("2. Solve reCAPTCHA/Imperva challenge if shown")
        print("3. Wait for fares/results to load")
        print("4. The tool will auto-capture the UID=FARE JSON and continue automatically")
        print("")

        deadline = time.time() + max(10, int(args.timeout_s))
        last_log = 0.0
        while time.time() < deadline and not captured["ok"]:
            now = time.time()
            if now - last_log >= 10:
                remain = int(max(0, deadline - now))
                page_url = page.url
                print(f"[q2-runner] Waiting for UID=FARE response... ({remain}s left) page={page_url}")
                last_log = now
            # If the page falls back to a generic error, nudge the operator early.
            if _page_looks_bad_request(page):
                print("[q2-runner][warn] Browser is on 'Bad Request'. Open/reload https://book.maldivian.aero/ then return to booking and run the search again.")
                # Avoid spamming this warning; wait a bit before checking again.
                try:
                    page.wait_for_timeout(3000)
                except Exception:
                    time.sleep(3)
                continue
            try:
                page.wait_for_timeout(max(100, int(args.poll_ms)))
            except Exception:
                time.sleep(max(0.1, float(args.poll_ms) / 1000.0))

        if captured["ok"] and isinstance(captured["fare_payload"], dict):
            _json_dump(fare_json_path, captured["fare_payload"])
            captured["fare_response_path"] = str(fare_json_path)

        rows = q2._extract_rows_from_fare_ajax(
            captured.get("fare_payload"),
            requested_cabin=args.cabin,
            adt=args.adt,
            chd=args.chd,
            inf=args.inf,
        ) if captured.get("ok") else []

        parsed_rows_count = len(rows)
        parsed_sample = rows[:3]
        mismatch = None
        if rows:
            parsed_routes = {(str(r.get("origin") or "").upper(), str(r.get("destination") or "").upper()) for r in rows}
            parsed_dates = {(str(r.get("search_date") or "")[:10]) or (str(r.get("departure") or "")[:10]) for r in rows}
            exp_route = (args.origin.upper(), args.destination.upper())
            exp_date = str(args.date)[:10]
            if (exp_route not in parsed_routes) or (exp_date and exp_date not in parsed_dates):
                mismatch = {
                    "expected_route": list(exp_route),
                    "expected_date": exp_date,
                    "parsed_routes": [list(x) for x in sorted(parsed_routes)],
                    "parsed_dates": sorted(parsed_dates),
                }

        summary = {
            "carrier": "Q2",
            "status": captured.get("status"),
            "ok": bool(captured.get("ok") and parsed_rows_count > 0),
            "origin": args.origin.upper(),
            "destination": args.destination.upper(),
            "date": args.date,
            "cabin": args.cabin,
            "adt": args.adt,
            "chd": args.chd,
            "inf": args.inf,
            "fare_uid_url": captured.get("fare_url"),
            "fare_uid_response_path": captured.get("fare_response_path"),
            "seen_fare_calls": captured.get("seen_fare_calls") or [],
            "parsed_selected_days_rows_count": parsed_rows_count,
            "parsed_selected_days_sample_rows": parsed_sample,
            "parsed_selected_days_input_mismatch": mismatch,
            "final_page_url": page.url,
            "timeout_s": args.timeout_s,
        }
        _json_dump(summary_path, summary)

        print("")
        print("[q2-runner] Artifacts")
        print(f"  run_dir: {run_dir}")
        print(f"  summary: {summary_path}")
        if fare_json_path.exists():
            print(f"  fare_json: {fare_json_path}")
        print("")
        print("[q2-runner] Result summary")
        print(json.dumps(
            {
                "status": summary.get("status"),
                "ok": summary.get("ok"),
                "parsed_selected_days_rows_count": summary.get("parsed_selected_days_rows_count"),
                "final_page_url": summary.get("final_page_url"),
            },
            indent=2,
        ))
        if mismatch:
            print("")
            print("[q2-runner][warn] Parsed route/date does not match CLI inputs.")
            print(json.dumps(mismatch, indent=2))

        if not summary["ok"]:
            if captured.get("seen_fare_calls"):
                print("")
                print("[q2-runner][warn] Saw UID=FARE calls but could not parse a successful 200 JSON fare response.")
            else:
                print("")
                print("[q2-runner][warn] No UID=FARE response was captured. Search may not have reached FARE Ajax.")
            print("[q2-runner] Browser is left open (CDP attach).")
            return 1

        if args.ingest:
            print("")
            print("[q2-runner] Starting auto-ingest ...")
            try:
                ingest_result = _auto_ingest(rows, summary_path, summary, dry_run=args.ingest_dry_run)
            except Exception as exc:
                print(f"[q2-runner] FAILED: auto-ingest error: {exc}")
                return 2
            _json_dump(ingest_path, ingest_result)
            print(json.dumps(ingest_result, indent=2))
            print("")
            if args.ingest_dry_run:
                print("[q2-runner] SUCCESS: capture + ingest dry-run completed")
            else:
                print(f"[q2-runner] SUCCESS: capture + ingest completed (rows_inserted={ingest_result.get('rows_inserted')})")
            print("[q2-runner] Browser is left open (CDP attach).")
            return 0

        print("")
        print(f"[q2-runner] SUCCESS: parsed_selected_days_rows_count={parsed_rows_count}")
        print("[q2-runner] Browser is left open (CDP attach).")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
