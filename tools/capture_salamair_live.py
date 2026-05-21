from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date as dt_date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from modules import salamair as ov


DEFAULT_SESSION_ROOT = REPO_ROOT / "output" / "manual_sessions"
SEARCH_PAGE_URL = "https://booking.salamair.com/en/search"
# SalamAir migrated from POST /api/flights/flightFares to GET /api/flights?TripType=...
# The token must include the "?" to avoid matching /api/flights/specialFares
FLIGHT_FARES_URL_TOKEN = "api.salamair.com/api/flights?"
CONFIRM_URL_TOKEN = "/api/flights/confirm"


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_UTC")


def _json_dump(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")


def _parse_iso_dates(values: List[str]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for raw in values:
        text = str(raw or "").strip()
        if not text:
            continue
        try:
            normalized = dt_date.fromisoformat(text).isoformat()
        except Exception:
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


def _expand_date_range(start_raw: Optional[str], end_raw: Optional[str]) -> List[str]:
    if not start_raw and not end_raw:
        return []
    if not start_raw or not end_raw:
        return _parse_iso_dates([start_raw or end_raw or ""])
    start = dt_date.fromisoformat(str(start_raw))
    end = dt_date.fromisoformat(str(end_raw))
    if end < start:
        start, end = end, start
    current = start
    out: List[str] = []
    while current <= end:
        out.append(current.isoformat())
        current += timedelta(days=1)
    return out


def _safe_json_or_none(text: str) -> Any:
    try:
        return json.loads(text)
    except Exception:
        return None


def _choose_confirm_sell_key(payload: Dict[str, Any], preferred_brand: str) -> Optional[str]:
    preferred = str(preferred_brand or "").strip().lower()
    fallback: Optional[str] = None
    for flight in payload.get("flights") or []:
        if not isinstance(flight, dict):
            continue
        for fare in flight.get("fares") or []:
            if not isinstance(fare, dict):
                continue
            brand = str(fare.get("fareTypeName") or "").strip().lower()
            for fare_info in fare.get("fareInfos") or []:
                if not isinstance(fare_info, dict):
                    continue
                sell_key = str(fare_info.get("fareSellKey") or "").strip()
                if not sell_key:
                    return sell_key
                if brand == preferred:
                    return sell_key
                if fallback is None:
                    fallback = sell_key
    return fallback


def _dismiss_popups(page) -> None:
    labels = ["Accept", "Accept All", "Allow All", "I Agree", "Got it", "OK", "Close"]
    for label in labels:
        try:
            button = page.get_by_role("button", name=label).first
            if button and button.is_visible(timeout=300):
                button.click(timeout=1200)
                page.wait_for_timeout(100)
        except Exception:
            continue


def _fill_react_select(page, input_id: str, value: str) -> bool:
    """Fill a SalamAir react-select airport dropdown and confirm selection.

    Clicks the first visible option directly via mouse coordinates, which
    reliably fires react-select's onMouseDown→selectOption chain.
    Falls back to ArrowDown+Enter if no option is clickable.
    """
    try:
        inp = page.locator(f"#{input_id}")
        inp.click()
        time.sleep(0.4)
        inp.type(value, delay=120)

        # Wait for the dropdown option list to appear
        try:
            page.wait_for_selector(".airport-select__option", timeout=4000)
        except Exception:
            pass
        time.sleep(0.8)

        # Click the first visible option using raw mouse coordinates so that
        # react-select's mousedown handler fires before any focus/blur side-effects.
        for sel in [".airport-select__option--is-focused", ".airport-select__option"]:
            try:
                opt = page.locator(sel).first
                if opt.count() > 0 and opt.is_visible(timeout=600):
                    box = opt.bounding_box()
                    if box:
                        cx = box["x"] + box["width"] / 2
                        cy = box["y"] + box["height"] / 2
                        page.mouse.move(cx, cy)
                        time.sleep(0.08)
                        page.mouse.down()
                        time.sleep(0.05)
                        page.mouse.up()
                        time.sleep(0.5)
                        return True
            except Exception:
                continue

        # Keyboard fallback
        page.keyboard.press("ArrowDown")
        time.sleep(0.25)
        page.keyboard.press("Enter")
        time.sleep(0.4)
        return True
    except Exception:
        pass
    return False


def _select_datepicker_day(page, dep_date: str, calendar_idx: int = 0) -> bool:
    """Click a specific day in SalamAir's react-datepicker calendar.

    calendar_idx=0 targets the left (departure) calendar.
    calendar_idx=1 targets the right (return) calendar in round-trip mode.
    dep_date format: YYYY-MM-DD.

    Does NOT filter by "Not available" — clicks the target day regardless,
    because unavailability labels only appear when no route is selected yet
    and we want the click to register.
    """
    import calendar as cal
    from datetime import date as dt_date_cls

    try:
        target = dt_date_cls.fromisoformat(dep_date)
    except Exception:
        return False

    month_map = {m[:3].upper(): i for i, m in enumerate(cal.month_name) if m}

    def _get_container():
        containers = page.query_selector_all(".react-datepicker__month-container")
        if not containers:
            return None
        return containers[min(calendar_idx, len(containers) - 1)]

    def _header_ym(container):
        hdr = container.query_selector(".react-datepicker__current-month")
        if not hdr:
            return None, None
        parts = str(hdr.inner_text() or "").strip().upper().split()
        if len(parts) < 2 or not parts[1].isdigit():
            return None, None
        mn = month_map.get(parts[0][:3])
        return mn, int(parts[1])

    # Navigate the correct calendar to the target month
    for _ in range(8):
        try:
            container = _get_container()
            if not container:
                time.sleep(0.4)
                continue
            month_num, year_num = _header_ym(container)
            if month_num is None:
                break
            if year_num == target.year and month_num == target.month:
                break
            if (year_num, month_num) < (target.year, target.month):
                # Need to go forward — click the rightmost "next" button so we
                # advance the correct side in a two-calendar layout.
                next_btns = page.query_selector_all(".react-datepicker__navigation--next")
                if not next_btns:
                    break
                next_btns[-1].click()
            else:
                prev_btns = page.query_selector_all(".react-datepicker__navigation--previous")
                if not prev_btns:
                    break
                prev_btns[0].click()
            time.sleep(0.5)
        except Exception:
            break

    # Build aria-label fragments — react-datepicker uses ordinal suffixes
    try:
        month_name_full = cal.month_name[target.month]
        day = target.day
        if 11 <= day <= 13:
            suffix = "th"
        elif day % 10 == 1:
            suffix = "st"
        elif day % 10 == 2:
            suffix = "nd"
        elif day % 10 == 3:
            suffix = "rd"
        else:
            suffix = "th"
        fragments = [
            f"{month_name_full} {day}{suffix}",  # "May 8th"
            f"{month_name_full} {day},",          # "May 8,"
            f"{month_name_full} {day} ",          # "May 8 "
        ]

        container = _get_container()
        day_els = (
            container.query_selector_all(".react-datepicker__day")
            if container
            else page.query_selector_all(".react-datepicker__day")
        )

        # First pass: match by aria-label, skip outside-month days only
        for day_el in day_els:
            aria = str(day_el.get_attribute("aria-label") or "")
            cls = str(day_el.get_attribute("class") or "")
            if "outside-month" in cls:
                continue
            if any(frag in aria for frag in fragments):
                day_el.click()
                time.sleep(0.3)
                return True

        # Second pass: match by CSS class e.g. react-datepicker__day--008
        day_class_num = str(day).zfill(3)
        for day_el in day_els:
            cls = str(day_el.get_attribute("class") or "")
            if f"--{day_class_num}" in cls and "outside-month" not in cls and "disabled" not in cls:
                day_el.click()
                time.sleep(0.3)
                return True
    except Exception as exc:
        print(f"[warn] datepicker click error: {exc}", file=sys.stderr)

    return False


def _autofill_search_form(page, origin: str, destination: str, dep_date: str, adt: int) -> bool:
    """Fill the SalamAir search form and click Search.

    Operates in round-trip mode (the default) because the One Way radio button
    resists all programmatic click methods in React.  In round-trip mode the
    Search button stays disabled until BOTH departure and return dates are
    selected, so we pick a return date 30 days out (guaranteed next calendar
    month so the right calendar already shows it).

    Returns True if the Search button was successfully clicked.
    """
    from datetime import date as dt_date_cls, timedelta

    _dismiss_popups(page)
    page.wait_for_timeout(600)

    # Origin airport (react-select-2)
    _fill_react_select(page, "react-select-2-input", origin)
    page.wait_for_timeout(500)

    # Destination airport (react-select-3)
    _fill_react_select(page, "react-select-3-input", destination)
    page.wait_for_timeout(500)

    # Departure date — left calendar (calendar_idx=0)
    dep_ok = _select_datepicker_day(page, dep_date, calendar_idx=0)
    if not dep_ok:
        print(f"[warn] Could not click departure date {dep_date}", file=sys.stderr)
    page.wait_for_timeout(500)

    # Return date — 30 days after departure so it lands in the next calendar month
    # (right calendar in round-trip layout).  This unlocks the Search button.
    try:
        dep = dt_date_cls.fromisoformat(dep_date)
        ret_date = (dep + timedelta(days=30)).isoformat()
        ret_ok = _select_datepicker_day(page, ret_date, calendar_idx=1)
        if not ret_ok:
            print(f"[warn] Could not click return date {ret_date}", file=sys.stderr)
    except Exception as exc:
        print(f"[warn] Return date error: {exc}", file=sys.stderr)
    page.wait_for_timeout(500)

    # Click Search
    try:
        search_btn = page.query_selector("button.btn-submit")
        if search_btn and search_btn.is_visible():
            search_btn.click()
            return True
    except Exception:
        pass
    return False


def _adapt_trips_payload(trips_payload: Dict[str, Any], target_date: str) -> Dict[str, Any]:
    """Convert the new GET /api/flights?TripType=... response into the legacy
    {flights:[...]} shape that parse_flight_fares_payload already understands.

    The new response wraps flights in trips[0].markets[N] (one market per date,
    7-day window).  Segments and fareInfos fields are identical to the old format.
    """
    trips = trips_payload.get("trips") or []
    if not trips:
        return {"flights": []}
    markets = (trips[0] if isinstance(trips[0], dict) else {}).get("markets") or []
    target_prefix = str(target_date)[:10]
    matched: List[Dict[str, Any]] = []
    for market in markets:
        if not isinstance(market, dict):
            continue
        market_date = str(market.get("date") or "")[:10]
        if market_date == target_prefix:
            for flight in market.get("flights") or []:
                if isinstance(flight, dict):
                    matched.append(flight)
    # Currency: pull from the currencies list if present (SalamAir prices are in OMR)
    currencies = trips_payload.get("currencies") or []
    currency_code = "OMR"
    if isinstance(currencies, list) and currencies:
        first = currencies[0]
        if isinstance(first, str):
            currency_code = first
        elif isinstance(first, dict):
            currency_code = str(first.get("code") or "OMR")
    return {"flights": matched, "_currency": currency_code}


def _latest_matching(records: List[Dict[str, Any]], token: str) -> Optional[Dict[str, Any]]:
    for item in reversed(records):
        if token in str(item.get("url") or ""):
            return item
    return None


def _wait_for_capture(
    page, records: List[Dict[str, Any]], wait_seconds: float
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    deadline = time.time() + max(1.0, float(wait_seconds))
    while time.time() < deadline:
        fare_entry = _latest_matching(records, FLIGHT_FARES_URL_TOKEN)
        if fare_entry:
            confirm_entry = _latest_matching(records, CONFIRM_URL_TOKEN)
            return fare_entry, confirm_entry
        page.wait_for_timeout(250)
    return None, None


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Capture live SalamAir fare data via patchright (stealth) "
            "using native network interception instead of direct API calls."
        ),
    )
    parser.add_argument("--origin", required=True)
    parser.add_argument("--destination", required=True)
    parser.add_argument("--date", help="Single departure date YYYY-MM-DD")
    parser.add_argument("--dates", help="Comma-separated departure dates YYYY-MM-DD")
    parser.add_argument("--date-start", help="Inclusive departure date range start YYYY-MM-DD")
    parser.add_argument("--date-end", help="Inclusive departure date range end YYYY-MM-DD")
    parser.add_argument("--cabin", default="Economy")
    parser.add_argument("--adt", type=int, default=1)
    parser.add_argument("--chd", type=int, default=0)
    parser.add_argument("--inf", type=int, default=0)
    parser.add_argument("--session-root", default=str(DEFAULT_SESSION_ROOT))
    parser.add_argument("--user-data-dir", default=str(DEFAULT_SESSION_ROOT / "salamair_profile"))
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run headless (NOT recommended — site bot-detection is weaker without headless)",
    )
    parser.add_argument("--confirm-brand", default="Flexi", help="Fare brand to use when attempting confirm capture")
    parser.add_argument("--skip-confirm", action="store_true")
    parser.add_argument("--wait-seconds", type=float, default=60.0, help="Seconds to wait for API interception after form submit")
    parser.add_argument("--page-load-wait-ms", type=int, default=3000, help="Extra wait after page load (ms)")
    args = parser.parse_args()

    dates: List[str] = []
    if args.date:
        dates.extend(_parse_iso_dates([args.date]))
    if args.dates:
        dates.extend(_parse_iso_dates([piece.strip() for piece in str(args.dates).split(",")]))
    dates.extend(_expand_date_range(args.date_start, args.date_end))
    dates = _parse_iso_dates(dates)
    if not dates:
        raise SystemExit("Provide --date, --dates, or --date-start/--date-end")

    try:
        from patchright.sync_api import sync_playwright
    except ImportError:
        try:
            from playwright.sync_api import sync_playwright  # type: ignore[no-redef]
            print("[warn] patchright not available; falling back to standard playwright", file=sys.stderr)
        except Exception as exc:
            raise SystemExit(f"Neither patchright nor playwright found: {exc}")

    origin = str(args.origin or "").upper().strip()
    destination = str(args.destination or "").upper().strip()
    session_root = Path(args.session_root)
    results: List[Dict[str, Any]] = []

    with sync_playwright() as p:
        profile_dir = str(Path(args.user_data_dir).resolve())
        Path(profile_dir).mkdir(parents=True, exist_ok=True)

        context = p.chromium.launch_persistent_context(
            profile_dir,
            headless=bool(args.headless),
            args=["--disable-blink-features=AutomationControlled"],
            viewport={"width": 1280, "height": 900},
        )
        try:
            # One browser session; re-use page across dates so cookies persist.
            page = context.pages[0] if context.pages else context.new_page()
            page.set_default_timeout(90000)

            # Load the search page once to establish session cookies.
            records_global: List[Dict[str, Any]] = []

            def _on_response(resp) -> None:
                try:
                    url = str(resp.url or "")
                except Exception:
                    return
                if FLIGHT_FARES_URL_TOKEN not in url and CONFIRM_URL_TOKEN not in url:
                    return
                print(f"[intercept] {resp.status} {url}", file=sys.stderr)
                try:
                    body_text = resp.text()
                except Exception:
                    body_text = ""
                try:
                    request_post_data = resp.request.post_data or ""
                except Exception:
                    request_post_data = ""
                records_global.append(
                    {
                        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
                        "url": url,
                        "status": int(resp.status),
                        "ok": bool(resp.ok),
                        "request_body_json": _safe_json_or_none(request_post_data),
                        "response_body_json": _safe_json_or_none(body_text),
                    }
                )

            context.on("response", _on_response)
            page.goto(SEARCH_PAGE_URL, wait_until="domcontentloaded", timeout=90000)
            page.wait_for_timeout(args.page_load_wait_ms)

            for dep_date in dates:
                # Snapshot records count before this attempt so we only see
                # responses triggered by this particular search.
                pre_count = len(records_global)

                # Navigate back to search page between iterations.
                if page.url != SEARCH_PAGE_URL and "search" not in page.url:
                    page.goto(SEARCH_PAGE_URL, wait_until="domcontentloaded", timeout=90000)
                    page.wait_for_timeout(args.page_load_wait_ms)

                _autofill_search_form(page, origin, destination, dep_date, args.adt)

                # Wait for the intercepted flightFares response.
                fare_entry: Optional[Dict[str, Any]] = None
                confirm_entry: Optional[Dict[str, Any]] = None
                deadline = time.time() + args.wait_seconds
                while time.time() < deadline:
                    new_records = records_global[pre_count:]
                    fare_entry = _latest_matching(new_records, FLIGHT_FARES_URL_TOKEN)
                    if fare_entry:
                        confirm_entry = _latest_matching(new_records, CONFIRM_URL_TOKEN)
                        break
                    page.wait_for_timeout(300)

                if not fare_entry:
                    results.append({
                        "date": dep_date,
                        "ok": False,
                        "error": "flight_fares_not_intercepted",
                        "hint": (
                            "Form auto-fill did not trigger the flightFares API call. "
                            "Try running capture_salamair_manual.py interactively."
                        ),
                    })
                    continue

                fares_body = fare_entry.get("response_body_json")
                if not isinstance(fares_body, dict):
                    results.append({
                        "date": dep_date,
                        "ok": False,
                        "error": "fares_response_not_json",
                        "status": fare_entry.get("status"),
                    })
                    continue

                # Attempt confirm capture (if not skipped).
                confirm_body = None
                if not args.skip_confirm:
                    if isinstance(confirm_entry, dict):
                        confirm_body = confirm_entry.get("response_body_json")
                    if not isinstance(confirm_body, dict):
                        # The page may not have auto-clicked a fare; try to trigger
                        # confirm by clicking the first available fare option.
                        sell_key = _choose_confirm_sell_key(fares_body, args.confirm_brand)
                        if sell_key:
                            pre_confirm = len(records_global)
                            try:
                                fare_btn_selectors = [
                                    f'[data-sell-key="{sell_key}"]',
                                    'button[class*="fare" i]:first-of-type',
                                    'button[class*="select" i]:first-of-type',
                                ]
                                _try_click_button(page, fare_btn_selectors)
                                deadline_c = time.time() + 10.0
                                while time.time() < deadline_c:
                                    new_recs = records_global[pre_confirm:]
                                    ce = _latest_matching(new_recs, CONFIRM_URL_TOKEN)
                                    if ce:
                                        confirm_body = ce.get("response_body_json")
                                        break
                                    page.wait_for_timeout(300)
                            except Exception:
                                pass

                # The new GET /api/flights?TripType=... endpoint nests flights under
                # trips[0].markets[N]; adapt to the legacy {flights:[...]} shape.
                adapted_body = _adapt_trips_payload(fares_body, dep_date)
                currency_override = adapted_body.pop("_currency", "OMR")

                # Build a minimal confirm-like stub so parse_flight_fares_payload
                # picks up the currency code (OMR for SalamAir).
                if not isinstance(confirm_body, dict):
                    confirm_body = {"summary": {"currencyCode": currency_override}}

                rows = ov.parse_flight_fares_payload(
                    adapted_body,
                    requested_cabin=args.cabin,
                    adt=args.adt,
                    chd=args.chd,
                    inf=args.inf,
                    confirm_payload=confirm_body,
                )
                if not rows:
                    print(f"[warn] no_rows_parsed for {dep_date}; adapted_body flights={len(adapted_body.get('flights', []))}", file=sys.stderr)
                    results.append({"date": dep_date, "ok": False, "error": "no_rows_parsed"})
                    continue

                run_dir = session_root / "runs" / f"ov_{origin}_{destination}_{dep_date}_{_now_tag()}"
                fares_path = run_dir / "salamair_flight_fares_response.json"
                confirm_path = run_dir / "salamair_confirm_response.json"
                summary_path = run_dir / "salamair_capture_summary.json"
                rows_path = run_dir / "salamair_rows.json"

                _json_dump(fares_path, fares_body)
                _json_dump(rows_path, rows)
                if isinstance(confirm_body, dict):
                    _json_dump(confirm_path, confirm_body)

                summary = {
                    "captured_at_utc": datetime.now(timezone.utc).isoformat(),
                    "carrier": "OV",
                    "ok": True,
                    "source_type": "patchright_native_intercept",
                    "origin": origin,
                    "destination": destination,
                    "date": dep_date,
                    "cabin": args.cabin,
                    "adt": args.adt,
                    "chd": args.chd,
                    "inf": args.inf,
                    "search_page_url": SEARCH_PAGE_URL,
                    "flight_fares_request_body": fare_entry.get("request_body_json"),
                    "flight_fares_response_body_path": str(fares_path.resolve()),
                    "flight_fares_response_body": fares_body,
                    "confirm_response_body_path": str(confirm_path.resolve()) if isinstance(confirm_body, dict) else None,
                    "confirm_response_body": confirm_body,
                    "rows_path": str(rows_path.resolve()),
                    "rows_count": len(rows),
                    "sample_rows": rows[:3],
                }
                _json_dump(summary_path, summary)
                results.append(
                    {
                        "date": dep_date,
                        "ok": True,
                        "run_dir": str(run_dir.resolve()),
                        "summary_path": str(summary_path.resolve()),
                        "rows_count": len(rows),
                        "brands": [row.get("brand") for row in rows],
                    }
                )
        finally:
            try:
                context.close()
            except Exception:
                pass

    print(json.dumps(
        {"ok": True, "origin": origin, "destination": destination, "dates": dates, "results": results},
        indent=2, ensure_ascii=False, default=str,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
