from __future__ import annotations

import html
import json
import re
from typing import Any, Dict, List, Optional


_ARTICLE_RE = re.compile(
    r"<article\b(?=[^>]*\bflight-tariff\b)(?=[^>]*\bdata-selectfare='([^']+)')[^>]*?(?:\bdata-genericclass=\"([^\"]*)\")?[^>]*>(.*?)</article>",
    re.I | re.S,
)

_BRAND_RE = re.compile(r"<h4[^>]*class=\"[^\"]*ffs-type[^\"]*\"[^>]*>(.*?)</h4>", re.I | re.S)
_PRICE_DOLLARS_RE = re.compile(r"<div[^>]*class=\"[^\"]*dollars font-xl[^\"]*\"[^>]*>\s*([0-9][0-9,]*)\s*</div>", re.I | re.S)
_PRICE_ARIA_RE = re.compile(r"aria-label=\"[^\"]*?([0-9][0-9,]*)\"", re.I | re.S)
_AVAILABILITY_BLOCK_RE = re.compile(r"<p[^>]*class=\"[^\"]*fps-availability[^\"]*\"[^>]*>(.*?)</p>", re.I | re.S)
_SEATS_RE = re.compile(r"(\d+)\s*seat\(s\)\s*remaining", re.I)
_TAG_RE = re.compile(r"<[^>]+>")


def _strip_tags(text: str) -> str:
    if not text:
        return ""
    text = _TAG_RE.sub(" ", text)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _safe_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        try:
            s = str(value).strip()
            if not s:
                return None
            return int(s)
        except Exception:
            return None


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        try:
            s = str(value).replace(",", "").strip()
            if not s:
                return None
            return float(s)
        except Exception:
            return None


def _airport_id_to_code_map(config: Optional[Dict[str, Any]]) -> Dict[int, str]:
    airports = (
        (config or {}).get("sourceData", {})
        .get("Configuration", {})
        .get("Airports", {})
    )
    out: Dict[int, str] = {}
    for code, meta in (airports or {}).items():
        if not isinstance(meta, dict):
            continue
        data_id = _safe_int(meta.get("DataId"))
        if data_id is None:
            continue
        out[data_id] = str(code).upper().strip()
    return out


def _brand_to_cabin(brand: str, requested_cabin: str) -> str:
    b = (brand or "").lower()
    if "business" in b:
        return "Business"
    if "premium" in b:
        return "Premium Economy"
    if "economy" in b:
        return "Economy"
    return requested_cabin or "Economy"


def _parse_price(article_html: str) -> Optional[float]:
    m = _PRICE_DOLLARS_RE.search(article_html or "")
    if m:
        return _safe_float(m.group(1))
    m = _PRICE_ARIA_RE.search(article_html or "")
    if m:
        return _safe_float(m.group(1))
    return None


def _parse_seats_remaining(article_html: str) -> Optional[int]:
    m = _AVAILABILITY_BLOCK_RE.search(article_html or "")
    if not m:
        return None
    text = _strip_tags(m.group(1))
    m2 = _SEATS_RE.search(text)
    if not m2:
        return None
    return _safe_int(m2.group(1))


def _parse_availability_text(article_html: str) -> str:
    m = _AVAILABILITY_BLOCK_RE.search(article_html or "")
    if not m:
        return ""
    return _strip_tags(m.group(1))


def _extract_first_selected_segment(payload: Dict[str, Any]) -> Dict[str, Any]:
    user_sel = (payload.get("UserSelections") or [])
    if not user_sel or not isinstance(user_sel[0], dict):
        return {}
    segments = user_sel[0].get("SelectedSegments") or []
    if not segments or not isinstance(segments[0], dict):
        return {}
    return segments[0]


def _extract_user_selection(payload: Dict[str, Any]) -> Dict[str, Any]:
    user_sel = (payload.get("UserSelections") or [])
    if user_sel and isinstance(user_sel[0], dict):
        return user_sel[0]
    return {}


def extract_flexible_fares_from_html(
    html_text: Any,
    *,
    config: Optional[Dict[str, Any]],
    airline_code: str,
    requested_cabin: str = "Economy",
    adt: int = 1,
    chd: int = 0,
    inf: int = 0,
    source_endpoint: str = "FlexibleFlightStaticAjax/FlexibleFlightListLoadSelectedDays",
) -> List[Dict[str, Any]]:
    if not isinstance(html_text, str):
        return []
    if "flight-tariff" not in html_text or "data-selectfare" not in html_text:
        return []

    airport_map = _airport_id_to_code_map(config)
    airline_code = str(airline_code or "").upper().strip()
    rows: List[Dict[str, Any]] = []
    seen_keys = set()

    for m in _ARTICLE_RE.finditer(html_text):
        selectfare_raw = html.unescape(m.group(1) or "")
        generic_class_attr = (m.group(2) or "").strip()
        article_html = m.group(3) or ""

        try:
            payload = json.loads(selectfare_raw)
        except Exception:
            continue

        user_sel = _extract_user_selection(payload)
        seg = _extract_first_selected_segment(payload)
        if not seg:
            continue

        carrier = str(seg.get("AirlineDesignator") or airline_code).upper().strip()
        if airline_code and carrier and carrier != airline_code:
            continue

        flight_number = seg.get("FlightNumber")
        flight_number_s = str(flight_number).strip() if flight_number is not None else None
        departure = str(seg.get("DepartureDateTime") or "").strip() or None

        data_id_origin = _safe_int(user_sel.get("DataIdOrigin") or seg.get("DataIdOrigin"))
        data_id_dest = _safe_int(user_sel.get("DataIdDestination") or seg.get("DataIdDestination"))
        origin = airport_map.get(data_id_origin or -1)
        destination = airport_map.get(data_id_dest or -1)

        brand_match = _BRAND_RE.search(article_html)
        brand = _strip_tags(brand_match.group(1)) if brand_match else ""

        generic_class_id = _safe_int(user_sel.get("GenericClassDataId"))
        if generic_class_id is None:
            generic_class_id = _safe_int(generic_class_attr)
        fare_basis = f"GC{generic_class_id}" if generic_class_id is not None else "GC"

        price_total = _parse_price(article_html)
        availability_text = _parse_availability_text(article_html)
        seats_remaining = _parse_seats_remaining(article_html)
        soldout = ("sold out" in availability_text.lower()) or (price_total is None)

        currency = str(payload.get("CurrencyCode") or "BDT").upper().strip() or "BDT"
        brand_final = brand or fare_basis
        cabin = _brand_to_cabin(brand_final, requested_cabin)

        # Minimum identity for ingestion; rows without these will be dropped by run_all anyway.
        if not (carrier and flight_number_s and origin and destination and departure and cabin and brand_final):
            continue

        dedupe_key = (carrier, flight_number_s, departure, fare_basis, brand_final)
        if dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)

        row = {
            "airline": carrier,
            "flight_number": flight_number_s,
            "origin": origin,
            "destination": destination,
            "departure": departure,
            "arrival": None,
            "aircraft": None,
            "equipment_code": None,
            "duration_min": None,
            "stops": None,
            "cabin": cabin,
            "brand": brand_final,
            "fare_basis": fare_basis,
            "booking_class": fare_basis,
            "currency": currency,
            "fare_amount": price_total,
            "tax_amount": None,
            "total_amount": price_total,
            "price_total_bdt": _safe_float(price_total) if currency == "BDT" else None,
            "seats_remaining": seats_remaining,
            "seat_available": seats_remaining,
            "inventory_confidence": "reported" if seats_remaining is not None else "unknown",
            "soldout": soldout,
            "baggage": None,
            "adt_count": max(0, int(adt or 0)),
            "chd_count": max(0, int(chd or 0)),
            "inf_count": max(0, int(inf or 0)),
            "source_endpoint": source_endpoint,
            "search_date": (str(user_sel.get("SelectedDate") or "")[:10] or None),
            "raw_offer": {
                "source": "ttinteractive_flexible_html",
                "generic_class_id": generic_class_id,
                "availability_text": availability_text or None,
                "selectfare": payload,
            },
        }
        rows.append(row)

    return rows


def extract_flexible_fares_from_search_body(
    search_body: Any,
    *,
    config: Optional[Dict[str, Any]],
    airline_code: str,
    requested_cabin: str = "Economy",
    adt: int = 1,
    chd: int = 0,
    inf: int = 0,
    source_endpoint: str = "FlexibleFlightStaticAjax/FlexibleFlightListLoadSelectedDays",
) -> List[Dict[str, Any]]:
    """
    Best-effort extractor for TTInteractive responses.

    The fare HTML may arrive as:
    - a raw HTML string (AJAX endpoint body)
    - nested string values inside a JSON/dict payload
    - list/dict wrappers with multiple snippets (day resumes + selected day details)
    """
    best_rows: List[Dict[str, Any]] = []
    stack: List[Any] = [search_body]
    seen_container_ids: set[int] = set()

    while stack:
        current = stack.pop()
        if isinstance(current, str):
            rows = extract_flexible_fares_from_html(
                current,
                config=config,
                airline_code=airline_code,
                requested_cabin=requested_cabin,
                adt=adt,
                chd=chd,
                inf=inf,
                source_endpoint=source_endpoint,
            )
            if len(rows) > len(best_rows):
                best_rows = rows
            continue

        if isinstance(current, dict):
            container_id = id(current)
            if container_id in seen_container_ids:
                continue
            seen_container_ids.add(container_id)
            for value in current.values():
                if isinstance(value, (str, dict, list)):
                    stack.append(value)
            continue

        if isinstance(current, list):
            container_id = id(current)
            if container_id in seen_container_ids:
                continue
            seen_container_ids.add(container_id)
            for value in current:
                if isinstance(value, (str, dict, list)):
                    stack.append(value)

    return best_rows
