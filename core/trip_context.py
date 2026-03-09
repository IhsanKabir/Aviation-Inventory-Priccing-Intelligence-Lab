from __future__ import annotations

import hashlib
import json
from datetime import date
from typing import Any


TRIP_TYPE_ONE_WAY = "OW"
TRIP_TYPE_ROUND_TRIP = "RT"


def normalize_trip_type(value: Any) -> str:
    normalized = str(value or TRIP_TYPE_ONE_WAY).strip().upper().replace("-", "_")
    if normalized in {"RT", "ROUNDTRIP", "ROUND_TRIP", "ROUNDTRIP"}:
        return TRIP_TYPE_ROUND_TRIP
    return TRIP_TYPE_ONE_WAY


def normalize_iso_date(value: Any) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    return date.fromisoformat(raw).isoformat()


def build_trip_context(
    *,
    origin: str,
    destination: str,
    departure_date: str,
    cabin: str,
    adt: int,
    chd: int,
    inf: int,
    trip_type: str = TRIP_TYPE_ONE_WAY,
    return_date: str | None = None,
) -> dict[str, Any]:
    normalized_trip_type = normalize_trip_type(trip_type)
    outbound_date = normalize_iso_date(departure_date)
    inbound_date = normalize_iso_date(return_date)
    if not outbound_date:
        raise ValueError("departure_date is required")
    if normalized_trip_type == TRIP_TYPE_ROUND_TRIP and not inbound_date:
        raise ValueError("return_date is required for round-trip searches")
    if normalized_trip_type == TRIP_TYPE_ONE_WAY:
        inbound_date = None

    trip_duration_days = None
    if outbound_date and inbound_date:
        duration = (date.fromisoformat(inbound_date) - date.fromisoformat(outbound_date)).days
        if duration < 0:
            raise ValueError("return_date cannot be earlier than departure_date")
        trip_duration_days = duration

    fingerprint_payload = {
        "trip_type": normalized_trip_type,
        "origin": str(origin or "").strip().upper(),
        "destination": str(destination or "").strip().upper(),
        "departure_date": outbound_date,
        "return_date": inbound_date,
        "cabin": str(cabin or "").strip(),
        "adt": max(1, int(adt or 1)),
        "chd": max(0, int(chd or 0)),
        "inf": max(0, int(inf or 0)),
    }
    trip_request_id = hashlib.sha256(
        json.dumps(fingerprint_payload, sort_keys=True, ensure_ascii=True).encode("utf-8")
    ).hexdigest()[:24]

    return {
        "trip_request_id": trip_request_id,
        "search_trip_type": normalized_trip_type,
        "requested_outbound_date": outbound_date,
        "requested_return_date": inbound_date,
        "trip_duration_days": trip_duration_days,
        "trip_origin": fingerprint_payload["origin"],
        "trip_destination": fingerprint_payload["destination"],
    }


def apply_trip_context(row: dict[str, Any], trip_context: dict[str, Any]) -> dict[str, Any]:
    enriched = dict(row)
    for key, value in trip_context.items():
        enriched.setdefault(key, value)

    direction = str(enriched.get("leg_direction") or "").strip().lower()
    if direction in {"return"}:
        direction = "inbound"
    if not direction:
        direction = "outbound"
    enriched["leg_direction"] = direction

    if enriched.get("leg_sequence") is None:
        if direction == "outbound":
            enriched["leg_sequence"] = 1
        elif direction == "inbound":
            enriched["leg_sequence"] = 2

    if enriched.get("itinerary_leg_count") is None:
        enriched["itinerary_leg_count"] = 2 if trip_context.get("search_trip_type") == TRIP_TYPE_ROUND_TRIP else 1

    return enriched
