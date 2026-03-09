from __future__ import annotations

import io
import json
from datetime import UTC, date, datetime
from typing import Any, Sequence

import pandas as pd
from sqlalchemy.orm import Session

from . import reporting


EXPORT_SECTION_ORDER = ("routes", "operations", "changes", "taxes", "penalties")


def _json_ready(value: Any) -> Any:
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=True)
    return value


def _rows_to_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    normalized = [{key: _json_ready(value) for key, value in row.items()} for row in rows]
    return pd.DataFrame(normalized)


def _flatten_route_monitor(payload: dict[str, Any]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for route in payload.get("routes", []):
        flight_lookup = {
            str(item.get("flight_group_id")): item
            for item in route.get("flight_groups", [])
        }
        for date_group in route.get("date_groups", []):
            for capture in date_group.get("captures", []):
                for cell in capture.get("cells", []):
                    flight = flight_lookup.get(str(cell.get("flight_group_id"))) or {}
                    rows.append(
                        {
                            "cycle_id": payload.get("cycle_id"),
                            "route_key": route.get("route_key"),
                            "origin": route.get("origin"),
                            "destination": route.get("destination"),
                            "route_type": route.get("route_type"),
                            "origin_country_code": route.get("origin_country_code"),
                            "destination_country_code": route.get("destination_country_code"),
                            "country_pair": route.get("country_pair"),
                            "domestic_country_code": route.get("domestic_country_code"),
                            "is_cross_border": route.get("is_cross_border"),
                            "search_trip_type": route.get("search_trip_type"),
                            "trip_pair_key": route.get("trip_pair_key"),
                            "trip_request_id": route.get("trip_request_id"),
                            "requested_outbound_date": route.get("requested_outbound_date"),
                            "requested_return_date": route.get("requested_return_date"),
                            "trip_duration_days": route.get("trip_duration_days"),
                            "trip_origin": route.get("trip_origin"),
                            "trip_destination": route.get("trip_destination"),
                            "departure_date": date_group.get("departure_date"),
                            "day_label": date_group.get("day_label"),
                            "captured_at_utc": capture.get("captured_at_utc"),
                            "is_latest_capture": capture.get("is_latest"),
                            "airline": flight.get("airline"),
                            "flight_number": flight.get("flight_number"),
                            "departure_time": flight.get("departure_time"),
                            "cabin": flight.get("cabin"),
                            "aircraft": flight.get("aircraft"),
                            "leg_direction": flight.get("leg_direction"),
                            "leg_sequence": flight.get("leg_sequence"),
                            "itinerary_leg_count": flight.get("itinerary_leg_count"),
                            "signal": cell.get("signal"),
                            "min_total_price_bdt": cell.get("min_total_price_bdt"),
                            "max_total_price_bdt": cell.get("max_total_price_bdt"),
                            "tax_amount": cell.get("tax_amount"),
                            "seat_available": cell.get("seat_available"),
                            "seat_capacity": cell.get("seat_capacity"),
                            "load_factor_pct": cell.get("load_factor_pct"),
                            "soldout": cell.get("soldout"),
                        }
                    )
    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame(rows)
    return frame.sort_values(
        by=["route_key", "departure_date", "captured_at_utc", "departure_time", "airline", "flight_number"],
        ascending=[True, True, False, True, True, True],
        na_position="last",
    )


def _metadata_sheet_rows(
    *,
    sections: Sequence[str],
    cycle_id: str | None,
    airlines: Sequence[str] | None,
    origins: Sequence[str] | None,
    destinations: Sequence[str] | None,
    route_types: Sequence[str] | None,
    trip_types: Sequence[str] | None,
    return_date: date | None,
    cabins: Sequence[str] | None,
    start_date: date | None,
    end_date: date | None,
    domains: Sequence[str] | None,
    change_types: Sequence[str] | None,
    directions: Sequence[str] | None,
    route_limit: int,
    history_limit: int,
    limit: int,
    section_row_counts: dict[str, int],
) -> list[dict[str, Any]]:
    filters = {
        "sections": ", ".join(sections),
        "cycle_id": cycle_id or "",
        "airlines": ", ".join(airlines or ()),
        "origins": ", ".join(origins or ()),
        "destinations": ", ".join(destinations or ()),
        "route_types": ", ".join(route_types or ()),
        "trip_types": ", ".join(trip_types or ()),
        "return_date": return_date.isoformat() if return_date else "",
        "cabins": ", ".join(cabins or ()),
        "start_date": start_date.isoformat() if start_date else "",
        "end_date": end_date.isoformat() if end_date else "",
        "domains": ", ".join(domains or ()),
        "change_types": ", ".join(change_types or ()),
        "directions": ", ".join(directions or ()),
        "route_limit": route_limit,
        "history_limit": history_limit,
        "limit": limit,
    }
    rows = [
        {"group": "export", "key": "generated_at_utc", "value": datetime.now(UTC).isoformat()},
        {"group": "export", "key": "workbook_type", "value": "operational_filter_export"},
    ]
    rows.extend(
        {"group": "filters", "key": key, "value": value}
        for key, value in filters.items()
    )
    rows.extend(
        {"group": "counts", "key": key, "value": value}
        for key, value in section_row_counts.items()
    )
    return rows


def build_reporting_workbook(
    session: Session | None,
    *,
    sections: Sequence[str],
    cycle_id: str | None = None,
    airlines: Sequence[str] | None = None,
    origins: Sequence[str] | None = None,
    destinations: Sequence[str] | None = None,
    route_types: Sequence[str] | None = None,
    trip_types: Sequence[str] | None = None,
    return_date: date | None = None,
    cabins: Sequence[str] | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    domains: Sequence[str] | None = None,
    change_types: Sequence[str] | None = None,
    directions: Sequence[str] | None = None,
    route_limit: int = 8,
    history_limit: int = 12,
    limit: int = 250,
) -> tuple[bytes, str]:
    normalized_sections = [item for item in EXPORT_SECTION_ORDER if item in set(sections)]
    if not normalized_sections:
        normalized_sections = list(EXPORT_SECTION_ORDER)

    workbook = io.BytesIO()
    section_row_counts: dict[str, int] = {}

    with pd.ExcelWriter(workbook, engine="openpyxl") as writer:
        if "routes" in normalized_sections:
            route_payload = reporting.get_route_monitor_matrix(
                session,
                cycle_id=cycle_id,
                airlines=airlines,
                origins=origins,
                destinations=destinations,
                cabins=cabins,
                trip_types=trip_types,
                return_date=return_date,
                route_limit=route_limit,
                history_limit=history_limit,
            )
            route_frame = _flatten_route_monitor(route_payload)
            section_row_counts["routes"] = int(len(route_frame))
            route_frame.to_excel(writer, index=False, sheet_name="Routes")

        if "operations" in normalized_sections:
            operations_payload = reporting.get_airline_operations(
                session,
                cycle_id=cycle_id,
                airlines=airlines,
                origins=origins,
                destinations=destinations,
                route_types=route_types,
                start_date=start_date,
                end_date=end_date,
                route_limit=route_limit,
                trend_limit=history_limit,
            )
            operations_rows: list[dict[str, Any]] = []
            for route in operations_payload.get("routes", []):
                for airline_entry in route.get("airlines", []):
                    operations_rows.append(
                        {
                            "cycle_id": operations_payload.get("cycle_id"),
                            "route_key": route.get("route_key"),
                            "origin": route.get("origin"),
                            "destination": route.get("destination"),
                            "route_type": route.get("route_type"),
                            "origin_country_code": route.get("origin_country_code"),
                            "destination_country_code": route.get("destination_country_code"),
                            "country_pair": route.get("country_pair"),
                            "airline": airline_entry.get("airline"),
                            "flight_instance_count": airline_entry.get("flight_instance_count"),
                            "active_date_count": airline_entry.get("active_date_count"),
                            "first_departure_time": airline_entry.get("first_departure_time"),
                            "last_departure_time": airline_entry.get("last_departure_time"),
                            "departure_times": airline_entry.get("departure_times"),
                            "flight_numbers": airline_entry.get("flight_numbers"),
                            "weekday_profile": airline_entry.get("weekday_profile"),
                            "timeline": airline_entry.get("timeline"),
                        }
                    )
            operations_frame = _rows_to_frame(operations_rows)
            section_row_counts["operations"] = int(len(operations_frame))
            operations_frame.to_excel(writer, index=False, sheet_name="Operations")

        if "changes" in normalized_sections:
            change_rows = reporting.get_change_events(
                session,
                start_date=start_date,
                end_date=end_date,
                airlines=airlines,
                origins=origins,
                destinations=destinations,
                domains=domains,
                change_types=change_types,
                directions=directions,
                limit=limit,
            )
            change_frame = _rows_to_frame(change_rows)
            section_row_counts["changes"] = int(len(change_frame))
            change_frame.to_excel(writer, index=False, sheet_name="Changes")

        if "taxes" in normalized_sections:
            tax_payload = reporting.get_taxes(
                session,
                cycle_id=cycle_id,
                airlines=airlines,
                origins=origins,
                destinations=destinations,
                limit=limit,
            )
            tax_frame = _rows_to_frame(tax_payload.get("rows", []))
            section_row_counts["taxes"] = int(len(tax_frame))
            tax_frame.to_excel(writer, index=False, sheet_name="Taxes")

        if "penalties" in normalized_sections:
            penalty_payload = reporting.get_penalties(
                session,
                cycle_id=cycle_id,
                airlines=airlines,
                origins=origins,
                destinations=destinations,
                limit=limit,
            )
            penalty_frame = _rows_to_frame(penalty_payload.get("rows", []))
            section_row_counts["penalties"] = int(len(penalty_frame))
            penalty_frame.to_excel(writer, index=False, sheet_name="Penalties")

        summary_rows = _metadata_sheet_rows(
            sections=normalized_sections,
            cycle_id=cycle_id,
            airlines=airlines,
            origins=origins,
            destinations=destinations,
            route_types=route_types,
            trip_types=trip_types,
            return_date=return_date,
            cabins=cabins,
            start_date=start_date,
            end_date=end_date,
            domains=domains,
            change_types=change_types,
            directions=directions,
            route_limit=route_limit,
            history_limit=history_limit,
            limit=limit,
            section_row_counts=section_row_counts,
        )
        pd.DataFrame(summary_rows).to_excel(writer, index=False, sheet_name="Summary")

    workbook.seek(0)
    stamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    section_token = "_".join(normalized_sections)
    filename = f"aero_pulse_export_{section_token}_{stamp}.xlsx"
    return workbook.getvalue(), filename
