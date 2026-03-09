import unittest
import sys
import types


google_module = types.ModuleType("google")
google_api_core = types.ModuleType("google.api_core")
google_api_core_exceptions = types.ModuleType("google.api_core.exceptions")
google_cloud = types.ModuleType("google.cloud")
google_bigquery = types.ModuleType("google.cloud.bigquery")


class _GoogleAPIError(Exception):
    pass


class _Client:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


google_api_core_exceptions.GoogleAPIError = _GoogleAPIError
google_bigquery.Client = _Client
google_bigquery.ScalarQueryParameter = object
google_bigquery.ArrayQueryParameter = object
google_bigquery.QueryJobConfig = object

google_module.api_core = google_api_core
google_api_core.exceptions = google_api_core_exceptions
google_module.cloud = google_cloud
google_cloud.bigquery = google_bigquery

sys.modules.setdefault("google", google_module)
sys.modules.setdefault("google.api_core", google_api_core)
sys.modules.setdefault("google.api_core.exceptions", google_api_core_exceptions)
sys.modules.setdefault("google.cloud", google_cloud)
sys.modules.setdefault("google.cloud.bigquery", google_bigquery)

from apps.api.app.repositories import reporting


class RouteClassificationTests(unittest.TestCase):
    def test_domestic_route_uses_same_country_airports(self):
        payload = reporting._classify_route("DAC", "CXB")

        self.assertEqual("BD", payload["origin_country_code"])
        self.assertEqual("BD", payload["destination_country_code"])
        self.assertEqual("BD-BD", payload["country_pair"])
        self.assertEqual("DOM", payload["route_type"])
        self.assertEqual("BD", payload["domestic_country_code"])
        self.assertFalse(payload["is_cross_border"])

    def test_international_route_uses_cross_border_airports(self):
        payload = reporting._classify_route("DAC", "DXB")

        self.assertEqual("BD", payload["origin_country_code"])
        self.assertEqual("AE", payload["destination_country_code"])
        self.assertEqual("BD-AE", payload["country_pair"])
        self.assertEqual("INT", payload["route_type"])
        self.assertIsNone(payload["domestic_country_code"])
        self.assertTrue(payload["is_cross_border"])

    def test_route_monitor_payload_includes_route_type_metadata(self):
        payload = reporting._build_route_monitor_matrix_from_aggregates(
            resolved_cycle_id="cycle-1",
            selected_routes=[
                {
                    "route_key": "DAC-CXB",
                    "origin": "DAC",
                    "destination": "CXB",
                }
            ],
            current_rows=[
                {
                    "route_key": "DAC-CXB",
                    "origin": "DAC",
                    "destination": "CXB",
                    "captured_at_utc": "2026-03-09T06:00:00+00:00",
                    "airline": "BG",
                    "flight_number": "BG121",
                    "departure_date": "2026-03-10",
                    "departure_time": "08:00",
                    "cabin": "Economy",
                    "aircraft": "Dash 8",
                    "min_total_price_bdt": 5400,
                    "max_total_price_bdt": 5400,
                    "tax_amount": 600,
                    "seat_available": 9,
                    "seat_capacity": 74,
                    "load_factor_pct": 87.8,
                    "soldout": False,
                }
            ],
            history_rows=[
                {
                    "route_key": "DAC-CXB",
                    "origin": "DAC",
                    "destination": "CXB",
                    "captured_at_utc": "2026-03-09T06:00:00+00:00",
                    "airline": "BG",
                    "flight_number": "BG121",
                    "departure_date": "2026-03-10",
                    "departure_time": "08:00",
                    "cabin": "Economy",
                    "aircraft": "Dash 8",
                    "min_total_price_bdt": 5400,
                    "max_total_price_bdt": 5400,
                    "tax_amount": 600,
                    "seat_available": 9,
                    "seat_capacity": 74,
                    "load_factor_pct": 87.8,
                    "soldout": False,
                }
            ],
            history_limit=4,
        )

        self.assertEqual("cycle-1", payload["cycle_id"])
        self.assertEqual(1, len(payload["routes"]))
        self.assertEqual("DOM", payload["routes"][0]["route_type"])
        self.assertEqual("BD-BD", payload["routes"][0]["country_pair"])

    def test_route_monitor_payload_preserves_round_trip_metadata(self):
        payload = reporting._build_route_monitor_matrix_from_aggregates(
            resolved_cycle_id="cycle-rt",
            selected_routes=[
                {
                    "route_key": "DAC-DXB",
                    "origin": "DAC",
                    "destination": "DXB",
                }
            ],
            current_rows=[
                {
                    "route_key": "DAC-DXB",
                    "origin": "DAC",
                    "destination": "DXB",
                    "captured_at_utc": "2026-03-09T06:00:00+00:00",
                    "airline": "BG",
                    "flight_number": "BG047",
                    "departure_date": "2026-03-10",
                    "departure_time": "09:30",
                    "cabin": "Economy",
                    "aircraft": "787",
                    "search_trip_type": "RT",
                    "trip_pair_key": "DAC-DXB",
                    "trip_request_id": "trip-1",
                    "requested_outbound_date": "2026-03-10",
                    "requested_return_date": "2026-03-15",
                    "trip_duration_days": 5,
                    "trip_origin": "DAC",
                    "trip_destination": "DXB",
                    "leg_direction": "outbound",
                    "leg_sequence": 1,
                    "itinerary_leg_count": 2,
                    "min_total_price_bdt": 45000,
                    "max_total_price_bdt": 45000,
                    "tax_amount": 7200,
                    "seat_available": 4,
                    "seat_capacity": 271,
                    "load_factor_pct": 93.1,
                    "soldout": False,
                }
            ],
            history_rows=[
                {
                    "route_key": "DAC-DXB",
                    "origin": "DAC",
                    "destination": "DXB",
                    "captured_at_utc": "2026-03-09T06:00:00+00:00",
                    "airline": "BG",
                    "flight_number": "BG047",
                    "departure_date": "2026-03-10",
                    "departure_time": "09:30",
                    "cabin": "Economy",
                    "aircraft": "787",
                    "search_trip_type": "RT",
                    "trip_pair_key": "DAC-DXB",
                    "trip_request_id": "trip-1",
                    "requested_outbound_date": "2026-03-10",
                    "requested_return_date": "2026-03-15",
                    "trip_duration_days": 5,
                    "trip_origin": "DAC",
                    "trip_destination": "DXB",
                    "leg_direction": "outbound",
                    "leg_sequence": 1,
                    "itinerary_leg_count": 2,
                    "min_total_price_bdt": 45000,
                    "max_total_price_bdt": 45000,
                    "tax_amount": 7200,
                    "seat_available": 4,
                    "seat_capacity": 271,
                    "load_factor_pct": 93.1,
                    "soldout": False,
                }
            ],
            history_limit=4,
        )

        route = payload["routes"][0]
        self.assertEqual("RT", route["search_trip_type"])
        self.assertEqual("DAC-DXB", route["trip_pair_key"])
        self.assertEqual("2026-03-15", route["requested_return_date"])
        self.assertEqual("outbound", route["flight_groups"][0]["leg_direction"])
        self.assertEqual(2, route["flight_groups"][0]["itinerary_leg_count"])

    def test_airline_operations_payload_groups_airlines_and_timeline(self):
        payload = reporting._build_airline_operations_payload(
            resolved_cycle_id="cycle-2",
            selected_routes=[
                {
                    "route_key": "DAC-DXB",
                    "origin": "DAC",
                    "destination": "DXB",
                    "route_type": "INT",
                    "origin_country_code": "BD",
                    "destination_country_code": "AE",
                    "country_pair": "BD-AE",
                }
            ],
            current_rows=[
                {
                    "route_key": "DAC-DXB",
                    "origin": "DAC",
                    "destination": "DXB",
                    "airline": "BG",
                    "flight_number": "BG047",
                    "departure_date": "2026-03-10",
                    "departure_time": "09:30",
                },
                {
                    "route_key": "DAC-DXB",
                    "origin": "DAC",
                    "destination": "DXB",
                    "airline": "BG",
                    "flight_number": "BG049",
                    "departure_date": "2026-03-11",
                    "departure_time": "14:20",
                },
                {
                    "route_key": "DAC-DXB",
                    "origin": "DAC",
                    "destination": "DXB",
                    "airline": "EK",
                    "flight_number": "EK587",
                    "departure_date": "2026-03-10",
                    "departure_time": "19:45",
                },
            ],
            trend_route_rows=[
                {
                    "cycle_id": "cycle-1",
                    "route_key": "DAC-DXB",
                    "flight_instance_count": 2,
                    "active_date_count": 1,
                    "airline_count": 2,
                    "first_departure_time": "09:30",
                    "last_departure_time": "19:45",
                },
                {
                    "cycle_id": "cycle-2",
                    "route_key": "DAC-DXB",
                    "flight_instance_count": 3,
                    "active_date_count": 2,
                    "airline_count": 2,
                    "first_departure_time": "09:30",
                    "last_departure_time": "19:45",
                },
            ],
            trend_airline_rows=[
                {
                    "cycle_id": "cycle-1",
                    "route_key": "DAC-DXB",
                    "airline": "BG",
                    "flight_instance_count": 1,
                    "active_date_count": 1,
                    "first_departure_time": "09:30",
                    "last_departure_time": "09:30",
                },
                {
                    "cycle_id": "cycle-2",
                    "route_key": "DAC-DXB",
                    "airline": "BG",
                    "flight_instance_count": 2,
                    "active_date_count": 2,
                    "first_departure_time": "09:30",
                    "last_departure_time": "14:20",
                },
            ],
            recent_cycles=[
                {"cycle_id": "cycle-1", "cycle_completed_at_utc": "2026-03-08T06:00:00+00:00"},
                {"cycle_id": "cycle-2", "cycle_completed_at_utc": "2026-03-09T06:00:00+00:00"},
            ],
        )

        self.assertEqual("cycle-2", payload["cycle_id"])
        self.assertEqual(1, len(payload["routes"]))
        route = payload["routes"][0]
        self.assertEqual("INT", route["route_type"])
        self.assertEqual(2, route["airline_count"])
        self.assertEqual(3, route["flight_instance_count"])
        self.assertEqual("09:30", route["first_departure_time"])
        self.assertEqual("19:45", route["last_departure_time"])
        self.assertEqual(2, len(route["timeline"]))
        self.assertEqual(2, len(route["airlines"]))
        self.assertEqual("BG", route["airlines"][0]["airline"])
        self.assertEqual(2, len(route["airlines"][0]["timeline"]))

    def test_tax_monitor_payload_adds_route_and_airline_trends(self):
        payload = reporting._build_tax_monitor_payload(
            resolved_cycle_id="cycle-2",
            detail_rows=[
                {
                    "route_key": "DAC-DOH",
                    "origin": "DAC",
                    "destination": "DOH",
                    "airline": "BG",
                    "tax_amount": 6200,
                }
            ],
            route_summaries=[
                {
                    "route_key": "DAC-DOH",
                    "origin": "DAC",
                    "destination": "DOH",
                    "avg_tax_amount": 6200,
                    "spread_amount": 900,
                }
            ],
            airline_summaries=[
                {
                    "route_key": "DAC-DOH",
                    "origin": "DAC",
                    "destination": "DOH",
                    "airline": "BG",
                    "avg_tax_amount": 6200,
                    "spread_amount": 900,
                }
            ],
            route_trend_rows=[
                {
                    "cycle_id": "cycle-1",
                    "route_key": "DAC-DOH",
                    "origin": "DAC",
                    "destination": "DOH",
                    "avg_tax_amount": 5600,
                    "spread_amount": 700,
                },
                {
                    "cycle_id": "cycle-2",
                    "route_key": "DAC-DOH",
                    "origin": "DAC",
                    "destination": "DOH",
                    "avg_tax_amount": 6200,
                    "spread_amount": 900,
                },
            ],
            airline_trend_rows=[
                {
                    "cycle_id": "cycle-1",
                    "route_key": "DAC-DOH",
                    "origin": "DAC",
                    "destination": "DOH",
                    "airline": "BG",
                    "avg_tax_amount": 5600,
                    "spread_amount": 700,
                },
                {
                    "cycle_id": "cycle-2",
                    "route_key": "DAC-DOH",
                    "origin": "DAC",
                    "destination": "DOH",
                    "airline": "BG",
                    "avg_tax_amount": 6200,
                    "spread_amount": 900,
                },
            ],
            recent_cycles=[
                {"cycle_id": "cycle-1", "cycle_completed_at_utc": "2026-03-08T06:00:00+00:00"},
                {"cycle_id": "cycle-2", "cycle_completed_at_utc": "2026-03-09T06:00:00+00:00"},
            ],
        )

        self.assertEqual("cycle-2", payload["cycle_id"])
        self.assertEqual(1, len(payload["rows"]))
        self.assertEqual(1, len(payload["route_summaries"]))
        self.assertEqual(1, len(payload["airline_summaries"]))
        self.assertEqual(600.0, payload["route_summaries"][0]["avg_tax_change_amount"])
        self.assertEqual(600.0, payload["airline_summaries"][0]["avg_tax_change_amount"])
        self.assertEqual(2, len(payload["airline_summaries"][0]["timeline"]))

    def test_change_dashboard_payload_adds_display_names_and_route_metadata(self):
        payload = reporting._build_change_dashboard_payload(
            summary_row={
                "event_count": 12,
                "route_count": 2,
                "airline_count": 3,
                "latest_event_at_utc": "2026-03-09T06:00:00+00:00",
                "up_count": 7,
                "down_count": 3,
            },
            daily_rows=[
                {
                    "report_day": "2026-03-08",
                    "event_count": 5,
                }
            ],
            route_rows=[
                {
                    "route_key": "DAC-CXB",
                    "origin": "DAC",
                    "destination": "CXB",
                    "event_count": 4,
                }
            ],
            airline_rows=[
                {
                    "airline": "BG",
                    "event_count": 6,
                }
            ],
            domain_rows=[
                {
                    "domain": "price",
                    "event_count": 7,
                }
            ],
            field_rows=[
                {
                    "field_name": "total_price_bdt",
                    "event_count": 7,
                }
            ],
            largest_moves=[
                {
                    "route_key": "DAC-CXB",
                    "origin": "DAC",
                    "destination": "CXB",
                    "airline": "BG",
                    "field_name": "total_price_bdt",
                    "magnitude": 500,
                }
            ],
        )

        self.assertEqual(12, payload["summary"]["event_count"])
        self.assertEqual("Tax amount", reporting._display_change_field_name("tax_amount"))
        self.assertEqual("Total price", payload["field_mix"][0]["display_name"])
        self.assertEqual("DOM", payload["top_routes"][0]["route_type"])
        self.assertEqual("BD-BD", payload["largest_moves"][0]["country_pair"])


if __name__ == "__main__":
    unittest.main()
