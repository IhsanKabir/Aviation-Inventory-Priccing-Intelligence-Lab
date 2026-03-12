import json
import unittest
from pathlib import Path
from uuid import uuid4

from tools.validate_trip_config import validate_trip_config


class ValidateTripConfigTests(unittest.TestCase):
    def test_current_trip_config_validates_without_errors(self):
        route_trip_payload = json.loads(Path("config/route_trip_windows.json").read_text(encoding="utf-8"))
        market_priors_payload = json.loads(Path("config/market_priors.json").read_text(encoding="utf-8"))
        routes_payload = json.loads(Path("config/routes.json").read_text(encoding="utf-8"))

        warnings, errors, summary = validate_trip_config(
            route_trip_payload=route_trip_payload,
            market_priors_payload=market_priors_payload,
            routes_payload=routes_payload,
        )

        self.assertEqual([], errors)
        self.assertGreater(summary["configured_route_count"], 0)

    def test_unknown_profile_reference_is_reported(self):
        route_trip_payload = {
            "profiles": {"ow_default": {"trip_type": "OW"}},
            "airlines": {
                "BG": {
                    "routes": {
                        "DAC-CXB": {
                            "active_market_trip_profiles": ["missing_profile"],
                        }
                    }
                }
            },
        }
        market_priors_payload = {"trip_date_profiles": {"known_profile": {"trip_type": "OW"}}}
        routes_payload = [{"airline": "BG", "origin": "DAC", "destination": "CXB", "cabins": ["Economy"]}]

        warnings, errors, _summary = validate_trip_config(
            route_trip_payload=route_trip_payload,
            market_priors_payload=market_priors_payload,
            routes_payload=routes_payload,
        )

        self.assertEqual([], warnings)
        self.assertEqual(
            ["BG:DAC-CXB: unknown profile 'missing_profile' referenced in active_market_trip_profiles"],
            errors,
        )

    def test_unknown_route_reference_is_reported(self):
        route_trip_payload = {
            "airlines": {
                "BG": {
                    "routes": {
                        "DAC-XYZ": {
                            "active_market_trip_profiles": ["known_profile"],
                        }
                    }
                }
            }
        }
        market_priors_payload = {"trip_date_profiles": {"known_profile": {"trip_type": "OW"}}}
        routes_payload = [{"airline": "BG", "origin": "DAC", "destination": "CXB", "cabins": ["Economy"]}]

        warnings, errors, _summary = validate_trip_config(
            route_trip_payload=route_trip_payload,
            market_priors_payload=market_priors_payload,
            routes_payload=routes_payload,
        )

        self.assertEqual(["BG:DAC-CXB: present in config/routes.json but missing from config/route_trip_windows.json"], warnings)
        self.assertEqual(["BG:DAC-XYZ: route not found in config/routes.json"], errors)


if __name__ == "__main__":
    unittest.main()
