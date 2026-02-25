import unittest
from pathlib import Path

from modules.ttinteractive_flexible_html_parser import (
    extract_flexible_fares_from_html,
    extract_flexible_fares_from_search_body,
)


FIXTURE_HTML = (
    Path(__file__).parent
    / "fixtures"
    / "ttinteractive_usbangla_flexible_selected_days_entry60.html"
)


def _minimal_bs_cfg():
    # HAR fixture rows use TTInteractive airport DataIds 6707 (DAC) and 6658 (CXB).
    return {
        "sourceData": {
            "Configuration": {
                "Airports": {
                    "DAC": {"DataId": 6707},
                    "CXB": {"DataId": 6658},
                }
            }
        }
    }


class TTInteractiveFlexibleParserTests(unittest.TestCase):
    maxDiff = None

    def test_har_selected_days_html_extracts_fares(self):
        html_text = FIXTURE_HTML.read_text(encoding="utf-8", errors="replace")
        rows = extract_flexible_fares_from_html(
            html_text,
            config=_minimal_bs_cfg(),
            airline_code="BS",
            requested_cabin="Economy",
            adt=1,
            chd=0,
            inf=0,
        )

        self.assertEqual(16, len(rows))

        required = {"airline", "flight_number", "origin", "destination", "departure", "cabin", "brand"}
        for row in rows:
            self.assertTrue(required.issubset(row.keys()))
            self.assertEqual("BS", row["airline"])
            self.assertEqual("DAC", row["origin"])
            self.assertEqual("CXB", row["destination"])
            self.assertEqual("Economy", row["cabin"])
            self.assertIsNotNone(row["price_total_bdt"])

        by_key = {
            (r["flight_number"], r["brand"], r["fare_basis"]): r
            for r in rows
        }
        self.assertIn(("141", "Economy Lite", "GC20"), by_key)
        self.assertEqual(5049.0, by_key[("141", "Economy Lite", "GC20")]["price_total_bdt"])
        self.assertEqual("2026-03-10T07:45:00", by_key[("141", "Economy Lite", "GC20")]["departure"])

        self.assertIn(("141", "Economy Saver", "GC15"), by_key)
        self.assertEqual(5, by_key[("141", "Economy Saver", "GC15")]["seat_available"])
        self.assertEqual("reported", by_key[("141", "Economy Saver", "GC15")]["inventory_confidence"])

    def test_nested_search_body_wrapper_is_supported(self):
        html_text = FIXTURE_HTML.read_text(encoding="utf-8", errors="replace")
        body = {
            "ok": True,
            "viewModel": {
                "selectedDayHtml": html_text,
            },
        }
        rows = extract_flexible_fares_from_search_body(
            body,
            config=_minimal_bs_cfg(),
            airline_code="BS",
            requested_cabin="Economy",
        )
        self.assertEqual(16, len(rows))


if __name__ == "__main__":
    unittest.main()
