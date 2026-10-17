import os
import unittest
from unittest.mock import patch

from config import (
    get_catalog_from_valve_charts,
    get_details_refresh_days,
    get_refresh_max_requests,
    get_review_summaries_per_run,
    get_reviews_refresh_days,
    get_steamspy_page_interval_days,
    get_steamspy_pages,
    get_store_details_per_run,
    get_tag_refresh_days,
)


class CatalogSettingsTests(unittest.TestCase):
    def test_defaults(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertTrue(get_catalog_from_valve_charts())
            self.assertEqual(get_steamspy_page_interval_days(), 7)
            self.assertEqual(get_steamspy_pages(), 5)
            self.assertEqual(get_tag_refresh_days(), 30)
            self.assertEqual(get_details_refresh_days(), 7)
            self.assertEqual(get_reviews_refresh_days(), 1)
            self.assertEqual(get_refresh_max_requests(), 300)
            # Per-run counts are derived unless overridden (ADR 6).
            self.assertIsNone(get_store_details_per_run())
            self.assertIsNone(get_review_summaries_per_run())

    def test_empty_overrides_count_as_unset(self):
        with patch.dict(os.environ, {"STORE_DETAILS_PER_RUN": "",
                                     "REVIEW_SUMMARIES_PER_RUN": " "}):
            self.assertIsNone(get_store_details_per_run())
            self.assertIsNone(get_review_summaries_per_run())

    def test_refresh_days_accept_fractions_and_zero(self):
        with patch.dict(os.environ, {"DETAILS_REFRESH_DAYS": "0.5",
                                     "REVIEWS_REFRESH_DAYS": "0"}):
            self.assertEqual(get_details_refresh_days(), 0.5)
            self.assertEqual(get_reviews_refresh_days(), 0)

    def test_valve_charts_can_be_switched_off(self):
        for value in ("false", "0", "no", "FALSE"):
            with patch.dict(os.environ, {"CATALOG_FROM_VALVE_CHARTS": value}):
                self.assertFalse(get_catalog_from_valve_charts())

    def test_invalid_values_are_rejected(self):
        for name, value in (
            ("CATALOG_FROM_VALVE_CHARTS", "maybe"),
            ("STEAMSPY_PAGE_INTERVAL_DAYS", "0"),
            ("STEAMSPY_PAGE_INTERVAL_DAYS", "weekly"),
            ("REVIEW_SUMMARIES_PER_RUN", "301"),
            ("REVIEW_SUMMARIES_PER_RUN", "-1"),
            ("STORE_DETAILS_PER_RUN", "many"),
            ("TAG_REFRESH_DAYS", "-1"),
            ("DETAILS_REFRESH_DAYS", "0.01"),
            ("REVIEWS_REFRESH_DAYS", "daily"),
            ("REFRESH_MAX_REQUESTS", "0"),
            ("STEAMSPY_PAGES", "21"),
        ):
            getter = {
                "CATALOG_FROM_VALVE_CHARTS": get_catalog_from_valve_charts,
                "STEAMSPY_PAGE_INTERVAL_DAYS": get_steamspy_page_interval_days,
                "REVIEW_SUMMARIES_PER_RUN": get_review_summaries_per_run,
                "STORE_DETAILS_PER_RUN": get_store_details_per_run,
                "TAG_REFRESH_DAYS": get_tag_refresh_days,
                "DETAILS_REFRESH_DAYS": get_details_refresh_days,
                "REVIEWS_REFRESH_DAYS": get_reviews_refresh_days,
                "REFRESH_MAX_REQUESTS": get_refresh_max_requests,
                "STEAMSPY_PAGES": get_steamspy_pages,
            }[name]

            with patch.dict(os.environ, {name: value}):
                with self.assertRaises(ValueError, msg=f"{name}={value}"):
                    getter()


if __name__ == "__main__":
    unittest.main()
