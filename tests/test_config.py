import os
import unittest
from unittest.mock import patch

from config import (
    get_catalog_from_valve_charts,
    get_review_summaries_per_run,
    get_steamspy_page_interval_days,
    get_tag_refresh_days,
)


class CatalogSettingsTests(unittest.TestCase):
    def test_defaults(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertTrue(get_catalog_from_valve_charts())
            self.assertEqual(get_steamspy_page_interval_days(), 7)
            self.assertEqual(get_review_summaries_per_run(), 120)
            self.assertEqual(get_tag_refresh_days(), 30)

    def test_valve_charts_can_be_switched_off(self):
        for value in ("false", "0", "no", "FALSE"):
            with patch.dict(os.environ, {"CATALOG_FROM_VALVE_CHARTS": value}):
                self.assertFalse(get_catalog_from_valve_charts())

    def test_invalid_values_are_rejected(self):
        for name, value in (
            ("CATALOG_FROM_VALVE_CHARTS", "maybe"),
            ("STEAMSPY_PAGE_INTERVAL_DAYS", "0"),
            ("STEAMSPY_PAGE_INTERVAL_DAYS", "weekly"),
            ("REVIEW_SUMMARIES_PER_RUN", "141"),
            ("REVIEW_SUMMARIES_PER_RUN", "-1"),
            ("TAG_REFRESH_DAYS", "-1"),
        ):
            getter = {
                "CATALOG_FROM_VALVE_CHARTS": get_catalog_from_valve_charts,
                "STEAMSPY_PAGE_INTERVAL_DAYS": get_steamspy_page_interval_days,
                "REVIEW_SUMMARIES_PER_RUN": get_review_summaries_per_run,
                "TAG_REFRESH_DAYS": get_tag_refresh_days,
            }[name]

            with patch.dict(os.environ, {name: value}):
                with self.assertRaises(ValueError, msg=f"{name}={value}"):
                    getter()


if __name__ == "__main__":
    unittest.main()
