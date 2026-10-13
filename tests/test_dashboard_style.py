import unittest
from datetime import datetime

from dashboard_style import compact, count_ticks, run_badge, time_ago


NOW = datetime(2026, 10, 13, 12, 0)


class CompactTests(unittest.TestCase):
    def test_shortens_large_values(self):
        self.assertEqual(compact(1186957), "1.19M")
        self.assertEqual(compact(12345), "12.3k")
        self.assertEqual(compact(2_500_000_000), "2.5B")

    def test_keeps_small_values_and_handles_missing(self):
        self.assertEqual(compact(950), "950")
        self.assertEqual(compact(None), "N/A")
        self.assertEqual(compact(float("nan")), "N/A")


class CountTicksTests(unittest.TestCase):
    def test_large_counts_use_si_ticks(self):
        self.assertEqual(count_ticks(1_200_000)["tickformat"], "~s")

    def test_small_counts_stay_whole_numbers(self):
        self.assertEqual(count_ticks(2), {"tickformat": ",d", "nticks": 5,
                                          "dtick": 1})
        self.assertIsNone(count_ticks(400)["dtick"])
        self.assertEqual(count_ticks(None)["dtick"], 1)


class TimeAgoTests(unittest.TestCase):
    def test_buckets(self):
        self.assertEqual(time_ago(None, NOW), "never")
        self.assertEqual(time_ago(datetime(2026, 10, 13, 11, 59), NOW),
                         "just now")
        self.assertEqual(time_ago(datetime(2026, 10, 13, 11, 30), NOW),
                         "30 min ago")
        self.assertEqual(time_ago(datetime(2026, 10, 13, 6, 0), NOW),
                         "6 h ago")
        self.assertEqual(time_ago(datetime(2026, 10, 10, 12, 0), NOW),
                         "3 days ago")


class RunBadgeTests(unittest.TestCase):
    def test_failure_wins_over_everything(self):
        badge = run_badge("FAILED", "warn", datetime(2026, 10, 1),
                          "players_hourly", NOW)
        self.assertIn("red-badge", badge)
        self.assertIn("failed", badge)

    def test_late_when_past_one_and_a_half_intervals(self):
        late = run_badge("SUCCESS", None, datetime(2026, 10, 13, 10, 0),
                         "players_hourly", NOW)
        on_time = run_badge("SUCCESS", None, datetime(2026, 10, 13, 10, 0),
                            "catalog_daily", NOW)
        self.assertIn("late", late)
        self.assertIn("green-badge", on_time)

    def test_warnings_and_unknown_jobs(self):
        warned = run_badge("SUCCESS", "2 rejected",
                           datetime(2026, 10, 13, 11, 50), "players_hourly",
                           NOW)
        unknown = run_badge("SUCCESS", "", datetime(2020, 1, 1), "other", NOW)
        self.assertIn("warnings", warned)
        self.assertIn("ok", unknown)


if __name__ == "__main__":
    unittest.main()
