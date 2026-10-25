import unittest
from datetime import datetime, timedelta
from unittest.mock import patch
from urllib.parse import unquote

import pandas as pd
from streamlit.testing.v1 import AppTest

import dashboard_common
from dashboard_data import freshness_levels, hours
from dashboard_motion import (
    DEFAULT_EXPECTED_SECONDS,
    dot,
    expected_seconds,
    is_interrupted,
    job_states,
    liquid_gauge,
)


NOW = datetime(2026, 10, 24, 18, 0)


class ExpectedSecondsTests(unittest.TestCase):
    def test_median_of_recent_successful_durations(self):
        history = pd.DataFrame({
            "job_name": ["refresh_hourly"] * 3 + ["players_hourly"],
            "duration_seconds": [60.0, 250.0, 120.0, 1.0],
        })

        self.assertEqual(expected_seconds(history),
                         {"refresh_hourly": 120.0, "players_hourly": 1.0})

    def test_missing_or_zero_durations_are_ignored(self):
        history = pd.DataFrame({
            "job_name": ["reviews_daily", "reviews_daily"],
            "duration_seconds": [None, 0.0],
        })

        self.assertEqual(expected_seconds(history), {})


class JobStatesTests(unittest.TestCase):
    def latest(self, *rows):
        return pd.DataFrame(rows, columns=[
            "run_id", "job_name", "status", "started_at", "finished_at"
        ])

    def states(self, latest, expected=None):
        return {
            state["job"]: state
            for state in job_states(latest, expected or {}, NOW)
        }

    def test_every_current_job_has_a_bubble_in_a_fixed_order(self):
        self.assertEqual(
            [state["letter"] for state in job_states(self.latest(), {}, NOW)],
            ["C", "P", "R", "V"]
        )

    def test_a_job_without_runs_is_idle(self):
        state = self.states(self.latest())["catalog_daily"]

        self.assertIsNone(state["id"])
        self.assertIsNone(state["status"])
        self.assertIsNone(state["since"])
        self.assertEqual(state["expected"], DEFAULT_EXPECTED_SECONDS)

    def test_a_running_job_measures_time_until_now(self):
        state = self.states(self.latest(
            (7, "refresh_hourly", "RUNNING", NOW - timedelta(seconds=90),
             None),
        ), {"refresh_hourly": 250.0})["refresh_hourly"]

        self.assertEqual((state["id"], state["status"]), (7, "RUNNING"))
        self.assertEqual(state["elapsed"], 90.0)
        self.assertEqual(state["expected"], 250.0)
        self.assertIsNone(state["since"])
        self.assertIn("--c:", state["style"])

    def test_a_finished_job_reports_its_duration_and_age(self):
        state = self.states(self.latest(
            (8, "players_hourly", "FAILED", NOW - timedelta(seconds=12),
             NOW - timedelta(seconds=11)),
        ))["players_hourly"]

        self.assertEqual(state["status"], "FAILED")
        self.assertEqual(state["elapsed"], 1.0)
        self.assertEqual(state["since"], 11.0)


class InterruptedTests(unittest.TestCase):
    def test_a_run_past_its_limit_and_margin_is_interrupted(self):
        # refresh_hourly: 20-minute limit plus a 10-minute margin.
        started = NOW - timedelta(minutes=37)
        self.assertTrue(
            is_interrupted("refresh_hourly", "RUNNING", started, NOW)
        )
        self.assertFalse(is_interrupted(
            "refresh_hourly", "RUNNING", NOW - timedelta(minutes=29), NOW
        ))

    def test_a_job_without_a_limit_uses_the_default(self):
        self.assertTrue(is_interrupted(
            "players_hourly", "RUNNING", NOW - timedelta(minutes=21), NOW
        ))

    def test_only_running_rows_can_be_interrupted(self):
        self.assertFalse(is_interrupted(
            "refresh_hourly", "SUCCESS", NOW - timedelta(hours=5), NOW
        ))

    def test_an_interrupted_run_is_reported_as_such(self):
        latest = pd.DataFrame([(9, "refresh_hourly", "RUNNING",
                                NOW - timedelta(minutes=37), None)],
                              columns=["run_id", "job_name", "status",
                                       "started_at", "finished_at"])
        (state,) = [s for s in job_states(latest, {}, NOW)
                    if s["job"] == "refresh_hourly"]

        self.assertEqual(state["status"], "INTERRUPTED")
        self.assertEqual(state["title"], "Hourly refresh")
        self.assertTrue(state["about"])


class FreshnessLevelTests(unittest.TestCase):
    def test_hours_reads_the_first_number(self):
        self.assertEqual(hours("1.33 h"), 1.33)
        self.assertEqual(hours("newest success younger than 36 h"), 36.0)
        self.assertIsNone(hours("never fetched"))

    def test_the_level_drains_as_data_ages(self):
        freshness = pd.DataFrame({
            "source": ["fresh", "half", "stale", "never"],
            "status": ["pass", "pass", "fail", "warn"],
            "age": ["0.0 h", "0.75 h", "3 h", None],
            "threshold": ["younger than 1.5 h"] * 4,
        })

        levels = {row["source"]: row["left"]
                  for row in freshness_levels(freshness)}

        self.assertEqual(levels, {
            "fresh": 1.0, "half": 0.5, "stale": 0.0, "never": None,
        })


class GaugeTests(unittest.TestCase):
    def level(self, html):
        svg = unquote(html.split("data:image/svg+xml,")[1])
        return float(svg.split("transform='translate(0,")[1].split(")")[0])

    def test_an_image_the_html_sanitiser_keeps(self):
        html = liquid_gauge(0.4, "#dd7516")

        self.assertTrue(html.startswith("<img "))
        self.assertIn("data:image/svg+xml,", html)
        self.assertIn("alt='40%'", html)

    def test_the_fill_is_clamped_to_empty_and_full(self):
        self.assertEqual(self.level(liquid_gauge(-1, "#dd7516")), 40.0)
        self.assertEqual(self.level(liquid_gauge(2, "#dd7516")), -4.0)
        self.assertEqual(self.level(liquid_gauge(None, "#dd7516")), 40.0)

    def test_a_running_job_dot_breathes(self):
        self.assertIn("m-live", dot("players_hourly", live=True))
        self.assertNotIn("m-live", dot("players_hourly"))


class DashboardMotionAppTests(unittest.TestCase):
    def test_the_dashboard_runs_with_the_bubbles_mounted(self):
        running = pd.DataFrame({
            "run_id": [1], "job_name": ["refresh_hourly"],
            "status": ["RUNNING"], "started_at": [NOW], "finished_at": [None],
        })

        def fake_read_sql(query, params=()):
            if "AS latest ON latest.run_id" in query:
                return running
            raise dashboard_common.DashboardBusy("no database in this test")

        with patch.object(dashboard_common, "read_sql", fake_read_sql), \
                patch("dashboard_motion.read_sql", fake_read_sql):
            app = AppTest.from_file("../src/dashboard.py", default_timeout=30)
            app.run()

        self.assertEqual(len(app.exception), 0)


if __name__ == "__main__":
    unittest.main()
