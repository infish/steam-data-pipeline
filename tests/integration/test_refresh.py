"""ADR 6: the hourly refresh slices and a multi-page SteamSpy catalog."""
import os
import unittest
from datetime import datetime
from unittest.mock import patch

import requests

from .base import (
    CANNED_TOP_GAMES,
    IntegrationTestCase,
    review_summary,
    steamspy_game,
)


def at(day, hour=18):
    moment = datetime(2026, 10, day, hour, 0)
    return lambda: moment


# Unset the baseline overrides, so the slices follow the refresh days.
DERIVED = {
    "STORE_DETAILS_PER_RUN": "",
    "REVIEW_SUMMARIES_PER_RUN": "",
    # Every game every 3 hours: ceil(4 games / 3 runs) = 2 per run.
    "DETAILS_REFRESH_DAYS": "0.125",
    "REVIEWS_REFRESH_DAYS": "0.125",
}


class RefreshIntegrationTests(IntegrationTestCase):
    def requests(self, run_id, source):
        return [
            row["appid"]
            for row in self.query(
                "SELECT JSON_UNQUOTE(JSON_EXTRACT(request_params,"
                " '$.appids')) AS appid FROM raw_api_responses"
                " WHERE run_id = %s AND source = %s ORDER BY response_id",
                (run_id, source)
            )
        ]

    @patch.dict(os.environ, DERIVED)
    def test_slices_follow_the_catalog_size_and_refresh_days(self):
        self.run_job("catalog_daily", now_fn=at(1))
        summaries = {"steam_review_summary:570": review_summary(800, 200)}

        first = self.run_job("refresh_hourly", summaries, now_fn=at(1, 18))
        second = self.run_job("refresh_hourly", summaries, now_fn=at(1, 19))

        # 4 games, 2 per run: two runs cover the catalog, oldest first.
        self.assertEqual(len(self.requests(first, "store_appdetails")), 2)
        self.assertEqual(len(self.requests(second, "store_appdetails")), 2)
        self.assertEqual(self.count("game_details"), 4)
        # Review summaries rotate through the games that are not tracked
        # (only 570 here); tracked games get theirs from reviews_daily.
        self.assertEqual(
            [row["appid"] for row in self.query(
                "SELECT appid FROM review_observations WHERE run_id = %s",
                (first,)
            )],
            [570]
        )

    @patch.dict(os.environ, {"STEAMSPY_PAGES": "2"})
    def test_several_steamspy_pages_build_one_catalog(self):
        second_page = {
            "4000": steamspy_game(
                4000, "Garry's Mod", "20,000,000 .. 50,000,000", 900, 100
            ),
        }

        run_id = self.run_job(
            "catalog_daily", {"steamspy_all:1": second_page}, now_fn=at(1)
        )

        self.assertEqual(self.run_row(run_id)["rows_loaded"], 5)
        (game,) = self.query("SELECT name FROM games WHERE appid = 4000")
        self.assertEqual(game["name"], "Garry's Mod")

    @patch.dict(os.environ, {"STEAMSPY_PAGES": "2"})
    def test_a_failed_page_is_a_warning_and_keeps_the_others(self):
        run_id = self.run_job(
            "catalog_daily",
            {"steamspy_all:1": requests.ConnectionError("page 1 down")},
            now_fn=at(1)
        )

        run = self.run_row(run_id)
        self.assertEqual(run["status"], "SUCCESS")
        self.assertIn("SteamSpy catalog page 1 failed", run["warning_message"])
        # Page 0 (three games) plus the backfilled tracked app.
        self.assertEqual(run["rows_loaded"], len(CANNED_TOP_GAMES) + 1)


class QuietReviewsIntegrationTests(IntegrationTestCase):
    """Games whose review total stopped changing are asked less often."""

    SETTINGS = {
        "STORE_DETAILS_PER_RUN": "0",
        "REVIEW_SUMMARIES_PER_RUN": "",
        "REVIEWS_REFRESH_DAYS": "1",
        "QUIET_REVIEWS_REFRESH_DAYS": "3",
    }

    def reviewed(self, run_id):
        return [
            row["appid"]
            for row in self.query(
                "SELECT appid FROM review_observations WHERE run_id = %s",
                (run_id,)
            )
        ]

    def refresh(self, day, hour, positive):
        return self.run_job(
            "refresh_hourly",
            {"steam_review_summary:570": review_summary(positive, 200)},
            now_fn=at(day, hour)
        )

    @patch.dict(os.environ, SETTINGS)
    def test_a_quiet_game_waits_three_days_an_active_one_one(self):
        self.run_job("catalog_daily", now_fn=at(1))

        first = self.refresh(1, 10, 800)
        too_soon = self.refresh(1, 11, 800)
        next_day = self.refresh(2, 10, 800)  # same total: now quiet
        quiet_wait = self.refresh(3, 10, 800)
        after_three = self.refresh(5, 10, 801)  # changed: active again
        active_again = self.refresh(6, 10, 802)

        self.assertEqual(self.reviewed(first), [570])
        self.assertEqual(self.reviewed(too_soon), [])
        self.assertEqual(self.reviewed(next_day), [570])
        self.assertEqual(self.reviewed(quiet_wait), [])
        self.assertEqual(self.reviewed(after_three), [570])
        self.assertEqual(self.reviewed(active_again), [570])

    @patch.dict(os.environ, SETTINGS)
    def test_a_failed_summary_waits_for_its_next_turn(self):
        # Steam answers {"response": {}} for some removed games; the game
        # used to head the list (no summary) and was asked every hour.
        self.run_job("catalog_daily", now_fn=at(1))
        empty = {"steam_review_summary:570": {"response": {}}}

        first = self.run_job("refresh_hourly", empty, now_fn=at(1, 10))
        next_hour = self.run_job("refresh_hourly", empty, now_fn=at(1, 11))
        next_day = self.run_job("refresh_hourly", empty, now_fn=at(2, 10))

        self.assertEqual(self.summary_requests(first), 1)
        self.assertIn("summary is missing for app 570",
                      self.run_row(first)["warning_message"])
        self.assertEqual(self.summary_requests(next_hour), 0)
        self.assertNotIn("review lookup failed",
                         self.run_row(next_hour)["warning_message"] or "")
        self.assertEqual(self.summary_requests(next_day), 1)

    def summary_requests(self, run_id):
        (row,) = self.query(
            "SELECT COUNT(*) AS n FROM raw_api_responses"
            " WHERE run_id = %s AND source = 'steam_review_summary'",
            (run_id,)
        )
        return row["n"]

    @patch.dict(os.environ, {**SETTINGS, "QUIET_REVIEWS_REFRESH_DAYS": "0"})
    def test_zero_treats_every_game_as_active(self):
        self.run_job("catalog_daily", now_fn=at(1))

        self.refresh(1, 10, 800)
        self.refresh(2, 10, 800)
        daily = self.refresh(3, 10, 800)

        self.assertEqual(self.reviewed(daily), [570])


class SteamSpyPagesDueIntegrationTests(IntegrationTestCase):
    def page_requests(self, run_id):
        (row,) = self.query(
            "SELECT COUNT(*) AS n FROM raw_api_responses"
            " WHERE run_id = %s AND source = 'steamspy_all'",
            (run_id,)
        )
        return row["n"]

    @patch.dict(os.environ, {"STEAMSPY_PAGE_INTERVAL_DAYS": "7"})
    def test_raising_the_page_count_fetches_the_pages_at_once(self):
        with patch.dict(os.environ, {"STEAMSPY_PAGES": "1"}):
            self.run_job("catalog_daily", now_fn=at(1))
            same = self.run_job("catalog_daily", now_fn=at(2))

        # Two pages wanted now; the last fetch asked only for page 0.
        with patch.dict(os.environ, {"STEAMSPY_PAGES": "2"}):
            raised = self.run_job(
                "catalog_daily", {"steamspy_all:1": {}}, now_fn=at(3)
            )
            after = self.run_job("catalog_daily", now_fn=at(4))

        self.assertEqual(self.page_requests(same), 0)
        self.assertEqual(self.page_requests(raised), 2)
        self.assertEqual(self.page_requests(after), 0)


if __name__ == "__main__":
    unittest.main()
