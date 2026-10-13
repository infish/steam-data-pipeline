import os
import unittest
from unittest.mock import patch

import requests

from jobs import catalog

from .base import (
    CANNED_TOP_GAMES,
    CHART_MEASURED_AT,
    IntegrationTestCase,
    steamspy_game,
)


class RunPipelineIntegrationTests(IntegrationTestCase):
    def test_full_run_loads_every_job(self):
        with self.assertLogs(level="INFO") as logs:
            runs = self.run_pipeline()

        logged = "\n".join(logs.output)
        self.assertIn("1 top-100 games not in catalog page; skipped", logged)
        self.assertNotIn("WARNING", logged)

        for job_name, run_id in runs.items():
            run = self.run_row(run_id)
            self.assertEqual(run["job_name"], job_name)
            self.assertEqual(run["status"], "SUCCESS")
            self.assertIsNotNone(run["finished_at"])
            self.assertIsNone(run["error_message"])
            self.assertIsNone(run["warning_message"])
            self.assertTrue(self.run_lock_is_free(job_name))

        catalog_run = self.run_row(runs["catalog_daily"])
        self.assertEqual(catalog_run["rows_extracted"], 4)
        self.assertEqual(catalog_run["rows_loaded"], 4)
        self.assertEqual(catalog_run["rows_rejected"], 0)
        self.assertEqual(
            self.run_row(runs["players_hourly"])["measurements_loaded"],
            3
        )
        self.assertEqual(
            self.run_row(runs["reviews_daily"])["measurements_loaded"],
            3
        )

        self.assertEqual(
            [row["appid"] for row in self.query(
                "SELECT appid FROM games ORDER BY appid"
            )],
            [570, 620, 730, 427520]
        )

        snapshots = self.query(
            "SELECT * FROM game_metric_snapshots ORDER BY appid"
        )
        self.assertEqual(len(snapshots), 4)
        counter_strike = snapshots[2]
        self.assertEqual(counter_strike["appid"], 730)
        self.assertEqual(counter_strike["owners_low"], 50_000_000)
        self.assertEqual(counter_strike["owners_high"], 100_000_000)
        self.assertEqual(counter_strike["total_reviews"], 10_000)
        self.assertEqual(float(counter_strike["review_score_percent"]), 90.0)

        players = {
            row["appid"]: row
            for row in self.query("SELECT * FROM player_observations")
        }
        self.assertEqual(sorted(players), [620, 730, 427520])
        self.assertEqual(players[730]["current_players"], 1_500_000)
        self.assertEqual(players[730]["player_rank"], 1)
        self.assertEqual(players[730]["source"], "top100")
        self.assertEqual(
            players[730]["source_measured_at"],
            CHART_MEASURED_AT
        )
        self.assertEqual(players[620]["source"], "direct")
        self.assertIsNone(players[620]["player_rank"])
        self.assertIsNone(players[620]["source_measured_at"])

        reviews = {
            row["appid"]: row
            for row in self.query("SELECT * FROM review_observations")
        }
        self.assertEqual(sorted(reviews), [620, 730, 427520])
        self.assertEqual(reviews[620]["total_reviews"], 1000)
        self.assertEqual(float(reviews[730]["review_score_percent"]), 87.5)

        self.assertEqual(self.count("player_daily"), 3)
        # The leaderboard comes from the recorded Phase 10 fixture.
        self.assertEqual(self.count("most_played_daily"), 100)
        (top,) = self.query(
            "SELECT * FROM most_played_daily WHERE player_rank = 1"
        )
        self.assertEqual(top["appid"], 730)
        self.assertEqual(top["rollup_date"].isoformat(), "2026-09-30")

    def test_failure_after_snapshot_insert_rolls_back_the_load(self):
        real_load = catalog.load_game_metric_snapshots

        def load_then_fail(*args, **kwargs):
            real_load(*args, **kwargs)
            raise RuntimeError("forced failure after load")

        with (
            patch.object(
                catalog,
                "load_game_metric_snapshots",
                side_effect=load_then_fail
            ),
            self.assertLogs(level="ERROR"),
            self.assertRaisesRegex(RuntimeError, "forced failure")
        ):
            self.run_job("catalog_daily")

        self.assertEqual(self.count("game_metric_snapshots"), 0)
        self.assertEqual(self.count("games"), 0)

        (run,) = self.query("SELECT * FROM pipeline_runs")
        self.assertEqual(run["status"], "FAILED")
        self.assertEqual(run["error_message"], "forced failure after load")
        self.assertIsNotNone(run["finished_at"])
        self.assertTrue(self.run_lock_is_free())

    def test_valve_warnings_reach_their_job_runs(self):
        self.run_job("catalog_daily")

        with self.assertLogs(level="WARNING"):
            runs = self.run_pipeline(
                {
                    "valve_current_players:620": (503, {"error": "busy"}),
                    "steam_review_summary:730": (502, {"error": "gateway"}),
                },
                jobs=("players_hourly", "reviews_daily")
            )

        players = self.run_row(runs["players_hourly"])
        self.assertEqual(players["status"], "SUCCESS")
        self.assertEqual(players["measurements_loaded"], 2)
        self.assertTrue(players["warning_message"].startswith(
            "Valve player lookup failed for app 620, row skipped: 503"
        ))

        reviews = self.run_row(runs["reviews_daily"])
        self.assertEqual(reviews["status"], "SUCCESS")
        self.assertEqual(reviews["measurements_loaded"], 2)
        self.assertTrue(reviews["warning_message"].startswith(
            "Steam review lookup failed for app 730, skipped: 502"
        ))

    def test_steamspy_failure_fails_only_the_catalog_run(self):
        with (
            self.assertLogs(level="ERROR"),
            self.assertRaisesRegex(requests.ConnectionError, "SteamSpy down")
        ):
            self.run_pipeline({
                "steamspy_all": requests.ConnectionError("SteamSpy down")
            })

        runs = {
            row["job_name"]: row
            for row in self.query("SELECT * FROM pipeline_runs")
        }
        self.assertEqual(runs["catalog_daily"]["status"], "FAILED")
        self.assertEqual(
            runs["catalog_daily"]["error_message"],
            "SteamSpy down"
        )
        # The other jobs still ran; with no games rows they loaded nothing.
        self.assertEqual(runs["players_hourly"]["status"], "SUCCESS")
        self.assertEqual(runs["players_hourly"]["measurements_loaded"], 0)
        self.assertEqual(runs["reviews_daily"]["status"], "SUCCESS")

        (raw,) = self.query(
            "SELECT * FROM raw_api_responses WHERE source = 'steamspy_all'"
        )
        self.assertIsNone(raw["http_status"])
        self.assertEqual(raw["error_message"], "SteamSpy down")

    def with_bad_rows(self):
        top_games = dict(CANNED_TOP_GAMES)
        top_games["111"] = steamspy_game(111, "", "1 .. 2", 1, 1)
        top_games["222"] = steamspy_game(222, "No Owners", None, 1, 1)
        return top_games

    def test_rejected_rows_are_quarantined_and_recorded(self):
        with (
            patch.dict(os.environ, {"PIPELINE_MAX_REJECTED_PERCENT": "50"}),
            self.assertLogs(level="WARNING") as logs
        ):
            run_id = self.run_job(
                "catalog_daily",
                {"steamspy_all": self.with_bad_rows()}
            )

        logged = "\n".join(logs.output)
        self.assertIn("Rejected 2 of 6 SteamSpy rows", logged)
        self.assertIn("111: name is missing or empty", logged)
        self.assertIn("222: owners is missing", logged)

        run = self.run_row(run_id)
        self.assertEqual(run["status"], "SUCCESS")
        self.assertEqual(run["rows_extracted"], 6)
        self.assertEqual(run["rows_loaded"], 4)
        self.assertEqual(run["rows_rejected"], 2)
        self.assertIn(
            "Rejected 2 of 6 SteamSpy rows that failed validation "
            "(app IDs: 111, 222)",
            run["warning_message"]
        )
        self.assertEqual(
            [row["appid"] for row in self.query(
                "SELECT appid FROM games ORDER BY appid"
            )],
            [570, 620, 730, 427520]
        )
        self.assertEqual(self.count("game_metric_snapshots"), 4)

    def test_rejections_over_the_limit_fail_the_run(self):
        with (
            patch.dict(os.environ, {"PIPELINE_MAX_REJECTED_PERCENT": "5"}),
            self.assertLogs(level="ERROR"),
            self.assertRaisesRegex(ValueError, "above the 5% limit")
        ):
            self.run_job(
                "catalog_daily",
                {"steamspy_all": self.with_bad_rows()}
            )

        (run,) = self.query("SELECT * FROM pipeline_runs")
        self.assertEqual(run["status"], "FAILED")
        self.assertIn("2 of 6 rows (33.3%)", run["error_message"])
        self.assertEqual(self.count("games"), 0)

    def test_off_page_tracked_app_is_backfilled_with_its_observations(self):
        with self.assertLogs(level="INFO") as logs:
            self.run_pipeline()

        self.assertIn(
            "Backfilled 1 tracked app(s) missing from the SteamSpy page: "
            "427520",
            "\n".join(logs.output)
        )

        (game,) = self.query("SELECT * FROM games WHERE appid = 427520")
        self.assertEqual(game["name"], "Factorio")

        (snapshot,) = self.query(
            "SELECT * FROM game_metric_snapshots WHERE appid = 427520"
        )
        self.assertEqual(snapshot["owners_low"], 5_000_000)

        (players,) = self.query(
            "SELECT * FROM player_observations WHERE appid = 427520"
        )
        self.assertEqual(players["current_players"], 20000)

        (reviews,) = self.query(
            "SELECT * FROM review_observations WHERE appid = 427520"
        )
        self.assertEqual(reviews["total_reviews"], 200)

    def test_backfill_failure_is_a_warning_and_the_run_succeeds(self):
        with self.assertLogs(level="WARNING") as logs:
            runs = self.run_pipeline({
                "steamspy_appdetails:427520": (503, {"error": "busy"})
            })

        self.assertIn(
            "SteamSpy appdetails failed for tracked app 427520",
            "\n".join(logs.output)
        )

        catalog_run = self.run_row(runs["catalog_daily"])
        self.assertEqual(catalog_run["status"], "SUCCESS")
        self.assertEqual(catalog_run["rows_loaded"], 3)
        self.assertTrue(catalog_run["warning_message"].startswith(
            "SteamSpy appdetails failed for tracked app 427520, skipped: 503"
        ))
        self.assertEqual(
            self.run_row(runs["players_hourly"])["measurements_loaded"],
            2
        )
        self.assertEqual(
            self.query(
                "SELECT appid FROM player_observations WHERE appid = 427520"
            ),
            []
        )


if __name__ == "__main__":
    unittest.main()
