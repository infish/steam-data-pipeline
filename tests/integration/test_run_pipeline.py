import os
import unittest
from unittest.mock import patch

import main

from .base import (
    CANNED_MEASUREMENTS,
    CANNED_TOP_GAMES,
    CHART_MEASURED_AT,
    IntegrationTestCase,
    steamspy_game,
)


class RunPipelineIntegrationTests(IntegrationTestCase):
    def test_successful_run_loads_all_tables(self):
        with self.assertLogs(level="INFO") as logs:
            self.run_pipeline()

        logged = "\n".join(logs.output)
        self.assertIn("1 top-100 games not in catalog page; skipped", logged)
        self.assertNotIn("WARNING", logged)

        runs = self.query("SELECT * FROM pipeline_runs")
        self.assertEqual(len(runs), 1)
        run = runs[0]
        self.assertEqual(run["status"], "SUCCESS")
        self.assertEqual(run["rows_extracted"], 4)
        self.assertEqual(run["rows_loaded"], 4)
        self.assertEqual(run["measurements_loaded"], 3)
        self.assertEqual(run["rows_rejected"], 0)
        self.assertIsNotNone(run["finished_at"])
        self.assertIsNone(run["error_message"])
        self.assertIsNone(run["warning_message"])
        self.assertTrue(self.run_lock_is_free())

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
        self.assertTrue(
            all(row["run_id"] == run["run_id"] for row in snapshots)
        )
        counter_strike = snapshots[2]
        self.assertEqual(counter_strike["appid"], 730)
        self.assertEqual(counter_strike["owners_low"], 50_000_000)
        self.assertEqual(counter_strike["owners_high"], 100_000_000)
        self.assertEqual(counter_strike["total_reviews"], 10_000)
        self.assertEqual(float(counter_strike["review_score_percent"]), 90.0)

        measurements = {
            row["appid"]: row
            for row in self.query("SELECT * FROM steam_game_measurements")
        }
        self.assertEqual(sorted(measurements), [620, 730, 427520])
        self.assertEqual(measurements[730]["current_players"], 1_500_000)
        self.assertEqual(measurements[730]["player_rank"], 1)
        self.assertEqual(
            measurements[730]["source_measured_at"],
            CHART_MEASURED_AT
        )
        self.assertIsNone(measurements[620]["player_rank"])
        self.assertIsNone(measurements[620]["source_measured_at"])
        self.assertEqual(measurements[620]["total_reviews"], 1000)

    def test_failure_after_snapshot_insert_rolls_back_the_load(self):
        real_load = main.load_steam_measurements

        def load_then_fail(*args, **kwargs):
            real_load(*args, **kwargs)
            raise RuntimeError("forced failure after load")

        with (
            patch.object(
                main,
                "load_steam_measurements",
                side_effect=load_then_fail
            ),
            self.assertLogs(level="ERROR"),
            self.assertRaisesRegex(RuntimeError, "forced failure")
        ):
            self.run_pipeline()

        self.assertEqual(self.count("game_metric_snapshots"), 0)
        self.assertEqual(self.count("steam_game_measurements"), 0)
        self.assertEqual(self.count("games"), 0)

        runs = self.query("SELECT * FROM pipeline_runs")
        self.assertEqual(len(runs), 1)
        self.assertEqual(runs[0]["status"], "FAILED")
        self.assertEqual(
            runs[0]["error_message"],
            "forced failure after load"
        )
        self.assertIsNotNone(runs[0]["finished_at"])
        self.assertTrue(self.run_lock_is_free())


    def test_valve_warnings_reach_the_run_row(self):
        warnings = [
            "Valve player lookup failed for app 620, row skipped: timeout",
            "Steam review lookup failed for app 730, reviews left empty: 502",
        ]

        with self.assertLogs(level="WARNING") as logs:
            self.run_pipeline(
                measurements=CANNED_MEASUREMENTS[:1],
                warnings=warnings
            )

        logged = "\n".join(logs.output)
        for warning in warnings:
            self.assertIn(warning, logged)

        (run,) = self.query("SELECT * FROM pipeline_runs")
        self.assertEqual(run["status"], "SUCCESS")
        self.assertEqual(run["measurements_loaded"], 1)
        self.assertEqual(run["warning_message"], "\n".join(warnings))

    def test_steamspy_failure_still_fails_the_run(self):
        with (
            patch.object(
                main,
                "get_top_games",
                side_effect=RuntimeError("SteamSpy down")
            ),
            patch.object(main, "get_steam_measurements") as valve,
            self.assertLogs(level="ERROR"),
            self.assertRaisesRegex(RuntimeError, "SteamSpy down")
        ):
            main.run_pipeline()

        valve.assert_not_called()
        (run,) = self.query("SELECT * FROM pipeline_runs")
        self.assertEqual(run["status"], "FAILED")
        self.assertEqual(run["error_message"], "SteamSpy down")


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
            self.run_pipeline(top_games=self.with_bad_rows())

        logged = "\n".join(logs.output)
        self.assertIn("Rejected 2 of 6 SteamSpy rows", logged)
        self.assertIn("111: name is missing or empty", logged)
        self.assertIn("222: owners is missing", logged)

        (run,) = self.query("SELECT * FROM pipeline_runs")
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
            self.run_pipeline(top_games=self.with_bad_rows())

        (run,) = self.query("SELECT * FROM pipeline_runs")
        self.assertEqual(run["status"], "FAILED")
        self.assertIn("2 of 6 rows (33.3%)", run["error_message"])
        self.assertEqual(self.count("games"), 0)


    def test_off_page_tracked_app_is_backfilled_with_its_measurement(self):
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

        (measurement,) = self.query(
            "SELECT * FROM steam_game_measurements WHERE appid = 427520"
        )
        self.assertEqual(measurement["current_players"], 20000)
        self.assertEqual(measurement["total_reviews"], 200)

    def test_backfill_failure_is_a_warning_and_the_run_succeeds(self):
        with self.assertLogs(level="WARNING") as logs:
            self.run_pipeline(app_details={})

        self.assertIn(
            "SteamSpy appdetails failed for tracked app 427520",
            "\n".join(logs.output)
        )

        (run,) = self.query("SELECT * FROM pipeline_runs")
        self.assertEqual(run["status"], "SUCCESS")
        self.assertEqual(run["rows_loaded"], 3)
        self.assertEqual(run["measurements_loaded"], 2)
        self.assertEqual(
            run["warning_message"],
            "SteamSpy appdetails failed for tracked app 427520, "
            "skipped: canned failure"
        )
        self.assertEqual(
            self.query(
                "SELECT appid FROM steam_game_measurements WHERE appid = 427520"
            ),
            []
        )


if __name__ == "__main__":
    unittest.main()
