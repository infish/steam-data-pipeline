import unittest
from unittest.mock import patch

import main

from .base import CHART_MEASURED_AT, IntegrationTestCase


class RunPipelineIntegrationTests(IntegrationTestCase):
    def test_successful_run_loads_all_tables(self):
        with self.assertLogs(level="WARNING") as logs:
            self.run_pipeline()

        self.assertIn("[999999]", "\n".join(logs.output))

        runs = self.query("SELECT * FROM pipeline_runs")
        self.assertEqual(len(runs), 1)
        run = runs[0]
        self.assertEqual(run["status"], "SUCCESS")
        self.assertEqual(run["rows_extracted"], 3)
        self.assertEqual(run["rows_loaded"], 3)
        self.assertEqual(run["measurements_loaded"], 2)
        self.assertEqual(run["rows_rejected"], 0)
        self.assertIsNotNone(run["finished_at"])
        self.assertIsNone(run["error_message"])
        self.assertIsNone(run["warning_message"])
        self.assertTrue(self.run_lock_is_free())

        self.assertEqual(
            [row["appid"] for row in self.query(
                "SELECT appid FROM games ORDER BY appid"
            )],
            [570, 620, 730]
        )

        snapshots = self.query(
            "SELECT * FROM game_metric_snapshots ORDER BY appid"
        )
        self.assertEqual(len(snapshots), 3)
        self.assertTrue(
            all(row["run_id"] == run["run_id"] for row in snapshots)
        )
        counter_strike = snapshots[2]
        self.assertEqual(counter_strike["owners_low"], 50_000_000)
        self.assertEqual(counter_strike["owners_high"], 100_000_000)
        self.assertEqual(counter_strike["total_reviews"], 10_000)
        self.assertEqual(float(counter_strike["review_score_percent"]), 90.0)

        measurements = {
            row["appid"]: row
            for row in self.query("SELECT * FROM steam_game_measurements")
        }
        self.assertEqual(sorted(measurements), [620, 730])
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


if __name__ == "__main__":
    unittest.main()
