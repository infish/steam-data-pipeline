import unittest
from datetime import datetime

from database.mysql_database import get_connection
from jobs.base import ABANDONED_RUN_MESSAGE

from .base import IntegrationTestCase


class RunLifecycleIntegrationTests(IntegrationTestCase):
    def test_a_held_job_lock_blocks_only_that_job(self):
        holder = get_connection()

        try:
            cursor = holder.cursor()
            cursor.execute(
                "SELECT GET_LOCK(%s, 0)",
                ("steam_pipeline:catalog_daily",)
            )
            self.assertEqual(cursor.fetchall()[0][0], 1)

            session = self.session()

            with self.assertLogs(level="WARNING") as logs:
                self.assertIsNone(
                    self.run_job("catalog_daily", session=session)
                )

            self.assertEqual(session.calls, [])
            self.assertIn(
                "Another catalog_daily run is active",
                "\n".join(logs.output)
            )
            self.assertEqual(self.count("pipeline_runs"), 0)

            # Another job has its own lock and still runs.
            players_run = self.run_job("players_hourly")
            self.assertEqual(self.run_row(players_run)["status"], "SUCCESS")
        finally:
            holder.close()

        self.assertTrue(self.run_lock_is_free("catalog_daily"))

    def insert_running(self, job_name):
        return self.execute(
            "INSERT INTO pipeline_runs (job_name, started_at, status)"
            " VALUES (%s, %s, 'RUNNING')",
            (job_name, datetime(2026, 9, 30, 18, 0))
        )

    def test_a_job_reaps_only_its_own_stale_runs(self):
        stale_catalog = self.insert_running("catalog_daily")
        stale_legacy = self.insert_running("daily_legacy")
        live_players = self.insert_running("players_hourly")

        with self.assertLogs(level="WARNING") as logs:
            new_run = self.run_job("catalog_daily")

        self.assertIn(
            "Marked 2 abandoned RUNNING catalog_daily run(s) as FAILED",
            "\n".join(logs.output)
        )

        for run_id in (stale_catalog, stale_legacy):
            run = self.run_row(run_id)
            self.assertEqual(run["status"], "FAILED")
            self.assertEqual(run["error_message"], ABANDONED_RUN_MESSAGE)
            self.assertIsNone(run["finished_at"])

        self.assertEqual(self.run_row(live_players)["status"], "RUNNING")
        self.assertEqual(self.run_row(new_run)["status"], "SUCCESS")


if __name__ == "__main__":
    unittest.main()
