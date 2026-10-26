import time
import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

from database.mysql_database import get_connection
from jobs.base import ABANDONED_RUN_MESSAGE
from scheduler import latest_successes

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

    def test_scheduler_catch_up_reads_the_newest_successful_start(self):
        for job_name, started_at, status in (
            ("players_hourly", datetime(2026, 10, 26, 16, 7), "SUCCESS"),
            ("players_hourly", datetime(2026, 10, 26, 17, 7), "FAILED"),
            ("refresh_hourly", datetime(2026, 10, 26, 16, 37), "RUNNING"),
        ):
            self.execute(
                "INSERT INTO pipeline_runs (job_name, started_at, status)"
                " VALUES (%s, %s, %s)",
                (job_name, started_at, status)
            )

        prague = ZoneInfo("Europe/Prague")

        self.assertEqual(
            latest_successes(prague),
            {"players_hourly": datetime(2026, 10, 26, 17, 7, tzinfo=prague)}
        )

    def insert_running(self, job_name):
        return self.execute(
            "INSERT INTO pipeline_runs (job_name, started_at, status)"
            " VALUES (%s, %s, 'RUNNING')",
            (job_name, datetime(2026, 9, 30, 18, 0))
        )

    def test_a_job_starts_while_another_job_is_loading(self):
        # A running job's open transaction holds locks on its own run row
        # (its loads reference it). Reaping used to scan and lock every
        # pipeline_runs row, so a players run started meanwhile waited 50 s
        # for the lock and failed (2026-10-24, on the local stack).
        self.run_job("catalog_daily")
        loading = get_connection()

        try:
            cursor = loading.cursor()
            cursor.execute(
                "INSERT INTO pipeline_runs (job_name, started_at, status)"
                " VALUES ('refresh_hourly', UTC_TIMESTAMP(6), 'RUNNING')"
            )
            run_id = cursor.lastrowid
            loading.commit()
            cursor.execute(
                "INSERT INTO review_observations (run_id, appid,"
                " collected_at, positive_reviews, negative_reviews,"
                " total_reviews) VALUES (%s, 570, UTC_TIMESTAMP(6), 1, 0, 1)",
                (run_id,)
            )  # not committed: the refresh is still loading

            started = time.monotonic()
            players = self.run_job("players_hourly")
            took = time.monotonic() - started
        finally:
            loading.rollback()
            loading.close()

        self.assertEqual(self.run_row(players)["status"], "SUCCESS")
        self.assertIsNone(self.run_row(players)["warning_message"])
        self.assertLess(took, 20)

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
