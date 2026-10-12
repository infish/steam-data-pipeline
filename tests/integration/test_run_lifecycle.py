import unittest
from datetime import datetime

from database.mysql_database import get_connection
from jobs.base import ABANDONED_RUN_MESSAGE
from jobs.daily import RUN_LOCK_NAME

from .base import IntegrationTestCase


class RunLifecycleIntegrationTests(IntegrationTestCase):
    def test_second_concurrent_run_exits_without_a_run_row(self):
        holder = get_connection()

        try:
            cursor = holder.cursor()
            cursor.execute("SELECT GET_LOCK(%s, 0)", (RUN_LOCK_NAME,))
            self.assertEqual(cursor.fetchall()[0][0], 1)

            session = self.session()

            with self.assertLogs(level="WARNING") as logs:
                self.assertIsNone(self.run_pipeline(session=session))

            self.assertEqual(session.calls, [])
            self.assertIn(
                "Another pipeline run is active",
                "\n".join(logs.output)
            )
            self.assertEqual(self.count("pipeline_runs"), 0)
        finally:
            holder.close()

        self.assertTrue(self.run_lock_is_free())

    def test_stale_running_row_is_reaped_before_the_next_run(self):
        connection = get_connection()

        try:
            cursor = connection.cursor()
            cursor.execute(
                """
                INSERT INTO pipeline_runs (started_at, status)
                VALUES (%s, 'RUNNING')
                """,
                (datetime(2026, 9, 30, 18, 0),)
            )
            stale_run_id = cursor.lastrowid
            connection.commit()
        finally:
            connection.close()

        with self.assertLogs(level="WARNING") as logs:
            self.run_pipeline()

        self.assertIn(
            "Marked 1 abandoned RUNNING pipeline run(s) as FAILED",
            "\n".join(logs.output)
        )

        runs = {
            row["run_id"]: row
            for row in self.query("SELECT * FROM pipeline_runs")
        }
        self.assertEqual(len(runs), 2)
        stale = runs.pop(stale_run_id)
        self.assertEqual(stale["status"], "FAILED")
        self.assertEqual(stale["error_message"], ABANDONED_RUN_MESSAGE)
        self.assertIsNone(stale["finished_at"])
        (new_run,) = runs.values()
        self.assertEqual(new_run["status"], "SUCCESS")


if __name__ == "__main__":
    unittest.main()
