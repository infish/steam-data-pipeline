import unittest
from unittest.mock import MagicMock, patch

from jobs import base
from jobs.base import Job, run_job, summarize_warnings


class RunJobErrorHandlingTests(unittest.TestCase):
    def setUp(self):
        self.connection = MagicMock()
        self.cursor = self.connection.cursor.return_value
        self.cursor.fetchall.return_value = [(1,)]
        self.cursor.rowcount = 0
        self.session = MagicMock()

    def run_with(self, body):
        job = Job(name="test", body=body)

        with (
            patch.object(base, "get_connection", return_value=self.connection),
            self.assertLogs(level="WARNING") as logs
        ):
            try:
                return run_job(job, session_factory=lambda: self.session)
            finally:
                self.logs = logs.output

    def test_original_error_propagates_when_rollback_fails(self):
        self.connection.rollback.side_effect = RuntimeError("rollback broke")

        with self.assertRaisesRegex(ValueError, "original failure"):
            self.run_with(MagicMock(side_effect=ValueError("original failure")))

        logged = "\n".join(self.logs)
        self.assertIn("Could not record pipeline run", logged)
        self.assertIn("rollback broke", logged)
        self.cursor.execute.assert_any_call(
            "SELECT RELEASE_LOCK(%s)",
            ("steam_pipeline:test",)
        )
        self.session.close.assert_called_once_with()
        # The main connection and the raw store's connection are both closed.
        self.assertEqual(self.connection.close.call_count, 2)

    def test_skips_without_starting_a_run_when_lock_is_held(self):
        self.cursor.fetchall.return_value = [(0,)]
        body = MagicMock()

        self.assertIsNone(self.run_with(body))

        self.assertIn("Another test run is active", "\n".join(self.logs))
        body.assert_not_called()
        self.cursor.execute.assert_called_once_with(
            "SELECT GET_LOCK(%s, 0)",
            ("steam_pipeline:test",)
        )
        self.connection.commit.assert_not_called()
        self.connection.close.assert_called_once_with()


class SummarizeWarningsTests(unittest.TestCase):
    def test_no_warnings_is_null(self):
        self.assertIsNone(summarize_warnings([]))

    def test_joins_warnings_one_per_line(self):
        self.assertEqual(
            summarize_warnings(["first", "second"]),
            "first\nsecond"
        )

    def test_truncates_to_the_limit(self):
        message = summarize_warnings(["x" * 3000])

        self.assertEqual(len(message), base.WARNING_MESSAGE_LIMIT)
        self.assertTrue(message.endswith("…"))


if __name__ == "__main__":
    unittest.main()
