import unittest
from unittest.mock import MagicMock, patch

import main


class RunPipelineErrorHandlingTests(unittest.TestCase):
    def setUp(self):
        self.connection = MagicMock()
        self.cursor = self.connection.cursor.return_value
        self.cursor.fetchall.return_value = [(1,)]
        self.cursor.rowcount = 0

    def run_with(self, **patches):
        with (
            patch.object(
                main,
                "get_connection",
                return_value=self.connection
            ),
            patch.multiple(main, **patches),
            self.assertLogs(level="WARNING") as logs
        ):
            try:
                main.run_pipeline()
            finally:
                self.logs = logs.output

    def test_original_error_propagates_when_rollback_fails(self):
        self.connection.rollback.side_effect = RuntimeError("rollback broke")

        with self.assertRaisesRegex(ValueError, "original failure"):
            self.run_with(
                get_top_games=MagicMock(
                    side_effect=ValueError("original failure")
                ),
                get_steam_measurements=MagicMock()
            )

        logged = "\n".join(self.logs)
        self.assertIn("Could not record pipeline run", logged)
        self.assertIn("rollback broke", logged)
        self.cursor.execute.assert_any_call(
            "SELECT RELEASE_LOCK(%s)",
            (main.RUN_LOCK_NAME,)
        )
        self.connection.close.assert_called_once_with()

    def test_skips_without_starting_a_run_when_lock_is_held(self):
        self.cursor.fetchall.return_value = [(0,)]
        get_top_games = MagicMock()

        self.run_with(
            get_top_games=get_top_games,
            get_steam_measurements=MagicMock()
        )

        self.assertIn("Another pipeline run is active", "\n".join(self.logs))
        get_top_games.assert_not_called()
        self.cursor.execute.assert_called_once_with(
            "SELECT GET_LOCK(%s, 0)",
            (main.RUN_LOCK_NAME,)
        )
        self.connection.commit.assert_not_called()
        self.connection.close.assert_called_once_with()


class SummarizeWarningsTests(unittest.TestCase):
    def test_no_warnings_is_null(self):
        self.assertIsNone(main.summarize_warnings([]))

    def test_joins_warnings_one_per_line(self):
        self.assertEqual(
            main.summarize_warnings(["first", "second"]),
            "first\nsecond"
        )

    def test_truncates_to_the_limit(self):
        message = main.summarize_warnings(["x" * 3000])

        self.assertEqual(len(message), main.WARNING_MESSAGE_LIMIT)
        self.assertTrue(message.endswith("\u2026"))


if __name__ == "__main__":
    unittest.main()
