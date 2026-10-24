import threading
import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from ingest.raw_store import MemoryRawStore
from jobs.refresh import ReviewsStopped, fetch_summaries_on_worker


def context(session):
    return SimpleNamespace(
        run_id=1,
        now_fn=lambda: datetime(2026, 10, 24, 12, 0),
        base_sleep_fn=lambda seconds: None,
        session_factory=lambda: session,
    )


class ReviewWorkerTests(unittest.TestCase):
    def test_a_stopped_worker_makes_no_request_and_closes_up(self):
        session = MagicMock()
        connection = MagicMock()
        stop = threading.Event()
        stop.set()

        with patch("jobs.refresh.get_connection", return_value=connection), \
                patch("jobs.refresh.RawStore",
                      lambda *args: MemoryRawStore()):
            with self.assertRaises(ReviewsStopped):
                fetch_summaries_on_worker(context(session), [570, 620], stop)

        session.get.assert_not_called()
        session.close.assert_called_once()
        connection.close.assert_called_once()

    def test_a_failed_connection_still_closes_the_session(self):
        session = MagicMock()

        with patch("jobs.refresh.get_connection",
                   side_effect=RuntimeError("database down")):
            with self.assertRaisesRegex(RuntimeError, "database down"):
                fetch_summaries_on_worker(
                    context(session), [570], threading.Event()
                )

        session.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
