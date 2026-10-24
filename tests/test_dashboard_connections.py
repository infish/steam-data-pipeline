import threading
import time
import unittest
from unittest.mock import MagicMock, patch

import dashboard_common


class DashboardConnectionCapTests(unittest.TestCase):
    def test_no_more_than_the_cap_are_open_at_once(self):
        open_now = []
        peak = []
        lock = threading.Lock()

        def connect():
            with lock:
                open_now.append(1)
                peak.append(len(open_now))

            connection = MagicMock()
            connection.cursor.return_value.description = [("n",)]
            connection.cursor.return_value.fetchall.side_effect = (
                lambda: time.sleep(0.05) or [(1,)]
            )

            def close():
                with lock:
                    open_now.pop()

            connection.close.side_effect = close
            return connection

        with patch("dashboard_common.get_connection", side_effect=connect):
            threads = [
                threading.Thread(target=dashboard_common.read_sql,
                                 args=("SELECT 1",))
                for _ in range(dashboard_common.MAX_DB_CONNECTIONS * 3)
            ]

            for thread in threads:
                thread.start()

            for thread in threads:
                thread.join()

        self.assertEqual(max(peak), dashboard_common.MAX_DB_CONNECTIONS)
        self.assertEqual(open_now, [])


if __name__ == "__main__":
    unittest.main()
