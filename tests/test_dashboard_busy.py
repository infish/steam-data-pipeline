import unittest
from unittest.mock import patch

from streamlit.testing.v1 import AppTest

import dashboard_common


class DashboardBusyTests(unittest.TestCase):
    def test_a_busy_database_shows_a_note_not_an_error(self):
        with patch.object(dashboard_common, "DB_WAIT_SECONDS", 0), \
                patch.object(dashboard_common._db_connections, "acquire",
                             return_value=False):
            app = AppTest.from_file("../src/dashboard.py", default_timeout=30)
            app.run()

        self.assertEqual(len(app.exception), 0)
        notes = [element.value for element in app.info] + [
            element.value for element in app.sidebar.caption
        ]
        self.assertTrue(any("try again in a minute" in note
                            for note in notes))


if __name__ == "__main__":
    unittest.main()
