import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

from scheduler import get_next_run


class SchedulerTests(unittest.TestCase):
    def setUp(self):
        self.timezone = ZoneInfo("Europe/Prague")

    def test_schedules_same_day_before_run_time(self):
        now = datetime(
            2026, 9, 1, 19, 30,
            tzinfo=self.timezone
        )

        next_run = get_next_run(now, 20, 0)

        self.assertEqual(
            next_run,
            datetime(
                2026, 9, 1, 20, 0,
                tzinfo=self.timezone
            )
        )

    def test_schedules_next_day_after_run_time(self):
        now = datetime(
            2026, 9, 1, 20, 30,
            tzinfo=self.timezone
        )

        next_run = get_next_run(now, 20, 0)

        self.assertEqual(
            next_run,
            datetime(
                2026, 9, 2, 20, 0,
                tzinfo=self.timezone
            )
        )

    def test_exact_run_time_schedules_next_day(self):
        now = datetime(
            2026, 9, 1, 20, 0,
            tzinfo=self.timezone
        )

        next_run = get_next_run(now, 20, 0)

        self.assertEqual(
            next_run,
            datetime(
                2026, 9, 2, 20, 0,
                tzinfo=self.timezone
            )
        )


if __name__ == "__main__":
    unittest.main()