import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

from scheduler import get_next_run, wait_until


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

    def test_wait_until_rechecks_after_early_wakeup(self):
        target = datetime(
            2026, 9, 1, 20, 0,
            tzinfo=self.timezone
        )
        observed_times = iter([
            datetime(2026, 9, 1, 19, 59, 59, tzinfo=self.timezone),
            datetime(2026, 9, 1, 19, 59, 59, 500000, tzinfo=self.timezone),
            target
        ])
        sleeps = []

        wait_until(
            target,
            now_fn=lambda: next(observed_times),
            sleep_fn=sleeps.append
        )

        self.assertEqual(sleeps, [1.0, 0.5])

    def wait_across_night(self, now):
        next_run = get_next_run(now, 20, 0)
        observed_times = iter([now, next_run])
        sleeps = []

        wait_until(
            next_run,
            now_fn=lambda: next(observed_times),
            sleep_fn=sleeps.append
        )

        return sleeps

    def test_wait_until_is_23_hours_across_spring_forward(self):
        sleeps = self.wait_across_night(
            datetime(2027, 3, 27, 20, 0, 1, tzinfo=self.timezone)
        )

        self.assertEqual(len(sleeps), 1)
        self.assertAlmostEqual(sleeps[0], 23 * 3600 - 1)

    def test_wait_until_is_25_hours_across_fall_back(self):
        sleeps = self.wait_across_night(
            datetime(2026, 10, 24, 20, 0, 1, tzinfo=self.timezone)
        )

        self.assertEqual(len(sleeps), 1)
        self.assertAlmostEqual(sleeps[0], 25 * 3600 - 1)


if __name__ == "__main__":
    unittest.main()
