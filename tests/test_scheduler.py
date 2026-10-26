import os
import unittest
from datetime import datetime, timedelta
from unittest.mock import patch
from zoneinfo import ZoneInfo

from scheduler import (
    Scheduler,
    ScheduledJob,
    daily_at,
    get_next_hourly_run,
    get_next_run,
    get_schedule_config,
    hourly_at,
    missed_slot,
    wait_until,
)


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


class FakeClock:
    """A clock that only moves when the scheduler sleeps or a job runs."""

    def __init__(self, start):
        self.now = start

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.now += timedelta(seconds=seconds)


class HourlyScheduleTests(unittest.TestCase):
    def setUp(self):
        self.timezone = ZoneInfo("Europe/Prague")

    def test_next_hourly_run_is_the_given_minute_past_the_hour(self):
        now = datetime(2026, 10, 12, 14, 8, tzinfo=self.timezone)

        self.assertEqual(
            get_next_hourly_run(now, 7),
            datetime(2026, 10, 12, 15, 7, tzinfo=self.timezone)
        )
        self.assertEqual(
            get_next_hourly_run(now, 30),
            datetime(2026, 10, 12, 14, 30, tzinfo=self.timezone)
        )

    def test_hourly_runs_stay_one_real_hour_apart_across_fall_back(self):
        # 2026-10-25: Prague clocks go from 03:00 CEST back to 02:00 CET.
        now = datetime(2026, 10, 25, 0, 30, tzinfo=self.timezone)
        runs = []

        for _ in range(4):
            now = get_next_hourly_run(now, 7)
            runs.append(now)

        gaps = {
            later.timestamp() - earlier.timestamp()
            for earlier, later in zip(runs, runs[1:])
        }
        self.assertEqual(gaps, {3600.0})

    def test_hourly_runs_stay_one_real_hour_apart_across_spring_forward(self):
        now = datetime(2027, 3, 28, 0, 30, tzinfo=self.timezone)
        runs = []

        for _ in range(4):
            now = get_next_hourly_run(now, 7)
            runs.append(now)

        gaps = {
            later.timestamp() - earlier.timestamp()
            for earlier, later in zip(runs, runs[1:])
        }
        self.assertEqual(gaps, {3600.0})


class MultiJobSchedulerTests(unittest.TestCase):
    def setUp(self):
        self.timezone = ZoneInfo("Europe/Prague")
        self.jobs = [
            ScheduledJob("catalog_daily", daily_at(20, 0)),
            ScheduledJob("players_hourly", hourly_at(0)),
            ScheduledJob("reviews_daily", daily_at(20, 30)),
        ]

    def scheduler(self, start, run_seconds=0):
        clock = FakeClock(start)
        ran = []

        def run(names):
            ran.extend(names)
            clock.sleep(run_seconds)

        return Scheduler(self.jobs, clock, run, clock.sleep), clock, ran

    def test_due_jobs_run_in_table_order(self):
        scheduler, clock, ran = self.scheduler(
            datetime(2026, 10, 12, 19, 30, tzinfo=self.timezone)
        )

        self.assertEqual(scheduler.step(), ["catalog_daily", "players_hourly"])
        self.assertEqual(
            clock.now,
            datetime(2026, 10, 12, 20, 0, tzinfo=self.timezone)
        )
        self.assertEqual(scheduler.step(), ["reviews_daily"])
        self.assertEqual(scheduler.step(), ["players_hourly"])
        self.assertEqual(
            ran,
            ["catalog_daily", "players_hourly", "reviews_daily",
             "players_hourly"]
        )

    def test_a_long_run_does_not_retrigger_or_skip_ahead(self):
        # The catalog takes 40 minutes; the 20:30 reviews run is then late
        # but still runs once, and players resumes at the next hour.
        scheduler, clock, ran = self.scheduler(
            datetime(2026, 10, 12, 19, 59, tzinfo=self.timezone),
            run_seconds=40 * 60
        )

        scheduler.step()
        scheduler.step()

        self.assertEqual(
            ran,
            ["catalog_daily", "players_hourly", "reviews_daily"]
        )
        self.assertEqual(
            scheduler.next_runs["catalog_daily"],
            datetime(2026, 10, 13, 20, 0, tzinfo=self.timezone)
        )

    def test_daily_job_keeps_its_wall_clock_time_across_spring_forward(self):
        scheduler, clock, ran = self.scheduler(
            datetime(2027, 3, 27, 20, 0, 1, tzinfo=self.timezone)
        )
        start = clock.now

        while ran.count("catalog_daily") < 1:
            scheduler.step()

        self.assertEqual(
            clock.now,
            datetime(2027, 3, 28, 20, 0, tzinfo=self.timezone)
        )
        self.assertAlmostEqual(
            clock.now.timestamp() - start.timestamp(),
            23 * 3600 - 1
        )


class CatchUpTests(unittest.TestCase):
    """A restart during an update must not lose the slot it landed on."""

    def setUp(self):
        self.timezone = ZoneInfo("Europe/Prague")
        self.players = ScheduledJob("players_hourly", hourly_at(7))
        self.catalog = ScheduledJob("catalog_daily", daily_at(20, 0))

    def at(self, hour, minute, day=26):
        return datetime(2026, 10, day, hour, minute, tzinfo=self.timezone)

    def test_a_slot_missed_minutes_ago_is_returned(self):
        self.assertEqual(
            missed_slot(self.players, self.at(18, 10), self.at(17, 7)),
            self.at(18, 7)
        )

    def test_a_slot_with_a_successful_run_is_not_missed(self):
        self.assertIsNone(
            missed_slot(self.players, self.at(18, 10), self.at(18, 7))
        )

    def test_a_job_that_never_ran_catches_up_too(self):
        self.assertEqual(
            missed_slot(self.players, self.at(18, 10), None),
            self.at(18, 7)
        )

    def test_slots_older_than_the_window_are_skipped(self):
        self.assertIsNone(
            missed_slot(self.players, self.at(18, 30), self.at(17, 7))
        )
        self.assertIsNone(
            missed_slot(self.catalog, self.at(23, 0), self.at(20, 0, day=25))
        )

    def test_scheduler_runs_the_missed_job_first_then_keeps_the_schedule(self):
        clock = FakeClock(self.at(18, 10))
        ran = []
        scheduler = Scheduler(
            [self.catalog, self.players],
            clock,
            ran.extend,
            clock.sleep,
            last_success_fn=lambda: {
                "catalog_daily": self.at(20, 0, day=25),
                "players_hourly": self.at(17, 7),
            }
        )

        self.assertEqual(scheduler.step(), ["players_hourly"])
        self.assertEqual(clock.now, self.at(18, 10))
        self.assertEqual(scheduler.next_runs["players_hourly"], self.at(19, 7))
        self.assertEqual(scheduler.step(), ["players_hourly"])
        self.assertEqual(clock.now, self.at(19, 7))

    def test_without_history_the_schedule_is_unchanged(self):
        clock = FakeClock(self.at(18, 10))
        scheduler = Scheduler([self.players], clock, lambda names: None,
                              clock.sleep)

        self.assertEqual(scheduler.next_runs["players_hourly"], self.at(19, 7))


class ScheduleConfigTests(unittest.TestCase):
    def config(self, **env):
        with patch.dict(os.environ, env, clear=True):
            return get_schedule_config()

    def next_runs(self, jobs, now):
        return {job.name: job.next_run(now) for job in jobs}

    def test_defaults(self):
        timezone, jobs = self.config()
        now = datetime(2026, 10, 12, 12, 0, tzinfo=timezone)

        self.assertEqual(timezone.key, "Europe/Prague")
        self.assertEqual(
            self.next_runs(jobs, now),
            {
                "catalog_daily": now.replace(hour=20),
                "players_hourly": now.replace(minute=7),
                "refresh_hourly": now.replace(minute=37),
                "reviews_daily": now.replace(hour=20, minute=30),
            }
        )

    def test_job_times_from_env(self):
        timezone, jobs = self.config(
            JOB_CATALOG_TIME="06:15",
            JOB_REVIEWS_TIME="07:45",
            JOB_PLAYERS_MINUTE="23",
            JOB_REFRESH_MINUTE="50"
        )
        now = datetime(2026, 10, 12, 5, 0, tzinfo=timezone)

        self.assertEqual(
            self.next_runs(jobs, now),
            {
                "catalog_daily": now.replace(hour=6, minute=15),
                "players_hourly": now.replace(minute=23),
                "refresh_hourly": now.replace(minute=50),
                "reviews_daily": now.replace(hour=7, minute=45),
            }
        )

    def test_legacy_run_hour_still_sets_the_catalog_time(self):
        timezone, jobs = self.config(
            PIPELINE_RUN_HOUR="21",
            PIPELINE_RUN_MINUTE="15"
        )
        now = datetime(2026, 10, 12, 12, 0, tzinfo=timezone)

        self.assertEqual(
            self.next_runs(jobs, now)["catalog_daily"],
            now.replace(hour=21, minute=15)
        )

    def test_invalid_values_raise(self):
        for env in (
            {"JOB_CATALOG_TIME": "25:00"},
            {"JOB_REVIEWS_TIME": "noon"},
            {"JOB_PLAYERS_MINUTE": "60"},
            {"PIPELINE_TIMEZONE": "Mars/Base"},
        ):
            with self.subTest(env=env), self.assertRaises(ValueError):
                self.config(**env)


if __name__ == "__main__":
    unittest.main()
