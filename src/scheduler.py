"""In-process scheduler: run each job on its own schedule.

Jobs run one at a time, in table order when several are due together.
Daily times are wall-clock times in PIPELINE_TIMEZONE; hourly jobs run at a
fixed minute past every hour. All waits use absolute time, so DST changes do
not shift them.
"""

import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone as dt_timezone
from typing import Callable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from main import run_pipeline


DEFAULT_TIMEZONE = "Europe/Prague"
DEFAULT_PLAYERS_MINUTE = 7
# Half an hour after the players job, so the two never queue behind each
# other (ADR 6).
DEFAULT_REFRESH_MINUTE = 37
DEFAULT_CATALOG_TIME = "20:00"
DEFAULT_REVIEWS_TIME = "20:30"


def get_next_run(now, run_hour, run_minute):
    next_run = now.replace(
        hour=run_hour,
        minute=run_minute,
        second=0,
        microsecond=0
    )

    if next_run <= now:
        next_run = (
            now + timedelta(days=1)
        ).replace(
            hour=run_hour,
            minute=run_minute,
            second=0,
            microsecond=0
        )

    return next_run


def get_next_hourly_run(now, minute):
    """The next time at `minute` past the hour, computed in UTC.

    Prague's offset is a whole number of hours, so minute N past the hour
    is the same instant in UTC and local time, including across DST.
    """
    utc_now = now.astimezone(dt_timezone.utc)
    next_run = utc_now.replace(minute=minute, second=0, microsecond=0)

    if next_run <= utc_now:
        next_run += timedelta(hours=1)

    return next_run.astimezone(now.tzinfo)


@dataclass(frozen=True)
class ScheduledJob:
    name: str
    # next_run(now) -> the next aware datetime at which the job is due.
    next_run: Callable


def daily_at(hour, minute):
    return lambda now: get_next_run(now, hour, minute)


def hourly_at(minute):
    return lambda now: get_next_hourly_run(now, minute)


def parse_time(name, value):
    try:
        hour_text, minute_text = value.split(":")
        hour, minute = int(hour_text), int(minute_text)
    except ValueError as error:
        raise ValueError(f"{name} must look like HH:MM") from error

    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ValueError(f"{name} must be a valid time of day")

    return hour, minute


def get_env_text(name, default):
    value = os.getenv(name)
    return value.strip() if value and value.strip() else default


def parse_minute(name, default):
    try:
        minute = int(get_env_text(name, str(default)))
    except ValueError as error:
        raise ValueError(f"{name} must be a whole number") from error

    if not 0 <= minute <= 59:
        raise ValueError(f"{name} must be between 0 and 59")

    return minute


def get_schedule_config():
    timezone_name = get_env_text("PIPELINE_TIMEZONE", DEFAULT_TIMEZONE)

    try:
        timezone = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError as error:
        raise ValueError(
            f"Unknown pipeline timezone: {timezone_name}"
        ) from error

    # PIPELINE_RUN_HOUR/MINUTE predate the job split; they still set the
    # catalog time when JOB_CATALOG_TIME is not given.
    legacy_catalog_time = "{}:{}".format(
        get_env_text("PIPELINE_RUN_HOUR", "20"),
        get_env_text("PIPELINE_RUN_MINUTE", "0")
    )
    catalog_time = parse_time(
        "JOB_CATALOG_TIME",
        get_env_text("JOB_CATALOG_TIME", legacy_catalog_time)
    )
    reviews_time = parse_time(
        "JOB_REVIEWS_TIME",
        get_env_text("JOB_REVIEWS_TIME", DEFAULT_REVIEWS_TIME)
    )

    players_minute = parse_minute("JOB_PLAYERS_MINUTE", DEFAULT_PLAYERS_MINUTE)
    refresh_minute = parse_minute("JOB_REFRESH_MINUTE", DEFAULT_REFRESH_MINUTE)

    jobs = [
        ScheduledJob("catalog_daily", daily_at(*catalog_time)),
        ScheduledJob("players_hourly", hourly_at(players_minute)),
        ScheduledJob("refresh_hourly", hourly_at(refresh_minute)),
        ScheduledJob("reviews_daily", daily_at(*reviews_time)),
    ]
    return timezone, jobs


def execute_job(name, run_fn=run_pipeline):
    try:
        run_fn([name])
    except Exception:
        logging.exception(
            "Scheduled %s run failed; scheduler will continue",
            name
        )


def wait_until(target, now_fn=None, sleep_fn=time.sleep):
    if now_fn is None:
        now_fn = lambda: datetime.now(target.tzinfo)

    while True:
        # Compare absolute instants: subtracting aware datetimes that share
        # a tzinfo uses wall-clock time and is off by an hour across DST.
        remaining_seconds = target.timestamp() - now_fn().timestamp()

        if remaining_seconds <= 0:
            return

        sleep_fn(remaining_seconds)


class Scheduler:
    def __init__(self, jobs, now_fn, run_fn=run_pipeline, sleep_fn=time.sleep):
        self.jobs = jobs
        self.now_fn = now_fn
        self.run_fn = run_fn
        self.sleep_fn = sleep_fn
        now = now_fn()
        self.next_runs = {job.name: job.next_run(now) for job in jobs}

    def step(self):
        """Wait for the earliest due job, run every job that is due."""
        target = min(self.next_runs.values())
        logging.info(
            "Next run: %s at %s",
            ", ".join(
                name for name, at in self.next_runs.items() if at == target
            ),
            target.isoformat()
        )
        wait_until(target, self.now_fn, self.sleep_fn)

        due = [
            job for job in self.jobs
            if self.next_runs[job.name].timestamp()
            <= self.now_fn().timestamp()
        ]

        for job in due:
            execute_job(job.name, self.run_fn)
            # Schedule from after the run, so a long run never re-triggers.
            self.next_runs[job.name] = job.next_run(self.now_fn())

        return [job.name for job in due]


def run_scheduler():
    timezone, jobs = get_schedule_config()
    now_fn = lambda: datetime.now(timezone)

    logging.info(
        "Scheduler started (%s): %s",
        timezone.key,
        ", ".join(job.name for job in jobs)
    )

    if get_env_text(
        "PIPELINE_RUN_ON_STARTUP",
        "false"
    ).lower() in {"1", "true", "yes"}:
        for job in jobs:
            execute_job(job.name)

    scheduler = Scheduler(jobs, now_fn)

    while True:
        scheduler.step()


if __name__ == "__main__":
    run_scheduler()
