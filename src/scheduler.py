import logging
import os
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from main import run_pipeline


DEFAULT_TIMEZONE = "Europe/Prague"
DEFAULT_RUN_HOUR = 20
DEFAULT_RUN_MINUTE = 0


def get_schedule_config():
    timezone_name = os.getenv(
        "PIPELINE_TIMEZONE",
        DEFAULT_TIMEZONE
    )

    try:
        timezone = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError as error:
        raise ValueError(
            f"Unknown pipeline timezone: {timezone_name}"
        ) from error

    run_hour = int(
        os.getenv("PIPELINE_RUN_HOUR", str(DEFAULT_RUN_HOUR))
    )
    run_minute = int(
        os.getenv("PIPELINE_RUN_MINUTE", str(DEFAULT_RUN_MINUTE))
    )

    if not 0 <= run_hour <= 23:
        raise ValueError("PIPELINE_RUN_HOUR must be between 0 and 23")

    if not 0 <= run_minute <= 59:
        raise ValueError("PIPELINE_RUN_MINUTE must be between 0 and 59")

    return timezone, run_hour, run_minute


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


def execute_pipeline():
    try:
        run_pipeline()
    except Exception:
        logging.exception(
            "Scheduled pipeline run failed; scheduler will continue"
        )


def wait_until(target, now_fn=None, sleep_fn=time.sleep):
    if now_fn is None:
        now_fn = lambda: datetime.now(target.tzinfo)

    while True:
        remaining_seconds = (target - now_fn()).total_seconds()

        if remaining_seconds <= 0:
            return

        sleep_fn(remaining_seconds)


def run_scheduler():
    timezone, run_hour, run_minute = get_schedule_config()

    logging.info(
        "Scheduler started: daily at %02d:%02d %s",
        run_hour,
        run_minute,
        timezone.key
    )

    if os.getenv(
        "PIPELINE_RUN_ON_STARTUP",
        "false"
    ).lower() in {"1", "true", "yes"}:
        execute_pipeline()

    while True:
        now = datetime.now(timezone)
        next_run = get_next_run(now, run_hour, run_minute)
        logging.info(
            "Next pipeline run scheduled for %s",
            next_run.isoformat()
        )

        wait_until(next_run)
        execute_pipeline()


if __name__ == "__main__":
    run_scheduler()
