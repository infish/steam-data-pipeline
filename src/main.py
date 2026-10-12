"""Run the pipeline once: `python src/main.py`."""

import logging

from jobs.base import run_job
from jobs.daily import DAILY_JOB


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def run_pipeline(**options):
    """Run the daily job; options are passed to jobs.base.run_job."""
    return run_job(DAILY_JOB, **options)


if __name__ == "__main__":
    run_pipeline()
