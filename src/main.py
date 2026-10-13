"""Run pipeline jobs once.

    python src/main.py                    # catalog_daily, players_hourly, reviews_daily
    python src/main.py players_hourly     # one job
"""

import argparse
import logging

from jobs.base import run_job
from jobs.registry import JOBS


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def run_pipeline(job_names=None, **options):
    """Run the named jobs (default: all) in order; return {name: run_id}.

    A failing job does not stop the jobs after it; the first failure is
    re-raised once all have run. options are passed to jobs.base.run_job.
    """
    run_ids = {}
    first_error = None

    for name in job_names or JOBS:
        try:
            run_ids[name] = run_job(JOBS[name], **options)
        except Exception as error:
            first_error = first_error or error

    if first_error is not None:
        raise first_error

    return run_ids


def main(argv=None):
    parser = argparse.ArgumentParser(description="Run pipeline jobs once.")
    parser.add_argument(
        "jobs",
        nargs="*",
        metavar="job",
        help=f"one of {', '.join(JOBS)} (default: all)"
    )
    args = parser.parse_args(argv)
    unknown = [name for name in args.jobs if name not in JOBS]

    if unknown:
        parser.error(f"unknown job(s): {', '.join(unknown)}")

    run_pipeline(args.jobs or None)


if __name__ == "__main__":
    main()
