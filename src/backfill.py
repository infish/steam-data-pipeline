"""Load review history or news for chosen apps outside the daily schedule.

    python src/backfill.py --source review_histogram --appids 730,620
    python src/backfill.py --source news --appids 730 --count 100

Runs as its own 'backfill' job, with a run row, lock and raw landing like any
other job. The apps must already have a games row. Upserts make it safe to run
repeatedly: review periods and news gids are never duplicated.
"""

import argparse
import logging
from functools import partial

from config import get_tracked_appids
from jobs.base import Job, run_job
from jobs.loading import known_appids
from jobs.review_history import collect_histograms, collect_news


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

SOURCES = ("review_histogram", "news")


def run_backfill(ctx, source, appids, count):
    known = known_appids(ctx.cursor, appids)

    for appid in appids:
        if appid not in known:
            ctx.warn(f"App {appid} has no games row; skipped")

    appids = [appid for appid in appids if appid in known]

    if source == "review_histogram":
        loaded = collect_histograms(ctx, appids)
    else:
        loaded = collect_news(ctx, appids, count)

    ctx.counters.update(rows_extracted=len(appids), rows_loaded=loaded)


def backfill(source, appids, count=100, **options):
    if source not in SOURCES:
        raise ValueError(f"source must be one of {', '.join(SOURCES)}")

    job = Job(
        name="backfill",
        body=partial(
            run_backfill,
            source=source,
            appids=list(dict.fromkeys(appids)),
            count=count
        )
    )
    return run_job(job, **options)


def parse_appids(text):
    try:
        return [int(value) for value in text.split(",") if value.strip()]
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "--appids must be comma-separated integers"
        ) from error


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--source", required=True, choices=SOURCES)
    parser.add_argument(
        "--appids",
        type=parse_appids,
        default=None,
        help="comma-separated app IDs (default: STEAM_TRACKED_APPIDS)"
    )
    parser.add_argument(
        "--count",
        type=int,
        default=100,
        help="news items per app (news only)"
    )
    args = parser.parse_args(argv)

    if args.appids is None:
        args.appids = get_tracked_appids()

    backfill(args.source, args.appids, args.count)


if __name__ == "__main__":
    main()
