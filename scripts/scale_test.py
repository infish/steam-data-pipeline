"""Push a synthetic catalog through every job and report the processing cost.

Usage (against an empty, migrated database whose name ends in "_test"):

    PYTHONPATH=src MYSQL_HOST=... MYSQL_DATABASE=steam_pipeline_test \\
        MYSQL_USER=... MYSQL_PASSWORD=... \\
        python scripts/scale_test.py --pages 5 --hours 24

It simulates `--hours` hours of the schedule: the daily catalog job first,
then players_hourly and refresh_hourly every hour and reviews_daily once,
against SyntheticSession (src/ingest/synthetic.py) with every API pause
switched off. What it measures is the pipeline's own work (parsing, MySQL,
marts, checks) at that size; the API pauses are added as an estimate from
the request counts. Results go to stdout as Markdown (docs/scale.md).
"""

import argparse
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta

from api.steam_api import API_INTERVALS
from config import get_mysql_config
from database.mysql_database import get_connection
from ingest.synthetic import SyntheticSession
from jobs.base import run_job
from jobs.registry import JOBS


SOURCE_API = {
    "steamspy_all": "steamspy_all",
    "steamspy_appdetails": "steamspy",
    "store_appdetails": "store",
    "store_prices": "store",
    "steam_review_summary": "reviews",
    "steam_review_histogram": "store",
}


def scalar(sql, params=()):
    connection = get_connection()

    try:
        cursor = connection.cursor()
        cursor.execute(sql, params)
        return cursor.fetchall()[0][0]
    finally:
        connection.close()


def rows(sql, params=()):
    connection = get_connection()

    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute(sql, params)
        return cursor.fetchall()
    finally:
        connection.close()


def run(session, name, moment):
    started = time.perf_counter()
    run_id = run_job(
        JOBS[name],
        session_factory=lambda: session,
        now_fn=lambda: moment,
        sleep_fn=lambda seconds: None,
    )
    return run_id, time.perf_counter() - started


def api_seconds(run_id):
    """What the run's requests would cost at the paced intervals."""
    total = 0.0

    for row in rows(
        "SELECT source, COUNT(*) AS n FROM raw_api_responses"
        " WHERE run_id = %s GROUP BY source",
        (run_id,)
    ):
        api = SOURCE_API.get(row["source"])
        total += row["n"] * API_INTERVALS[api] if api else 0

    return total


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--pages", type=int, default=5,
                        help="SteamSpy pages of 1,000 games (default 5)")
    parser.add_argument("--hours", type=int, default=24,
                        help="hours of the schedule to simulate (default 24)")
    args = parser.parse_args(argv)

    database = get_mysql_config()["database"]

    if not database.endswith("_test"):
        sys.exit(f"refusing to run against {database!r}: needs a *_test DB")

    if scalar("SELECT COUNT(*) FROM games"):
        sys.exit("the test database must be empty (no games rows)")

    os.environ.update({
        "STEAMSPY_PAGES": str(args.pages),
        "STEAMSPY_PAGE_INTERVAL_DAYS": "7",
        "CATALOG_FROM_VALVE_CHARTS": "true",
        "STEAM_TRACKED_APPIDS": "10,20,30",
    })
    session = SyntheticSession(pages=args.pages)
    start = datetime(2026, 10, 1, 20, 0)
    timings = defaultdict(list)

    def record(name, moment):
        run_id, seconds = run(session, name, moment)
        timings[name].append((run_id, seconds, api_seconds(run_id)))

    record("catalog_daily", start)

    for hour in range(args.hours):
        moment = start + timedelta(hours=hour, minutes=7)
        record("players_hourly", moment)
        record("refresh_hourly", moment + timedelta(minutes=30))

        if hour == 0:
            record("reviews_daily", moment + timedelta(minutes=23))

    print(f"## {args.pages * 1000:,} games, {args.hours} simulated hours\n")
    print("| Job | Runs | Processing, total | Processing, slowest run"
          " | API pauses if live, per run |")
    print("| --- | ---: | ---: | ---: | ---: |")

    for name, runs in timings.items():
        total = sum(seconds for _, seconds, _ in runs)
        slowest = max(seconds for _, seconds, _ in runs)
        api = max(api for _, _, api in runs)
        print(f"| `{name}` | {len(runs)} | {total:,.1f} s | {slowest:,.1f} s"
              f" | up to {api / 60:,.1f} min |")

    print("\n### Slowest steps\n")
    print("| Step | Inside | Runs | Total | Slowest |")
    print("| --- | --- | ---: | ---: | ---: |")

    for row in rows(
        "SELECT step, parent_step, COUNT(*) AS runs,"
        " SUM(duration_ms) / 1000 AS total, MAX(duration_ms) / 1000 AS worst"
        " FROM run_steps WHERE parent_step IS NOT NULL"
        " GROUP BY step, parent_step ORDER BY SUM(duration_ms) DESC LIMIT 12"
    ):
        print(f"| `{row['step']}` | `{row['parent_step']}` | {row['runs']}"
              f" | {row['total']:,.1f} s | {row['worst']:,.1f} s |")

    print("\n### Rows after the simulation\n")
    print("| Table | Rows |")
    print("| --- | ---: |")

    for table in ("games", "game_details", "game_tags", "game_price_history",
                  "review_observations", "player_observations",
                  "raw_api_responses", "data_quality_results", "run_steps"):
        print(f"| `{table}` | {scalar(f'SELECT COUNT(*) FROM {table}'):,} |")


if __name__ == "__main__":
    main()
