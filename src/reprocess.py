"""Rebuild a run's loaded rows from its stored raw responses.

    python src/reprocess.py --run-id 42
    python src/reprocess.py --seed-fixtures

The run's raw_api_responses are replayed through the same collection,
parsing and loading code as the original run's job, inside one transaction
that first deletes the run's loaded rows. Running it twice gives the same
result.
Requests the original run did not make (for example after STEAM_TRACKED_APPIDS
changed) fail like an unreachable API and become warnings.

--seed-fixtures fills an empty database for demos and dashboard checks: it
runs every job once against the recorded responses in tests/fixtures/sources
(docs/sources.md) instead of the live APIs.
"""

import argparse
import logging

from database.mysql_database import get_connection
from ingest.replay import FixtureSession, RawReplaySession, ReplayRawStore
from jobs.base import (
    RunContext,
    acquire_lock,
    refresh_marts,
    release_lock,
    summarize_warnings,
    update_run_results,
)
from jobs.registry import JOBS
import main as pipeline


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def find_load_time(cursor, job, run_id, finished_at):
    """The original load timestamp, so rebuilt rows match the originals."""
    cursor.execute(job.load_time_sql, (run_id,))
    return cursor.fetchall()[0][0] or finished_at


def reprocess_run(run_id):
    connection = get_connection()
    cursor = connection.cursor()
    job = None
    lock_acquired = False

    try:
        cursor.execute(
            "SELECT job_name, status, finished_at FROM pipeline_runs"
            " WHERE run_id = %s",
            (run_id,)
        )
        rows = cursor.fetchall()

        if not rows:
            raise LookupError(f"Run {run_id} does not exist")

        job_name, status, finished_at = rows[0]

        if job_name not in JOBS:
            raise ValueError(
                f"Run {run_id} belongs to job {job_name!r}, which cannot be"
                " reprocessed"
            )

        job = JOBS[job_name]

        if not acquire_lock(cursor, job.lock_name):
            raise RuntimeError(
                f"A {job_name} run is active; reprocess after it finishes"
            )

        lock_acquired = True

        if status != "SUCCESS":
            raise ValueError(
                f"Run {run_id} has status {status}; only successful runs"
                " are reprocessed"
            )

        cursor.execute(
            "SELECT COUNT(*) FROM raw_api_responses WHERE run_id = %s",
            (run_id,)
        )

        if cursor.fetchall()[0][0] == 0:
            raise ValueError(
                f"Run {run_id} has no stored raw responses (it predates the"
                " raw landing layer)"
            )

        loaded_at = find_load_time(cursor, job, run_id, finished_at)
        job.clear_run(cursor, run_id)

        ctx = RunContext(
            connection=connection,
            cursor=cursor,
            run_id=run_id,
            session=RawReplaySession(connection, run_id),
            store=ReplayRawStore(connection),
            sleep_fn=lambda seconds: None,
            loaded_at=loaded_at,
            replay=True
        )
        job.body(ctx)
        update_run_results(
            cursor,
            run_id,
            summarize_warnings(ctx.warnings),
            **ctx.counters
        )
        connection.commit()
        refresh_marts(connection, cursor, ctx)
        logging.info(
            "Reprocessed %s run %s: %s",
            job_name,
            run_id,
            ctx.counters
        )
        return ctx.counters

    except Exception:
        connection.rollback()
        raise

    finally:
        if lock_acquired:
            release_lock(cursor, job.lock_name)

        cursor.close()
        connection.close()


def seed_fixtures():
    """Run every job once against the recorded fixture responses."""
    runs = pipeline.run_pipeline(
        session_factory=FixtureSession,
        sleep_fn=lambda seconds: None
    )
    logging.info("Seeded from fixtures: %s", runs)
    return runs


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--run-id", type=int)
    action.add_argument("--seed-fixtures", action="store_true")
    args = parser.parse_args(argv)

    if args.seed_fixtures:
        seed_fixtures()
    else:
        reprocess_run(args.run_id)


if __name__ == "__main__":
    main()
