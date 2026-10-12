"""Rebuild a run's loaded rows from its stored raw responses.

    python src/reprocess.py --run-id 42

The run's raw_api_responses are replayed through the same collection,
parsing and loading code as the original run's job, inside one transaction
that first deletes the run's loaded rows. Running it twice gives the same
result.
Requests the original run did not make (for example after STEAM_TRACKED_APPIDS
changed) fail like an unreachable API and become warnings.
"""

import argparse
import logging

from database.mysql_database import get_connection
from ingest.replay import RawReplaySession, ReplayRawStore
from jobs.base import (
    RunContext,
    acquire_lock,
    release_lock,
    summarize_warnings,
    update_run_results,
)
from jobs.registry import JOBS


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


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--run-id", type=int, required=True)
    args = parser.parse_args(argv)
    reprocess_run(args.run_id)


if __name__ == "__main__":
    main()
