"""Run lifecycle shared by every pipeline job and by reprocessing."""

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Optional

from api.steam_api import create_session
from database.mysql_database import get_connection
from ingest.raw_store import RawStore


ABANDONED_RUN_MESSAGE = "Abandoned: process ended before completion"
WARNING_MESSAGE_LIMIT = 2000
RUN_COUNTERS = (
    "rows_extracted",
    "rows_loaded",
    "measurements_loaded",
    "rows_rejected",
)


def utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


@dataclass(frozen=True)
class Job:
    name: str
    # body(ctx) collects, parses and loads; it raises to fail the run.
    body: Callable
    # clear_run(cursor, run_id) deletes a run's loaded rows before reprocess.
    clear_run: Optional[Callable] = None
    # SQL returning the original load time of run %s, for reprocessing.
    load_time_sql: Optional[str] = None
    # Older job names whose stuck RUNNING rows this job also reaps.
    reaps: tuple = ()

    @property
    def lock_name(self):
        return f"steam_pipeline:{self.name}"


@dataclass
class RunContext:
    connection: object
    cursor: object
    run_id: int
    session: object
    store: object
    now_fn: Callable = utc_now
    sleep_fn: Callable = time.sleep
    # Reprocessing reuses the original load time; live runs leave it None.
    loaded_at: Optional[datetime] = None
    replay: bool = False
    warnings: list = field(default_factory=list)
    counters: dict = field(default_factory=dict)

    def warn(self, *messages):
        for message in messages:
            logging.warning(message)
            self.warnings.append(message)

    def load_time(self):
        """One timestamp for everything a run loads, fixed at first use.

        Every table loaded by a run then shares the same time, and
        reprocessing can reproduce it exactly.
        """
        if self.loaded_at is None:
            self.loaded_at = self.now_fn()

        return self.loaded_at


def acquire_lock(cursor, lock_name):
    cursor.execute("SELECT GET_LOCK(%s, 0)", (lock_name,))
    return cursor.fetchall()[0][0] == 1


def release_lock(cursor, lock_name):
    cursor.execute("SELECT RELEASE_LOCK(%s)", (lock_name,))
    cursor.fetchall()


def reap_abandoned_runs(cursor, job_names):
    # Only safe for jobs whose lock we hold: no run of theirs can be live.
    # Other jobs have their own locks and may legitimately be running.
    placeholders = ", ".join(["%s"] * len(job_names))
    cursor.execute(
        f"""
        UPDATE pipeline_runs
        SET status = 'FAILED', error_message = %s
        WHERE status = 'RUNNING'
          AND job_name IN ({placeholders})
        """,
        (ABANDONED_RUN_MESSAGE, *job_names)
    )
    return cursor.rowcount


def start_pipeline_run(cursor, started_at, job_name):
    cursor.execute(
        """
        INSERT INTO pipeline_runs (job_name, started_at, status)
        VALUES (%s, %s, %s)
        """,
        (job_name, started_at, "RUNNING")
    )
    return cursor.lastrowid


def finish_pipeline_run(
    cursor,
    run_id,
    status,
    finished_at,
    error_message=None,
    warning_message=None,
    **counters
):
    cursor.execute(
        """
        UPDATE pipeline_runs
        SET
            finished_at = %s,
            status = %s,
            rows_extracted = %s,
            rows_loaded = %s,
            measurements_loaded = %s,
            rows_rejected = %s,
            error_message = %s,
            warning_message = %s
        WHERE run_id = %s
        """,
        (
            finished_at,
            status,
            *(counters.get(name, 0) for name in RUN_COUNTERS),
            error_message,
            warning_message,
            run_id
        )
    )


def update_run_results(cursor, run_id, warning_message, **counters):
    """Rewrite a finished run's counters and warnings (after reprocessing)."""
    cursor.execute(
        """
        UPDATE pipeline_runs
        SET
            rows_extracted = %s,
            rows_loaded = %s,
            measurements_loaded = %s,
            rows_rejected = %s,
            warning_message = %s
        WHERE run_id = %s
        """,
        (
            *(counters.get(name, 0) for name in RUN_COUNTERS),
            warning_message,
            run_id
        )
    )


def summarize_warnings(warnings, limit=WARNING_MESSAGE_LIMIT):
    if not warnings:
        return None

    message = "\n".join(warnings)

    if len(message) <= limit:
        return message

    return message[:limit - 1] + "…"


def run_job(
    job,
    session_factory=create_session,
    now_fn=utc_now,
    sleep_fn=time.sleep
):
    """Run one job with a run row, lock, raw store and error handling.

    Returns the run ID, or None when another run held the lock.
    """
    connection = None
    cursor = None
    run_id = None
    lock_acquired = False
    raw_connection = None
    session = None

    try:
        connection = get_connection()
        cursor = connection.cursor()

        if not acquire_lock(cursor, job.lock_name):
            logging.warning(
                "Another %s run is active; skipping this run",
                job.name
            )
            return None

        lock_acquired = True

        abandoned_runs = reap_abandoned_runs(cursor, (job.name, *job.reaps))
        connection.commit()

        if abandoned_runs:
            logging.warning(
                "Marked %s abandoned RUNNING %s run(s) as FAILED",
                abandoned_runs,
                job.name
            )

        run_id = start_pipeline_run(cursor, now_fn(), job.name)
        connection.commit()

        raw_connection = get_connection()
        session = session_factory()
        ctx = RunContext(
            connection=connection,
            cursor=cursor,
            run_id=run_id,
            session=session,
            store=RawStore(raw_connection, run_id, now_fn),
            now_fn=now_fn,
            sleep_fn=sleep_fn
        )

        job.body(ctx)

        finish_pipeline_run(
            cursor,
            run_id,
            "SUCCESS",
            now_fn(),
            warning_message=summarize_warnings(ctx.warnings),
            **ctx.counters
        )
        connection.commit()

        logging.info(
            "%s run %s completed: %s; raw responses %s (%s warnings)",
            job.name,
            run_id,
            ", ".join(
                f"{name}={ctx.counters.get(name, 0)}"
                for name in RUN_COUNTERS
            ),
            dict(ctx.store.stats),
            len(ctx.warnings)
        )
        return run_id

    except Exception as error:
        logging.exception("%s run failed", job.name)

        if None not in (connection, cursor, run_id):
            try:
                connection.rollback()
                finish_pipeline_run(
                    cursor,
                    run_id,
                    "FAILED",
                    now_fn(),
                    error_message=str(error)[:1000]
                )
                connection.commit()
            except Exception:
                logging.exception(
                    "Could not record pipeline run %s as FAILED",
                    run_id
                )

        raise error

    finally:
        for closeable in (session, raw_connection):
            if closeable is not None:
                try:
                    closeable.close()
                except Exception:
                    logging.exception("Could not close %r", closeable)

        if lock_acquired:
            try:
                release_lock(cursor, job.lock_name)
            except Exception:
                logging.exception("Could not release the pipeline run lock")

        if cursor is not None:
            cursor.close()

        if connection is not None:
            connection.close()
