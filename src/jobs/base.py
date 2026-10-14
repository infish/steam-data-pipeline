"""Run lifecycle shared by every pipeline job and by reprocessing."""

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Optional

from api.pacing import Pacer
from api.steam_api import API_INTERVALS, create_session
from database.mysql_database import get_connection
from ingest.raw_store import RawStore
from jobs.timing import StepTimer, no_step, save_steps
from marts.build import build_and_test_marts
from quality.checks import (
    describe_failure,
    reported_by,
    run_quality_checks,
    store_results,
)


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
    # Rebuild and test the analytics marts after a successful run.
    build_marts: bool = True

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
    # The job being run; its own data checks run after it.
    job_name: Optional[str] = None
    # Times the run's steps (run_steps); reprocessing runs without one.
    timer: Optional[StepTimer] = None
    warnings: list = field(default_factory=list)
    counters: dict = field(default_factory=dict)
    # One Pacer per rate-limited API, shared by every step of the run.
    pacers: dict = field(default_factory=dict)

    def pace(self, api):
        """Wait until api's minimum interval since its last request passed.

        Call right before a request to "store" or "steamspy" (API_INTERVALS).
        """
        if api not in self.pacers:
            self.pacers[api] = Pacer(API_INTERVALS[api], self.sleep_fn)

        self.pacers[api].wait()

    def step(self, name):
        """Time a block of the job as one step: `with ctx.step("load"):`."""
        return self.timer.step(name) if self.timer else no_step(name)

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


def set_run_warnings(cursor, run_id, warnings):
    cursor.execute(
        "UPDATE pipeline_runs SET warning_message = %s WHERE run_id = %s",
        (summarize_warnings(warnings), run_id)
    )


def refresh_marts(connection, cursor, ctx):
    """Rebuild the marts after a committed load; problems become warnings."""
    try:
        mart_warnings, results = build_and_test_marts(connection, ctx.step)
    except Exception as error:
        logging.exception("Mart build failed")
        mart_warnings, results = [f"Mart build failed: {error}"], []

    if mart_warnings:
        ctx.warn(*mart_warnings)
        set_run_warnings(cursor, ctx.run_id, ctx.warnings)
        connection.commit()

    return results


def record_quality(connection, cursor, ctx, mart_results):
    """Run the job's observability checks and store them with the mart tests.

    Failed checks the run is responsible for become run warnings; nothing is
    deleted.
    """
    checked_at = ctx.now_fn()

    try:
        results = mart_results + run_quality_checks(
            connection, checked_at, ctx.job_name, ctx.step
        )
        store_results(cursor, results, ctx.run_id, checked_at)
        failures = [
            describe_failure(row)
            for row in results
            if row["status"] == "fail"
            and row not in mart_results
            and reported_by(row, ctx.job_name)
        ]
    except Exception as error:
        logging.exception("Data quality checks failed")
        connection.rollback()
        failures = [f"Data quality checks could not run: {error}"]

    if failures:
        ctx.warn(*failures)
        set_run_warnings(cursor, ctx.run_id, ctx.warnings)

    connection.commit()


def describe_timing(timer):
    """'took 12.3 s: setup 0.1 s, job 10.9 s, ...' for the run log."""
    phases = [
        record for record in timer.finished_records()
        if record["parent_step"] is None
    ]
    total = sum(record["duration_ms"] for record in phases) / 1000
    return f"took {total:.1f} s: " + ", ".join(
        f"{record['step']} {record['duration_ms'] / 1000:.1f} s"
        for record in phases
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
    timer = StepTimer(now_fn)

    try:
        with timer.step("setup"):
            connection = get_connection()
            cursor = connection.cursor()

            if not acquire_lock(cursor, job.lock_name):
                logging.warning(
                    "Another %s run is active; skipping this run",
                    job.name
                )
                return None

            lock_acquired = True

            abandoned_runs = reap_abandoned_runs(
                cursor, (job.name, *job.reaps)
            )
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
            store = RawStore(raw_connection, run_id, now_fn)
            timer.requests_fn = lambda: sum(store.stats.values())
            ctx = RunContext(
                connection=connection,
                cursor=cursor,
                run_id=run_id,
                session=session,
                store=store,
                now_fn=now_fn,
                sleep_fn=timer.wrap_sleep(sleep_fn),
                job_name=job.name,
                timer=timer
            )

        with timer.step("job"):
            job.body(ctx)

        with timer.step("finish"):
            finish_pipeline_run(
                cursor,
                run_id,
                "SUCCESS",
                now_fn(),
                warning_message=summarize_warnings(ctx.warnings),
                **ctx.counters
            )
            connection.commit()

        if job.build_marts:
            with timer.step("marts"):
                mart_results = refresh_marts(connection, cursor, ctx)

            with timer.step("checks"):
                record_quality(connection, cursor, ctx, mart_results)

        save_steps(connection, cursor, run_id, timer)
        logging.info(
            "%s run %s completed: %s; raw responses %s (%s warnings);"
            " %s",
            job.name,
            run_id,
            ", ".join(
                f"{name}={ctx.counters.get(name, 0)}"
                for name in RUN_COUNTERS
            ),
            dict(ctx.store.stats),
            len(ctx.warnings),
            describe_timing(timer)
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

            # The steps up to the failure show where it happened.
            save_steps(connection, cursor, run_id, timer)

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
