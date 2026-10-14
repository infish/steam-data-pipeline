"""Per-step timing of pipeline runs, stored in run_steps.

A run is split into steps with `with timer.step("name"):`. Steps nest: a
step started inside another records it as its parent. For every step the
timer records wall time from a monotonic clock, the part of it spent in
deliberate rate-limit pauses (wait_ms), how many API responses were
recorded (requests) and, when the step sets it, the rows it wrote.
"""

import logging
import time
from contextlib import contextmanager, nullcontext


class StepTimer:
    def __init__(self, now_fn, clock=time.perf_counter):
        self.now_fn = now_fn
        self.clock = clock
        # Total API responses recorded so far; set once the raw store exists.
        self.requests_fn = lambda: 0
        self.records = []
        self.waited = 0.0
        self._open = []

    def wrap_sleep(self, sleep_fn):
        """Return sleep_fn that also counts the time towards wait_ms."""
        def sleep(seconds):
            self.waited += seconds
            sleep_fn(seconds)

        return sleep

    @contextmanager
    def step(self, name):
        """Time a block; yields the step's record (set "rows" on it)."""
        record = {
            "step": name,
            "parent_step": self._open[-1]["step"] if self._open else None,
            "position": len(self.records),
            "status": "ok",
            "started_at": self.now_fn(),
            "rows": None,
        }
        started = self.clock()
        waited = self.waited
        requests = self.requests_fn()
        self.records.append(record)
        self._open.append(record)

        try:
            yield record
        except BaseException:
            record["status"] = "failed"
            raise
        finally:
            self._open.pop()
            record["duration_ms"] = round((self.clock() - started) * 1000)
            record["wait_ms"] = round((self.waited - waited) * 1000)
            record["requests"] = self.requests_fn() - requests

    def finished_records(self):
        return [record for record in self.records if "duration_ms" in record]


def no_step(name):
    """A stand-in for StepTimer.step where no timer is available."""
    return nullcontext({})


def store_steps(cursor, run_id, records):
    if not records:
        return

    cursor.executemany(
        """
        INSERT INTO run_steps (
            run_id, position, step, parent_step, status, started_at,
            duration_ms, wait_ms, requests, rows_affected
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        [
            (
                run_id,
                record["position"],
                record["step"][:100],
                record["parent_step"],
                record["status"],
                record["started_at"],
                record["duration_ms"],
                min(record["wait_ms"], record["duration_ms"]),
                max(record["requests"], 0),
                record["rows"],
            )
            for record in records
        ]
    )


def save_steps(connection, cursor, run_id, timer):
    """Store a run's step timings; a failure here never fails the run."""
    try:
        store_steps(cursor, run_id, timer.finished_records())
        connection.commit()
    except Exception:
        logging.exception("Could not store step timings for run %s", run_id)
        connection.rollback()
