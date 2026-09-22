"""Transactional resumable loader for deterministic benchmark history."""

import hashlib
import json
import os
import signal
import time
from datetime import datetime, timezone

from database.loaders import load_game_metric_snapshots, load_games
from database.mysql_database import get_connection
from benchmarks.generate import (
    APPID_BASE,
    GENERATOR_VERSION,
    batch_checksum,
    snapshot_time,
    synthetic_row,
)

MARKER_VALUE = "steam-data-pipeline-isolated-benchmark"
ALLOWED_DATABASES = {"steam_pipeline_bench"}
SCHEMA_FINGERPRINT = hashlib.sha256(b"V1-V3+benchmark-V1000").hexdigest()
_stop_requested = False


def _utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _signal_stop(signum, frame):
    global _stop_requested
    _stop_requested = True


def validate_target(connection):
    database = connection.database
    configured = {value.strip() for value in os.getenv(
        "BENCHMARK_DATABASE_ALLOWLIST", "steam_pipeline_bench"
    ).split(",") if value.strip()}
    if database not in ALLOWED_DATABASES or database not in configured:
        raise RuntimeError(f"refusing non-benchmark database {database!r}")
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT marker_value FROM benchmark_database_marker WHERE marker_key='purpose'")
        row = cursor.fetchone()
        if row != (MARKER_VALUE,):
            raise RuntimeError("database benchmark marker is missing or invalid")
    finally:
        cursor.close()


def _config_fingerprint(args):
    config = {
        "seed": args.seed, "games": args.games, "start_date": args.start_date,
        "generator_version": GENERATOR_VERSION, "schema_fingerprint": SCHEMA_FINGERPRINT,
    }
    return hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()


def _assert_lock_owned(lock_cursor, lock_name):
    lock_cursor.execute("SELECT IS_USED_LOCK(%s)=CONNECTION_ID()", (lock_name,))
    if lock_cursor.fetchone() != (1,):
        raise RuntimeError("scenario advisory lock connection was lost")


def _get_or_create_scenario(connection, args):
    cursor = connection.cursor(dictionary=True)
    fingerprint = _config_fingerprint(args)
    cursor.execute("SELECT * FROM benchmark_scenarios WHERE scenario_key=%s", (args.scenario,))
    scenario = cursor.fetchone()
    if scenario:
        actual = hashlib.sha256(json.dumps({
            "seed": scenario["seed"], "games": scenario["game_count"],
            "start_date": str(scenario["start_date"]),
            "generator_version": scenario["generator_version"],
            "schema_fingerprint": scenario["schema_fingerprint"],
        }, sort_keys=True).encode()).hexdigest()
        if actual != fingerprint:
            raise RuntimeError("scenario configuration conflict; use a new empty scenario")
        if args.days < scenario["requested_day_count"]:
            raise RuntimeError("a scenario cannot be resumed with fewer requested days")
        cursor.execute("UPDATE benchmark_scenarios SET requested_day_count=%s, state='RUNNING', heartbeat_at=%s WHERE scenario_id=%s",
                       (args.days, _utc_now(), scenario["scenario_id"]))
        connection.commit()
        scenario["requested_day_count"] = args.days
        cursor.close()
        return scenario
    cursor.execute("""
        INSERT INTO benchmark_scenarios
          (scenario_key, seed, generator_version, schema_fingerprint, game_count,
           requested_day_count, start_date, synthetic, state, actual_started_at, heartbeat_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,TRUE,'RUNNING',%s,%s)
    """, (args.scenario, args.seed, GENERATOR_VERSION, SCHEMA_FINGERPRINT,
          args.games, args.days, args.start_date, _utc_now(), _utc_now()))
    scenario_id = cursor.lastrowid
    connection.commit()
    cursor.close()
    return {"scenario_id": scenario_id, "requested_day_count": args.days}


def _get_or_create_day(connection, scenario_id, day_index, observed_at):
    cursor = connection.cursor(dictionary=True)
    cursor.execute("SELECT * FROM benchmark_days WHERE scenario_id=%s AND day_index=%s",
                   (scenario_id, day_index))
    day = cursor.fetchone()
    if day:
        cursor.close()
        return day
    cursor.execute("INSERT INTO pipeline_runs (started_at,status) VALUES (%s,'RUNNING')", (observed_at,))
    run_id = cursor.lastrowid
    cursor.execute("INSERT INTO benchmark_days (scenario_id,day_index,run_id,state) VALUES (%s,%s,%s,'RUNNING')",
                   (scenario_id, day_index, run_id))
    connection.commit()
    cursor.close()
    return {"scenario_id": scenario_id, "day_index": day_index, "run_id": run_id,
            "last_committed_appid": None, "committed_row_count": 0, "state": "RUNNING"}


def _commit_batch(connection, scenario_id, day_index, run_id, rows, observed_at, generation_ms):
    checksum = batch_checksum(rows)
    first_appid, last_appid = rows[0]["appid"], rows[-1]["appid"]
    cursor = connection.cursor(dictionary=True)
    cursor.execute("SELECT checksum,expected_count FROM benchmark_batches WHERE scenario_id=%s AND day_index=%s AND first_appid=%s",
                   (scenario_id, day_index, first_appid))
    existing = cursor.fetchone()
    if existing:
        if existing["checksum"] != checksum or existing["expected_count"] != len(rows):
            raise RuntimeError("completed batch checksum conflict")
        cursor.close()
        return 0
    started = time.monotonic()
    try:
        load_games(cursor, rows, observed_at)
        load_game_metric_snapshots(cursor, rows, run_id, observed_at)
        insert_ms = (time.monotonic() - started) * 1000
        cursor.execute("""
          INSERT INTO benchmark_batches
            (scenario_id,day_index,first_appid,last_appid,expected_count,checksum,
             generation_ms,insert_ms,committed_at)
          VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (scenario_id, day_index, first_appid, last_appid, len(rows), checksum,
              generation_ms, insert_ms, _utc_now()))
        cursor.execute("""
          UPDATE benchmark_days SET last_committed_appid=%s,
            committed_row_count=committed_row_count+%s
          WHERE scenario_id=%s AND day_index=%s
        """, (last_appid, len(rows), scenario_id, day_index))
        cursor.execute("UPDATE benchmark_scenarios SET heartbeat_at=%s WHERE scenario_id=%s",
                       (_utc_now(), scenario_id))
        connection.commit()
        return len(rows)
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()


def load_scenario(args):
    signal.signal(signal.SIGTERM, _signal_stop)
    connection = get_connection()
    lock_connection = get_connection()
    lock_cursor = lock_connection.cursor()
    try:
        validate_target(connection)
        validate_target(lock_connection)
        lock_name = f"steam-bench:{args.scenario}"[:64]
        lock_cursor.execute("SELECT GET_LOCK(%s, 0)", (lock_name,))
        if lock_cursor.fetchone() != (1,):
            raise RuntimeError("another writer owns the scenario lock")
        scenario = _get_or_create_scenario(connection, args)
        scenario_id = scenario["scenario_id"]
        for day_index in range(args.days):
            if _stop_requested:
                break
            observed_at = snapshot_time(args.start_date, day_index)
            day = _get_or_create_day(connection, scenario_id, day_index, observed_at)
            if day["state"] == "SUCCESS":
                continue
            start_game = 1
            if day.get("last_committed_appid"):
                start_game = day["last_committed_appid"] - APPID_BASE + 1
            for first_game in range(start_game, args.games + 1, args.batch_size):
                if _stop_requested:
                    break
                _assert_lock_owned(lock_cursor, lock_name)
                generated_at = time.monotonic()
                rows = [synthetic_row(args.seed, game_index, day_index)
                        for game_index in range(
                            first_game,
                            min(args.games + 1, first_game + args.batch_size),
                        )]
                generation_ms = (time.monotonic() - generated_at) * 1000
                _commit_batch(connection, scenario_id, day_index, day["run_id"], rows,
                              observed_at, generation_ms)
                test_delay_ms = int(os.getenv("BENCHMARK_TEST_BATCH_DELAY_MS", "0"))
                if test_delay_ms:
                    time.sleep(test_delay_ms / 1000)
            if _stop_requested:
                break
            cursor = connection.cursor()
            cursor.execute("SELECT COUNT(*) FROM game_metric_snapshots WHERE run_id=%s", (day["run_id"],))
            exact = cursor.fetchone()[0]
            if exact != args.games:
                raise RuntimeError(f"day {day_index} has {exact} rows, expected {args.games}")
            cursor.execute("UPDATE benchmark_days SET state='SUCCESS',completed_at=%s WHERE scenario_id=%s AND day_index=%s",
                           (_utc_now(), scenario_id, day_index))
            cursor.execute("UPDATE pipeline_runs SET status='SUCCESS',finished_at=%s,rows_extracted=%s,rows_loaded=%s WHERE run_id=%s",
                           (_utc_now(), args.games, args.games, day["run_id"]))
            connection.commit()
            cursor.close()
        if not _stop_requested:
            cursor = connection.cursor()
            cursor.execute("UPDATE benchmark_scenarios SET state='SUCCESS',actual_finished_at=%s,heartbeat_at=%s WHERE scenario_id=%s",
                           (_utc_now(), _utc_now(), scenario_id))
            connection.commit()
            cursor.close()
    finally:
        try:
            lock_cursor.execute("SELECT RELEASE_LOCK(%s)", (f"steam-bench:{args.scenario}"[:64],))
            lock_cursor.fetchone()
        finally:
            lock_cursor.close()
            lock_connection.close()
            connection.close()
