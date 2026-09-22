"""Execute and persist the fixed capacity-query workload."""

import argparse
import hashlib
import json
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path

from benchmarks.generate import APPID_BASE
from benchmarks.load import SCHEMA_FINGERPRINT, validate_target
from database.mysql_database import get_connection


QUERIES = {
    "history_one_game": "SELECT s.snapshot_time,s.estimated_owners,s.total_reviews,s.ccu FROM game_metric_snapshots s JOIN benchmark_days d ON d.run_id=s.run_id WHERE d.scenario_id=%(scenario_id)s AND s.appid=%(appid)s ORDER BY s.snapshot_time DESC LIMIT 365",
    "latest_top_25": "SELECT s.appid,g.name,s.estimated_owners FROM game_metric_snapshots s JOIN games g ON g.appid=s.appid WHERE s.run_id=%(latest_run_id)s ORDER BY s.estimated_owners DESC LIMIT 25",
    "latest_publisher_totals": "SELECT g.publisher,COUNT(*),SUM(s.estimated_owners) FROM game_metric_snapshots s JOIN games g ON g.appid=s.appid WHERE s.run_id=%(latest_run_id)s GROUP BY g.publisher ORDER BY 3 DESC",
    "daily_loaded_rows": "SELECT day_index,committed_row_count FROM benchmark_days WHERE scenario_id=%(scenario_id)s ORDER BY day_index",
    "one_game_deltas": "WITH selected AS (SELECT s.snapshot_time,s.estimated_owners,s.total_reviews FROM game_metric_snapshots s JOIN benchmark_days d ON d.run_id=s.run_id WHERE d.scenario_id=%(scenario_id)s AND s.appid=%(appid)s ORDER BY s.snapshot_time DESC LIMIT 366), deltas AS (SELECT snapshot_time,estimated_owners,estimated_owners-LAG(estimated_owners) OVER (ORDER BY snapshot_time),total_reviews-LAG(total_reviews) OVER (ORDER BY snapshot_time) FROM selected) SELECT * FROM deltas ORDER BY snapshot_time DESC LIMIT 365",
    "exact_fact_count": "SELECT COUNT(*) FROM game_metric_snapshots s JOIN benchmark_days d ON d.run_id=s.run_id WHERE d.scenario_id=%(scenario_id)s",
}


def digest_rows(rows):
    return hashlib.sha256(json.dumps(rows, default=str, separators=(",", ":")).encode()).hexdigest()


def percentile(values, fraction):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(len(ordered) * fraction))]


def run(args):
    connection = get_connection()
    validate_target(connection)
    cursor = connection.cursor(dictionary=True)
    cursor.execute("SELECT * FROM benchmark_scenarios WHERE scenario_key=%s AND state='SUCCESS'", (args.scenario,))
    scenario = cursor.fetchone()
    if not scenario:
        raise RuntimeError("scenario is not complete")
    cursor.execute("SELECT run_id FROM benchmark_days WHERE scenario_id=%s AND state='SUCCESS' ORDER BY day_index DESC LIMIT 1", (scenario["scenario_id"],))
    latest_run = cursor.fetchone()["run_id"]
    parameters = {"appid": APPID_BASE + args.game_index, "latest_run_id": latest_run,
                  "scenario_id": scenario["scenario_id"]}
    output = {"scenario": args.scenario, "recorded_at": datetime.now(timezone.utc).isoformat(),
              "schema_fingerprint": SCHEMA_FINGERPRINT, "queries": {}}
    for label, sql in QUERIES.items():
        repetitions = 1 if label == "exact_fact_count" else args.repetitions
        cursor.execute("SET SESSION MAX_EXECUTION_TIME=%s", (args.count_timeout_ms if label == "exact_fact_count" else args.timeout_ms,))
        cursor.execute("EXPLAIN " + sql, parameters)
        plan = cursor.fetchall()
        analyze = None
        if label != "exact_fact_count":
            cursor.execute("EXPLAIN ANALYZE " + sql, parameters)
            analyze = cursor.fetchall()
        times, first_digest, row_count, first_ms = [], None, 0, None
        for iteration in range(repetitions + (0 if label == "exact_fact_count" else 1)):
            started = time.perf_counter()
            cursor.execute(sql, parameters)
            rows = cursor.fetchall()
            elapsed = (time.perf_counter() - started) * 1000
            cache_label = "count" if label == "exact_fact_count" else ("first" if iteration == 0 else "warm")
            result_digest = digest_rows(rows)
            if first_digest is not None and result_digest != first_digest:
                raise RuntimeError(f"non-deterministic result for {label}")
            first_digest, row_count = result_digest, len(rows)
            if cache_label == "warm" or label == "exact_fact_count":
                times.append(elapsed)
            elif cache_label == "first":
                first_ms = elapsed
            cursor.execute("""
              INSERT INTO benchmark_results
                (scenario_id,benchmark_identity,schema_fingerprint,config_json,
                 query_label,iteration,cache_label,elapsed_ms,result_row_count,
                 result_digest,recorded_at)
              VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (scenario["scenario_id"], args.identity, SCHEMA_FINGERPRINT,
                  json.dumps({"timeout_ms": args.timeout_ms}), label, iteration,
                  cache_label, elapsed, row_count, result_digest,
                  datetime.now(timezone.utc).replace(tzinfo=None)))
            connection.commit()
        output["queries"][label] = {"sql": sql, "parameters": parameters,
            "plan": plan, "explain_analyze": analyze, "first_ms": first_ms,
            "repetitions": len(times), "row_count": row_count,
            "result_digest": first_digest, "median_ms": statistics.median(times),
            "p95_ms": percentile(times, .95), "samples_ms": times}
    target = Path(args.output)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(output, indent=2, default=str) + "\n")
    cursor.close()
    connection.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", default="scale-v1")
    parser.add_argument("--identity", required=True)
    parser.add_argument("--game-index", type=int, default=1)
    parser.add_argument("--repetitions", type=int, default=30)
    parser.add_argument("--timeout-ms", type=int, default=30000)
    parser.add_argument("--count-timeout-ms", type=int, default=120000)
    parser.add_argument("--output", required=True)
    run(parser.parse_args(argv))


if __name__ == "__main__":
    main()
