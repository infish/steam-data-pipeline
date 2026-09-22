import os
import unittest
from argparse import Namespace

from benchmarks.load import load_scenario
from database.mysql_database import get_connection


@unittest.skipUnless(
    os.getenv("RUN_MYSQL_INTEGRATION") == "1",
    "set RUN_MYSQL_INTEGRATION=1 against the isolated marked benchmark DB",
)
class BenchmarkMysqlIntegrationTests(unittest.TestCase):
    def test_atomic_load_and_idempotent_rerun(self):
        args = Namespace(
            scenario="unittest-integration-v1",
            games=20,
            days=2,
            seed=20260922,
            start_date="2025-01-01",
            batch_size=7,
            resume=True,
        )
        load_scenario(args)
        load_scenario(args)
        connection = get_connection()
        cursor = connection.cursor()
        cursor.execute("""
          SELECT COUNT(*), COUNT(DISTINCT s.run_id, s.appid),
                 SUM(d.committed_row_count), COUNT(DISTINCT d.run_id)
          FROM benchmark_scenarios b
          JOIN benchmark_days d ON d.scenario_id=b.scenario_id
          JOIN game_metric_snapshots s ON s.run_id=d.run_id
          WHERE b.scenario_key=%s
        """, (args.scenario,))
        self.assertEqual(cursor.fetchone(), (40, 40, 800, 2))
        # SUM repeats each day's committed count for every joined fact row.
        cursor.execute("""
          SELECT COUNT(*), SUM(committed_row_count), MIN(d.state), MAX(d.state)
          FROM benchmark_days d JOIN benchmark_scenarios b USING (scenario_id)
          WHERE b.scenario_key=%s
        """, (args.scenario,))
        self.assertEqual(cursor.fetchone(), (2, 40, "SUCCESS", "SUCCESS"))
        cursor.close()
        connection.close()


if __name__ == "__main__":
    unittest.main()
