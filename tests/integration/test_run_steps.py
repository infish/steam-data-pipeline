import unittest
from unittest.mock import patch

from .base import IntegrationTestCase


class RunStepsIntegrationTests(IntegrationTestCase):
    def steps(self, run_id):
        return self.query(
            "SELECT * FROM run_steps WHERE run_id = %s ORDER BY position",
            (run_id,)
        )

    def test_a_run_stores_the_time_of_every_step(self):
        run_id = self.run_job("catalog_daily")
        steps = self.steps(run_id)
        by_name = {step["step"]: step for step in steps}

        self.assertEqual(
            [step["step"] for step in steps if step["parent_step"] is None],
            ["setup", "job", "finish", "marts", "checks"]
        )
        self.assertEqual(
            [step["position"] for step in steps], list(range(len(steps)))
        )
        self.assertTrue({
            "fetch_catalog", "validate", "load_games", "load_company_links",
            "add_tracked", "prices",
        } <= {
            step["step"] for step in steps if step["parent_step"] == "job"
        })
        self.assertEqual(by_name["01_mart_player_trends"]["parent_step"],
                         "marts")
        self.assertEqual(by_name["mart_tests"]["parent_step"], "marts")
        self.assertEqual(by_name["freshness"]["parent_step"], "checks")
        self.assertGreater(by_name["load_games"]["rows_affected"], 0)
        self.assertTrue(all(step["status"] == "ok" for step in steps))

        # Every API response of the run is counted by exactly one job step.
        (raw,) = self.query(
            "SELECT COUNT(*) AS n FROM raw_api_responses WHERE run_id = %s",
            (run_id,)
        )
        self.assertEqual(by_name["job"]["requests"], raw["n"])
        self.assertEqual(
            sum(step["requests"] for step in steps
                if step["parent_step"] == "job"),
            raw["n"]
        )

    def test_a_failed_run_stores_its_steps_up_to_the_failure(self):
        with patch(
            "jobs.catalog.load_company_links",
            side_effect=RuntimeError("error 1869")
        ):
            with self.assertRaises(RuntimeError):
                self.run_job("catalog_daily")

        (run,) = self.query(
            "SELECT run_id, status FROM pipeline_runs"
            " WHERE job_name = 'catalog_daily'"
        )
        self.assertEqual(run["status"], "FAILED")
        statuses = {
            step["step"]: step["status"] for step in self.steps(run["run_id"])
        }

        self.assertEqual(statuses["load_company_links"], "failed")
        self.assertEqual(statuses["job"], "failed")
        self.assertEqual(statuses["load_games"], "ok")
        self.assertNotIn("marts", statuses)
        self.assertNotIn("store_details_and_tags", statuses)


if __name__ == "__main__":
    unittest.main()
