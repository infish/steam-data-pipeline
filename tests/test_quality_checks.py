import json
import re
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path

from marts.tests import MART_TESTS
from jobs.registry import JOBS
from quality.checks import (
    EXPECTED_INTERVAL_HOURS,
    INVARIANTS,
    REQUIRED_FIELDS,
    SOURCE_JOBS,
    VOLUME_TABLES,
    evaluate_freshness,
    evaluate_staleness,
    evaluate_volume,
    missing_paths,
    reported_by,
    result,
    runs_after,
)


ROOT = Path(__file__).parents[1]
FIXTURES = ROOT / "tests" / "fixtures" / "sources"
NOW = datetime(2026, 10, 12, 12, 0)

# One recorded response per source checked for schema drift.
FIXTURE_FOR_SOURCE = {
    "steamspy_all": "steamspy_all/page0_tracked.json",
    "steamspy_appdetails": "steamspy_appdetails/730.json",
    "valve_top_concurrent": "top_concurrent/all.json",
    "valve_most_played": "most_played/all.json",
    "valve_current_players": "current_players/730.json",
    "steam_review_summary": "review_summary/730.json",
    "steam_review_histogram": "review_histogram/730.json",
    "store_appdetails": "store_appdetails/730.json",
    "store_prices": "store_price_batch/batch.json",
    "steam_news": "news/730.json",
}


class FreshnessTests(unittest.TestCase):
    def test_within_one_and_a_half_intervals_passes(self):
        row = evaluate_freshness(
            "valve_top_concurrent", NOW - timedelta(minutes=80), NOW
        )
        self.assertEqual(row["status"], "pass")
        self.assertEqual(row["observed_value"], "1.33 h")

    def test_older_fails(self):
        row = evaluate_freshness(
            "steamspy_all", NOW - timedelta(hours=37), NOW
        )
        self.assertEqual(row["status"], "fail")
        self.assertEqual(row["threshold"], "newest success younger than 36 h")

    def test_never_fetched_is_a_warning(self):
        row = evaluate_freshness("steam_news", None, NOW)
        self.assertEqual(
            (row["status"], row["observed_value"]),
            ("warn", "never fetched")
        )


class VolumeTests(unittest.TestCase):
    day = date(2026, 10, 11)

    def counts(self, today, history):
        counts = {
            self.day - timedelta(days=offset): value
            for offset, value in enumerate(history, start=1)
        }
        counts[self.day] = today
        return counts

    def test_within_thirty_percent_passes(self):
        row = evaluate_volume(
            "player_observations", self.day,
            self.counts(1300, [1000, 1100, 1200, 1000, 900, 1000, 1050])
        )
        self.assertEqual(row["status"], "pass")
        self.assertEqual(row["observed_value"], "1300 rows (+30.0%)")

    def test_outside_thirty_percent_warns(self):
        row = evaluate_volume(
            "player_observations", self.day,
            self.counts(500, [1000] * 7)
        )
        self.assertEqual(row["status"], "warn")

    def test_no_rows_fails(self):
        row = evaluate_volume(
            "player_observations", self.day,
            self.counts(0, [1000] * 7)
        )
        self.assertEqual(row["status"], "fail")

    def test_too_little_history_is_not_judged(self):
        self.assertIsNone(evaluate_volume(
            "player_observations", self.day,
            self.counts(500, [1000, 1000])
        ))


class StalenessTests(unittest.TestCase):
    def test_thresholds(self):
        self.assertEqual(evaluate_staleness(1000, 600)["status"], "pass")
        self.assertEqual(evaluate_staleness(1000, 960)["status"], "warn")
        self.assertEqual(evaluate_staleness(1000, 1000)["status"], "fail")


class SchemaDriftTests(unittest.TestCase):
    def test_paths(self):
        payload = {"a": {"items": [{"x": 1}, {"x": 2, "y": 3}]},
                   "map": {"1": {"ok": 1}, "2": {}}}

        self.assertEqual(
            missing_paths(payload, [
                "a.items[].x", "a.items[].y", "map.*.ok", "b", "a.items"
            ]),
            ["a.items[].y", "map.*.ok", "b"]
        )

    def test_recorded_responses_have_every_required_field(self):
        self.assertEqual(sorted(FIXTURE_FOR_SOURCE), sorted(REQUIRED_FIELDS))

        for source, path in FIXTURE_FOR_SOURCE.items():
            with self.subTest(source=source):
                payload = json.loads((FIXTURES / path).read_text())
                self.assertEqual(
                    missing_paths(payload, REQUIRED_FIELDS[source]), []
                )

    def test_a_renamed_field_is_reported(self):
        payload = json.loads((FIXTURES / "top_concurrent/all.json").read_text())
        payload["response"]["ranks"][0]["players"] = (
            payload["response"]["ranks"][0].pop("concurrent_in_game")
        )

        self.assertEqual(
            missing_paths(payload, REQUIRED_FIELDS["valve_top_concurrent"]),
            ["response.ranks[].concurrent_in_game"]
        )


class CoverageTests(unittest.TestCase):
    def test_every_part_2_table_has_a_data_quality_check(self):
        created = set()

        for path in (ROOT / "db" / "migrations").glob("V*__*.sql"):
            version = int(path.name[1:].split("__")[0])

            if version >= 5:
                created |= set(re.findall(
                    r"CREATE TABLE (\w+)", path.read_text()
                ))

        covered = (
            set(VOLUME_TABLES)
            | {target for _, target, _, _, _ in INVARIANTS}
            | set(MART_TESTS)
        )
        self.assertEqual(sorted(created - covered), [])
        self.assertTrue(set(EXPECTED_INTERVAL_HOURS) <= set(REQUIRED_FIELDS))


class OwnershipTests(unittest.TestCase):
    def test_every_check_belongs_to_a_job_or_to_none(self):
        owners = (
            {job for _, job in VOLUME_TABLES.values()}
            | {job for _, _, _, job, _ in INVARIANTS}
            | set(SOURCE_JOBS.values())
        )
        self.assertTrue(owners - {None} <= set(JOBS))
        self.assertTrue(
            set(EXPECTED_INTERVAL_HOURS) | set(REQUIRED_FIELDS)
            <= set(SOURCE_JOBS)
        )

    def test_a_job_runs_its_own_and_the_global_checks(self):
        self.assertTrue(runs_after("catalog_daily", "catalog_daily"))
        self.assertTrue(runs_after(None, "players_hourly"))
        self.assertFalse(runs_after("catalog_daily", "players_hourly"))
        # Reprocessing and tests run every check.
        self.assertTrue(runs_after("catalog_daily", None))

    def test_only_the_runs_own_failures_become_warnings(self):
        catalog_check = result(
            "catalog_games_have_developers", "game_developers", "fail", 3,
            "0 violating rows", job="catalog_daily"
        )
        stale_catalog = result(
            "freshness", "steamspy_all", "fail", "40 h", "< 36 h",
            job="catalog_daily"
        )
        stuck_runs = result(
            "no_stuck_runs", "pipeline_runs", "fail", 1, "0 violating rows"
        )

        self.assertTrue(reported_by(catalog_check, "catalog_daily"))
        self.assertFalse(reported_by(catalog_check, "players_hourly"))
        # Freshness runs after every job but warns only its own job's runs.
        self.assertFalse(reported_by(stale_catalog, "players_hourly"))
        self.assertTrue(reported_by(stale_catalog, "catalog_daily"))
        self.assertTrue(reported_by(stuck_runs, "reviews_daily"))


if __name__ == "__main__":
    unittest.main()
