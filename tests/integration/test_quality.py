import calendar
import unittest
from datetime import datetime, timedelta

from .base import CANNED_RESPONSES, IntegrationTestCase


NOW = datetime(2026, 10, 12, 1, 0)


def at(moment):
    return lambda: moment


def chart(measured_at, players=1_500_000, field="concurrent_in_game"):
    return {
        "valve_top_concurrent": {
            "response": {
                "last_update": calendar.timegm(measured_at.timetuple()),
                "ranks": [{"rank": 1, "appid": 730, field: players}],
            }
        }
    }


class QualityIntegrationTests(IntegrationTestCase):
    def results(self, run_id, check_name=None):
        sql = "SELECT * FROM data_quality_results WHERE run_id = %s"
        params = [run_id]

        if check_name:
            sql += " AND check_name = %s"
            params.append(check_name)

        return {
            row["target"]: row
            for row in self.query(sql + " ORDER BY result_id", tuple(params))
        }

    def test_every_run_stores_its_check_results(self):
        run_id = self.run_job("catalog_daily", now_fn=at(NOW))

        rows = self.query(
            "SELECT check_name, COUNT(*) AS n FROM data_quality_results"
            " WHERE run_id = %s GROUP BY check_name",
            (run_id,)
        )
        checks = {row["check_name"]: row["n"] for row in rows}

        self.assertEqual(checks["freshness"], 8)
        self.assertIn("schema_drift", checks)
        self.assertIn("one_current_price_version", checks)
        # The Phase 16 mart tests are stored here too.
        self.assertIn("mart_unique_key", checks)
        self.assertIn("mart_not_empty", checks)

        freshness = self.results(run_id, "freshness")
        self.assertEqual(freshness["steamspy_all"]["status"], "pass")
        self.assertEqual(freshness["valve_top_concurrent"]["status"], "warn")
        self.assertEqual(
            freshness["valve_top_concurrent"]["observed_value"],
            "never fetched"
        )
        self.assertIsNone(self.run_row(run_id)["warning_message"])

    def test_stale_source_fails_freshness_after_any_job(self):
        self.run_job("catalog_daily", now_fn=at(NOW - timedelta(hours=3)))
        self.run_job("players_hourly", now_fn=at(NOW - timedelta(hours=3)))
        run_id = self.run_job("reviews_daily", now_fn=at(NOW))

        # Freshness is the watchdog: a reviews run notices that the players
        # job stopped...
        freshness = self.results(run_id, "freshness")
        chart_freshness = freshness["valve_top_concurrent"]
        self.assertEqual(chart_freshness["status"], "fail")
        self.assertEqual(chart_freshness["observed_value"], "3.0 h")
        self.assertEqual(freshness["steam_review_summary"]["status"], "pass")
        # ...but does not take the warning: the reviews run is fine.
        self.assertIsNone(self.run_row(run_id)["warning_message"])

    def test_a_job_runs_only_its_own_and_the_global_checks(self):
        self.run_job("catalog_daily", now_fn=at(NOW))
        self.execute(
            "UPDATE game_price_history SET is_current = FALSE,"
            " valid_to = valid_from WHERE appid = 620"
        )
        run_id = self.run_job("reviews_daily", now_fn=at(NOW))
        checks = {
            row["check_name"]
            for row in self.query(
                "SELECT DISTINCT check_name FROM data_quality_results"
                " WHERE run_id = %s",
                (run_id,)
            )
        }

        # The broken price history is the catalog job's to report.
        self.assertNotIn("one_current_price_version", checks)
        self.assertNotIn("catalog_games_have_developers", checks)
        self.assertIn("single_partial_month", checks)
        self.assertIn("no_stuck_runs", checks)
        self.assertEqual(len(self.results(run_id, "freshness")), 8)
        self.assertNotIn(
            "steamspy_all", self.results(run_id, "schema_drift")
        )
        self.assertIsNone(self.run_row(run_id)["warning_message"])

    def test_volume_outside_the_seven_day_median_warns(self):
        self.run_job("catalog_daily", now_fn=at(NOW - timedelta(days=9)))
        yesterday = NOW.replace(hour=0) - timedelta(days=1)

        # One players run (3 observations) a day for the week before...
        for days_ago in range(2, 9):
            moment = NOW.replace(hour=12) - timedelta(days=days_ago)
            self.run_job("players_hourly", chart(moment), now_fn=at(moment))

        # ...and three runs yesterday: three times the usual volume.
        for hour in (6, 12, 18):
            moment = yesterday.replace(hour=hour)
            self.run_job("players_hourly", chart(moment), now_fn=at(moment))

        run_id = self.run_job("players_hourly", now_fn=at(NOW))

        volume = self.results(run_id, "volume")["player_observations"]
        self.assertEqual(volume["status"], "warn")
        self.assertEqual(volume["observed_value"], "9 rows (+200.0%)")
        self.assertIn("median (3)", volume["threshold"])

    def test_unchanged_steamspy_values_fail_staleness(self):
        self.run_job("catalog_daily", now_fn=at(NOW - timedelta(days=1)))
        run_id = self.run_job("catalog_daily", now_fn=at(NOW))

        (staleness,) = self.results(
            run_id, "steamspy_unchanged_fields"
        ).values()
        self.assertEqual(staleness["status"], "fail")
        self.assertEqual(staleness["observed_value"], "100.0%")
        self.assertIn(
            "steamspy_unchanged_fields failed",
            self.run_row(run_id)["warning_message"]
        )

        changed = {
            appid: {**row, "positive": row["positive"] + 1, "ccu": 1}
            for appid, row in CANNED_RESPONSES["steamspy_all"].items()
        }
        run_id = self.run_job(
            "catalog_daily",
            {"steamspy_all": changed},
            now_fn=at(NOW + timedelta(days=1))
        )
        (staleness,) = self.results(
            run_id, "steamspy_unchanged_fields"
        ).values()
        self.assertEqual(staleness["status"], "pass")

    def test_repeated_chart_payloads_warn_on_duplicate_rate(self):
        self.run_job("catalog_daily", now_fn=at(NOW - timedelta(hours=3)))
        frozen = chart(NOW - timedelta(hours=3))

        for hours_ago in (2, 1, 0):
            run_id = self.run_job(
                "players_hourly",
                frozen,
                now_fn=at(NOW - timedelta(hours=hours_ago))
            )

        duplicates = self.results(run_id, "raw_duplicate_rate")
        self.assertEqual(duplicates["valve_top_concurrent"]["status"], "warn")
        self.assertEqual(
            duplicates["valve_top_concurrent"]["observed_value"],
            "66.7% of 3"
        )
        # Repeats are normal for the daily leaderboard: information only.
        self.assertEqual(duplicates["valve_most_played"]["status"], "pass")

    def test_a_renamed_field_fails_schema_drift(self):
        self.run_job("catalog_daily", now_fn=at(NOW))
        run_id = self.run_job(
            "players_hourly",
            chart(NOW, field="players_in_game"),
            now_fn=at(NOW)
        )

        drift = self.results(run_id, "schema_drift")
        self.assertEqual(drift["valve_top_concurrent"]["status"], "fail")
        self.assertEqual(
            drift["valve_top_concurrent"]["details"],
            "response.ranks[].concurrent_in_game"
        )
        # SteamSpy payloads are checked after catalog runs, not here.
        self.assertNotIn("steamspy_all", drift)
        self.assertIn(
            "schema_drift failed on valve_top_concurrent",
            self.run_row(run_id)["warning_message"]
        )

    def test_a_broken_invariant_fails_without_deleting_data(self):
        self.run_job("catalog_daily", now_fn=at(NOW))
        # An old day's rollup that cannot be right: minimum above peak.
        self.execute(
            "INSERT INTO player_daily (appid, observation_date,"
            " peak_players, avg_players, min_players, observation_count,"
            " updated_at) VALUES (730, '2026-10-01', 10, 20, 30, 1, %s)",
            (NOW,)
        )
        games = self.count("games")

        run_id = self.run_job("players_hourly", chart(NOW), now_fn=at(NOW))

        (invariant,) = self.results(
            run_id, "consistent_daily_rollup"
        ).values()
        self.assertEqual(invariant["status"], "fail")
        self.assertEqual(invariant["observed_value"], "1")
        self.assertIn(
            "consistent_daily_rollup failed on player_daily",
            self.run_row(run_id)["warning_message"]
        )
        # The broken row is reported, not deleted.
        (kept,) = self.query(
            "SELECT COUNT(*) AS n FROM player_daily"
            " WHERE observation_date = '2026-10-01'"
        )
        self.assertEqual(kept["n"], 1)
        self.assertEqual(self.count("games"), games)
        self.assertEqual(self.run_row(run_id)["status"], "SUCCESS")


if __name__ == "__main__":
    unittest.main()
