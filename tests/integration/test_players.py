import calendar
import unittest
from datetime import date, datetime, timedelta

from jobs.base import utc_now

from .base import IntegrationTestCase


def chart_at(measured_at, players):
    return {
        "valve_top_concurrent": {
            "response": {
                "last_update": calendar.timegm(measured_at.timetuple()),
                "ranks": [
                    {"rank": 1, "appid": 730, "concurrent_in_game": players},
                ],
            }
        }
    }


class PlayersJobIntegrationTests(IntegrationTestCase):
    def setUp(self):
        super().setUp()
        self.run_job("catalog_daily")

    def run_players_at(self, collected_at, players):
        return self.run_job(
            "players_hourly",
            chart_at(collected_at - timedelta(minutes=5), players),
            now_fn=lambda: collected_at
        )

    def daily(self, appid):
        return {
            row["observation_date"]: row
            for row in self.query(
                "SELECT * FROM player_daily WHERE appid = %s",
                (appid,)
            )
        }

    def test_hourly_runs_feed_the_daily_rollup(self):
        day = datetime(2026, 10, 1)

        for hour, players in ((10, 100), (11, 300), (12, 200)):
            self.run_players_at(day.replace(hour=hour, minute=7), players)

        (row,) = self.daily(730).values()
        self.assertEqual(row["observation_date"], date(2026, 10, 1))
        self.assertEqual(row["peak_players"], 300)
        self.assertEqual(row["min_players"], 100)
        self.assertEqual(float(row["avg_players"]), 200.0)
        self.assertEqual(row["observation_count"], 3)
        self.assertEqual(row["best_rank"], 1)

        # Tracked games outside the chart are rolled up from collection time.
        (portal,) = self.daily(620).values()
        self.assertEqual(portal["observation_count"], 3)
        self.assertEqual(portal["peak_players"], 4000)

        # A new UTC day starts a new row and leaves the finished day alone.
        self.run_players_at(datetime(2026, 10, 2, 0, 7), 50)
        days = self.daily(730)
        self.assertEqual(sorted(days), [date(2026, 10, 1), date(2026, 10, 2)])
        self.assertEqual(days[date(2026, 10, 1)]["observation_count"], 3)
        self.assertEqual(days[date(2026, 10, 2)]["peak_players"], 50)

    def test_old_hourly_rows_are_deleted_but_their_rollups_kept(self):
        long_ago = utc_now() - timedelta(days=100)
        old_run = self.run_players_at(long_ago, 100)
        old_days = self.query("SELECT * FROM player_daily")

        new_run = self.run_players_at(utc_now(), 200)

        run_ids = {
            row["run_id"]
            for row in self.query("SELECT run_id FROM player_observations")
        }
        self.assertEqual(run_ids, {new_run})
        self.assertNotIn(old_run, run_ids)

        rollups = self.query("SELECT * FROM player_daily")
        for old_day in old_days:
            self.assertIn(old_day, rollups)

        self.assertEqual(len(rollups), 2 * len(old_days))

    def test_the_daily_leaderboard_is_upserted(self):
        first = self.run_job("players_hourly")
        second = self.run_job("players_hourly")

        self.assertEqual(self.count("most_played_daily"), 100)
        self.assertEqual(
            {
                row["run_id"]
                for row in self.query("SELECT run_id FROM most_played_daily")
            },
            {second}
        )
        self.assertNotEqual(first, second)


if __name__ == "__main__":
    unittest.main()
