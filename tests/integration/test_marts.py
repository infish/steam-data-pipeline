import calendar
import json
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path

from database.mysql_database import get_connection
from marts.build import build_marts
from marts.tests import MART_TESTS, run_mart_tests

from .base import IntegrationTestCase, review_summary


FIXTURES = Path(__file__).parents[1] / "fixtures" / "sources"
FIRST_DAY = date(2026, 9, 24)
DAYS = 14
FACTORIO_TOP100_DAYS = 5


def chart(measured_at, ranks):
    return {
        "valve_top_concurrent": {
            "response": {
                "last_update": calendar.timegm(measured_at.timetuple()),
                "ranks": [
                    {"rank": rank, "appid": appid, "concurrent_in_game": count}
                    for appid, rank, count in ranks
                ],
            }
        }
    }


def counter_strike_players(day_index):
    return 1_000_000 + day_index * 10_000


class MartIntegrationTests(IntegrationTestCase):
    def setUp(self):
        super().setUp()
        start = datetime.combine(FIRST_DAY, datetime.min.time())
        self.run_job("catalog_daily", now_fn=lambda: start)

    def run_players(self, collected_at, ranks):
        return self.run_job(
            "players_hourly",
            chart(collected_at - timedelta(minutes=5), ranks),
            now_fn=lambda: collected_at
        )

    def seed_daily_players(self):
        for index in range(DAYS):
            day = FIRST_DAY + timedelta(days=index)
            ranks = [(730, 1, counter_strike_players(index))]

            if index < FACTORIO_TOP100_DAYS:
                ranks.append((427520, 98, 18_000))

            self.run_players(
                datetime.combine(day, datetime.min.time()).replace(hour=18),
                ranks
            )

    def seed_hourly_day(self, day, peak_utc_hour):
        # Collected at :05, so Valve's measurement time is on the hour.
        for hour in range(6, 19):
            players = 2_000_000 if hour == peak_utc_hour else 1_000_000
            self.run_players(
                datetime.combine(day, datetime.min.time()).replace(
                    hour=hour,
                    minute=5
                ),
                [(730, 1, players)]
            )

    def mart(self, table, where="", params=()):
        return self.query(f"SELECT * FROM {table} {where}", params)

    def build(self):
        connection = get_connection()

        try:
            return build_marts(connection)
        finally:
            connection.close()

    def checksums(self):
        connection = get_connection()

        try:
            cursor = connection.cursor()
            cursor.execute("CHECKSUM TABLE " + ", ".join(MART_TESTS))
            return dict(cursor.fetchall())
        finally:
            connection.close()

    def test_marts_build_from_collected_data(self):
        self.seed_daily_players()
        oct_1 = datetime(2026, 10, 1, 18, 30)
        self.run_job("reviews_daily", now_fn=lambda: oct_1)
        self.run_job(
            "reviews_daily",
            {"steam_review_summary:730": review_summary(7100, 1000)},
            now_fn=lambda: oct_1 + timedelta(days=1)
        )

        # Player trends: one row per game per day.
        trends = {
            row["observation_date"]: row
            for row in self.mart(
                "mart_player_trends", "WHERE appid = 730"
            )
        }
        self.assertEqual(len(trends), DAYS)
        day_7 = trends[FIRST_DAY + timedelta(days=7)]
        expected_avg = sum(counter_strike_players(i) for i in range(1, 8)) / 7
        self.assertAlmostEqual(float(day_7["avg_players_7d"]), expected_avg, 1)
        self.assertAlmostEqual(
            float(day_7["peak_change_pct_dod"]),
            round(10_000 / counter_strike_players(6) * 100, 2)
        )
        self.assertAlmostEqual(
            float(day_7["peak_change_pct_wow"]),
            round(70_000 / counter_strike_players(0) * 100, 2)
        )
        self.assertEqual(day_7["days_in_top100"], 8)
        self.assertGreater(day_7["volatility_7d_pct"], 0)
        self.assertIsNone(trends[FIRST_DAY]["peak_change_pct_dod"])

        factorio = {
            row["observation_date"]: row
            for row in self.mart(
                "mart_player_trends", "WHERE appid = 427520"
            )
        }
        last_top100 = FIRST_DAY + timedelta(days=FACTORIO_TOP100_DAYS - 1)
        self.assertFalse(factorio[FIRST_DAY]["entered_top100"])
        self.assertTrue(factorio[last_top100]["exited_top100"])
        self.assertEqual(factorio[last_top100]["days_in_top100"], 5)
        self.assertEqual(
            sum(row["exited_top100"] for row in factorio.values()),
            1
        )

        # Review trends: new reviews and 30-day share from the histogram.
        (oct_2,) = self.mart(
            "mart_review_trends",
            "WHERE appid = 730 AND observation_date = '2026-10-02'"
        )
        self.assertEqual(oct_2["new_reviews"], 100)
        # The 30 days ending 2 Oct start on 3 Sep; the recorded buckets run
        # from 2 Sep to 1 Oct.
        recent = [
            bucket
            for bucket in json.loads(
                (FIXTURES / "review_histogram/730.json").read_text()
            )["results"]["recent"]
            if bucket["date"] >= calendar.timegm((2026, 9, 3, 0, 0, 0))
        ]
        up = sum(bucket["recommendations_up"] for bucket in recent)
        total = up + sum(bucket["recommendations_down"] for bucket in recent)
        self.assertEqual(float(oct_2["positive_share_30d"]),
                         round(up / total * 100, 2))
        self.assertEqual(oct_2["reviews_30d"], total)
        self.assertAlmostEqual(
            float(oct_2["sentiment_drift_points"]),
            float(oct_2["positive_share_30d"])
            - float(oct_2["positive_share_lifetime"]),
            places=2
        )

        # Price status from the recorded Store prices.
        (portal,) = self.mart("mart_price_status", "WHERE appid = 620")
        self.assertEqual(
            (portal["on_sale"], portal["discount_percent"],
             portal["lowest_price"], float(portal["above_lowest_pct"])),
            (1, 80, 199, 0.0)
        )

        # Catalog summaries from Store genres and release dates.
        (action,) = self.mart("mart_genre_summary", "WHERE genre = 'Action'")
        self.assertEqual(action["game_count"], 3)
        self.assertEqual(action["top100_games"], 1)
        self.assertEqual(
            action["top100_current_players"],
            counter_strike_players(DAYS - 1)
        )
        self.assertEqual(
            sorted(row["release_year"] for row in self.mart(
                "mart_release_year_summary"
            )),
            [2011, 2012, 2013, 2020]
        )
        (fps,) = self.mart("mart_tag_summary", "WHERE tag = 'FPS'")
        fps_votes = [
            json.loads(
                (FIXTURES / f"steamspy_appdetails/{appid}.json").read_text()
            )["tags"].get("FPS")
            for appid in (730, 620)
        ]
        self.assertEqual(fps["game_count"], 2)
        self.assertEqual(fps["total_votes"], sum(fps_votes))

        # Engagement: latest daily peak against estimated owners.
        (engagement,) = self.mart("mart_engagement", "WHERE appid = 730")
        self.assertEqual(
            engagement["peak_players_latest_day"],
            counter_strike_players(DAYS - 1)
        )
        self.assertAlmostEqual(
            float(engagement["rough_peak_to_owners_pct"]),
            round(counter_strike_players(DAYS - 1) / 75_000_000 * 100, 4)
        )

        # Event impact: the 30 Sep update, with 6 days before and 7 after.
        (impact,) = self.mart(
            "mart_event_impact", "WHERE gid = '1845383656379136'"
        )
        self.assertEqual((impact["days_before"], impact["days_after"]), (6, 7))
        before = sum(counter_strike_players(i) for i in range(0, 6)) / 6
        after = sum(counter_strike_players(i) for i in range(7, 14)) / 7
        self.assertAlmostEqual(
            float(impact["change_pct"]),
            round((round(after, 2) - round(before, 2)) / round(before, 2)
                  * 100, 2)
        )

        # Every mart test passes on this data.
        connection = get_connection()
        try:
            results = run_mart_tests(connection)
        finally:
            connection.close()

        failures = [row for row in results if row["status"] == "fail"]
        self.assertEqual(failures, [])

    def test_typical_peak_hour_is_in_prague_time_across_dst(self):
        # CEST (UTC+2): an 18:00 UTC peak is 20:00 in Prague.
        self.seed_hourly_day(date(2026, 10, 8), peak_utc_hour=18)
        self.build()
        (row,) = self.mart("mart_player_peak_hours", "WHERE appid = 730")
        self.assertEqual(row["typical_peak_hour_prague"], 20)

        # CET (UTC+1) after 25 Oct: the same UTC peak is 19:00 in Prague.
        self.seed_hourly_day(date(2026, 11, 3), peak_utc_hour=18)
        self.seed_hourly_day(date(2026, 11, 4), peak_utc_hour=18)
        self.build()
        (row,) = self.mart("mart_player_peak_hours", "WHERE appid = 730")
        self.assertEqual(
            (row["typical_peak_hour_prague"], row["days_with_that_peak_hour"],
             row["days_counted"]),
            (19, 2, 3)
        )

    def test_rebuilding_is_idempotent(self):
        self.seed_daily_players()
        self.run_job("reviews_daily")
        self.build()
        counts = {mart: self.count(mart) for mart in MART_TESTS}
        checksums = self.checksums()

        self.assertEqual(self.build(), [])
        self.assertEqual(self.build(), [])

        self.assertEqual({mart: self.count(mart) for mart in MART_TESTS},
                         counts)
        self.assertEqual(self.checksums(), checksums)
        self.assertTrue(all(counts[mart] > 0 for mart in (
            "mart_player_trends", "mart_review_trends", "mart_price_status",
            "mart_genre_summary", "mart_event_impact"
        )))

    def test_a_value_out_of_range_fails_its_mart_test(self):
        self.build()
        self.execute(
            "UPDATE mart_price_status SET discount_percent = 150"
            " WHERE appid = 620"
        )
        connection = get_connection()

        try:
            results = run_mart_tests(connection)
        finally:
            connection.close()

        (failure,) = [row for row in results if row["status"] == "fail"]
        self.assertEqual(failure["check_name"], "mart_value_range")
        self.assertEqual(failure["target"],
                         "mart_price_status.discount_percent")
        self.assertEqual(failure["observed_value"], 1)

        # The next build replaces the bad row.
        self.build()
        (portal,) = self.mart("mart_price_status", "WHERE appid = 620")
        self.assertEqual(portal["discount_percent"], 80)


if __name__ == "__main__":
    unittest.main()
