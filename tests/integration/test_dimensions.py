import os
import unittest
from datetime import date, datetime, timedelta
from unittest.mock import patch

from .base import CANNED_RESPONSES, CANNED_TOP_GAMES, IntegrationTestCase


def at(day):
    moment = datetime(2026, 10, day, 18, 0)
    return lambda: moment


class DimensionsIntegrationTests(IntegrationTestCase):
    def names(self, table, appid):
        return [
            row["name"]
            for row in self.query(
                f"SELECT companies.name FROM {table}"
                " JOIN companies USING (company_id)"
                " WHERE appid = %s ORDER BY position",
                (appid,)
            )
        ]

    def test_company_links_are_parsed_from_steamspy_strings(self):
        top_games = {
            **CANNED_TOP_GAMES,
            "730": {
                **CANNED_TOP_GAMES["730"],
                "developer": "Valve, Inc., Hidden Path Entertainment",
                "publisher": "Valve",
            },
        }

        self.run_job("catalog_daily", {"steamspy_all": top_games})

        self.assertEqual(
            self.names("game_developers", 730),
            ["Valve, Inc.", "Hidden Path Entertainment"]
        )
        self.assertEqual(self.names("game_publishers", 730), ["Valve"])
        # One company row per distinct name across all games.
        self.assertEqual(
            sorted(row["name"] for row in self.query(
                "SELECT name FROM companies"
            )),
            ["Hidden Path Entertainment", "Valve", "Valve, Inc."]
        )
        # The raw string is kept on the games row.
        (game,) = self.query("SELECT developer FROM games WHERE appid = 730")
        self.assertEqual(
            game["developer"],
            "Valve, Inc., Hidden Path Entertainment"
        )

    def test_store_details_and_tags_fill_the_bridge_tables(self):
        self.run_job("catalog_daily")

        details = {
            row["appid"]: row
            for row in self.query("SELECT * FROM game_details")
        }
        self.assertEqual(sorted(details), [570, 620, 730, 427520])

        portal = details[620]
        self.assertTrue(portal["store_available"])
        self.assertEqual(portal["release_date"], date(2011, 4, 18))
        self.assertEqual(portal["metacritic_score"], 95)
        self.assertEqual(
            (portal["platform_windows"], portal["platform_mac"],
             portal["platform_linux"]),
            (1, 0, 1)
        )
        self.assertEqual(portal["is_free"], 0)

        counter_strike = details[730]
        self.assertEqual(counter_strike["is_free"], 1)
        self.assertIsNone(counter_strike["metacritic_score"])

        self.assertEqual(
            [row["description"] for row in self.query(
                "SELECT description FROM game_genres JOIN genres"
                " USING (genre_id) WHERE appid = 620 ORDER BY genre_id"
            )],
            ["Action", "Adventure"]
        )
        self.assertEqual(
            self.query(
                "SELECT COUNT(*) AS n FROM game_categories WHERE appid = 620"
            )[0]["n"],
            32
        )

        languages = {
            row["name"]: row["full_audio"]
            for row in self.query(
                "SELECT name, full_audio FROM game_languages JOIN languages"
                " USING (language_id) WHERE appid = 620"
            )
        }
        self.assertEqual(languages["English"], 1)
        self.assertEqual(languages["Czech"], 0)

        tags = {
            row["name"]: row["votes"]
            for row in self.query(
                "SELECT name, votes FROM game_tags JOIN tags USING (tag_id)"
                " WHERE appid = 730"
            )
        }
        self.assertEqual(len(tags), 20)
        self.assertEqual(tags["FPS"], 91172)
        self.assertIsNotNone(counter_strike["tags_fetched_at"])

        # Lookups hold each value once across games.
        (action,) = self.query(
            "SELECT COUNT(*) AS n FROM genres WHERE description = 'Action'"
        )
        self.assertEqual(action["n"], 1)

    def refreshed(self, run_id):
        return sorted(
            row["appid"]
            for row in self.query(
                "SELECT appid FROM game_details WHERE run_id = %s",
                (run_id,)
            )
        )

    def test_each_run_refreshes_the_stalest_games(self):
        with patch.dict(os.environ, {"STORE_DETAILS_PER_RUN": "2"}):
            first = self.run_job("catalog_daily", now_fn=at(1))
            first_set = self.refreshed(first)
            second = self.run_job("catalog_daily", now_fn=at(2))
            second_set = self.refreshed(second)
            third = self.run_job("catalog_daily", now_fn=at(3))

        # Games without details first (by appid), then the oldest refresh.
        self.assertEqual(first_set, [570, 620])
        self.assertEqual(second_set, [730, 427520])
        self.assertEqual(self.refreshed(third), [570, 620])

    def test_a_failed_refresh_keeps_previous_details(self):
        self.run_job("catalog_daily", now_fn=at(1))
        (before,) = self.query("SELECT * FROM game_details WHERE appid = 620")

        with self.assertLogs(level="WARNING") as logs:
            run_id = self.run_job(
                "catalog_daily",
                {"store_appdetails:620": (503, {"error": "busy"})},
                now_fn=at(2)
            )

        (after,) = self.query("SELECT * FROM game_details WHERE appid = 620")
        # The Store details are unchanged; the SteamSpy tags were refreshed.
        self.assertGreater(after.pop("tags_fetched_at"),
                           before.pop("tags_fetched_at"))
        self.assertEqual(after, before)
        self.assertIn(
            "Store details failed for 1 game(s), kept previous details: 620",
            "\n".join(logs.output)
        )
        self.assertIn(
            "Store details failed for 1 game(s)",
            self.run_row(run_id)["warning_message"]
        )

    def test_delisted_game_is_marked_unavailable_and_keeps_details(self):
        self.run_job("catalog_daily", now_fn=at(1))

        self.run_job(
            "catalog_daily",
            {"store_appdetails:570": {"570": {"success": False}}},
            now_fn=at(2)
        )

        (dota,) = self.query("SELECT * FROM game_details WHERE appid = 570")
        self.assertEqual(dota["store_available"], 0)
        self.assertEqual(dota["release_date"], date(2013, 7, 9))
        self.assertEqual(dota["details_fetched_at"], at(2)())
        self.assertEqual(
            self.query(
                "SELECT COUNT(*) AS n FROM game_genres WHERE appid = 570"
            )[0]["n"],
            2
        )

    def test_unparseable_release_date_is_null_with_a_warning(self):
        odd_date = {"570": {
            "success": True,
            "data": {
                **CANNED_RESPONSES["store_appdetails:570"]["570"]["data"],
                "release_date": {"coming_soon": False, "date": "Summer 2013"},
            },
        }}

        with self.assertLogs(level="WARNING"):
            run_id = self.run_job(
                "catalog_daily",
                {"store_appdetails:570": odd_date}
            )

        (dota,) = self.query("SELECT * FROM game_details WHERE appid = 570")
        self.assertIsNone(dota["release_date"])
        self.assertEqual(dota["release_date_text"], "Summer 2013")
        self.assertIn(
            "Unparseable release dates stored as NULL for 1 game(s):"
            " 570: 'Summer 2013'",
            self.run_row(run_id)["warning_message"]
        )

    def test_coming_soon_dates_are_null_without_a_warning(self):
        coming_soon = {"570": {
            "success": True,
            "data": {
                **CANNED_RESPONSES["store_appdetails:570"]["570"]["data"],
                "release_date": {"coming_soon": True, "date": "Q1 2027"},
            },
        }}

        run_id = self.run_job(
            "catalog_daily",
            {"store_appdetails:570": coming_soon}
        )

        (dota,) = self.query("SELECT * FROM game_details WHERE appid = 570")
        self.assertIsNone(dota["release_date"])
        self.assertEqual(dota["coming_soon"], 1)
        self.assertIsNone(self.run_row(run_id)["warning_message"])


if __name__ == "__main__":
    unittest.main()
