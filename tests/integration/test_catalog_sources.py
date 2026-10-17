"""The ADR 5 catalog: Valve's charts, Store names and companies, a weekly
SteamSpy page and rotating review summaries."""
import os
import unittest
from datetime import datetime
from unittest.mock import patch

from reprocess import reprocess_run

from .base import CANNED_TOP_GAMES, IntegrationTestCase, review_summary


def at(day):
    moment = datetime(2026, 10, day, 18, 0)
    return lambda: moment


NEW_GAME = 999999  # in the canned top-100 chart, not on the SteamSpy page

NEW_GAME_DETAILS = {
    # Released after SteamSpy froze: it answers without a name.
    f"steamspy_appdetails:{NEW_GAME}": {"appid": NEW_GAME, "name": None},
    f"store_appdetails:{NEW_GAME}": {
        str(NEW_GAME): {
            "success": True,
            "data": {
                "type": "game",
                "name": "New Hit",
                "developers": ["Indie Studio"],
                "publishers": ["Indie Studio"],
                "is_free": False,
                "release_date": {"coming_soon": False, "date": "1 Oct, 2026"},
                "platforms": {"windows": True},
            },
        }
    },
}


class CatalogSourcesIntegrationTests(IntegrationTestCase):
    def game(self, appid):
        (row,) = self.query(
            "SELECT name, name_source, catalog_source FROM games"
            " WHERE appid = %s",
            (appid,)
        )
        return row

    def companies(self, table, appid):
        return [
            row["name"]
            for row in self.query(
                f"SELECT companies.name FROM {table}"
                " JOIN companies USING (company_id)"
                " WHERE appid = %s ORDER BY position",
                (appid,)
            )
        ]

    def requests(self, run_id, source):
        (row,) = self.query(
            "SELECT COUNT(*) AS n FROM raw_api_responses"
            " WHERE run_id = %s AND source = %s",
            (run_id, source)
        )
        return row["n"]

    def test_store_name_and_companies_replace_steamspy_and_stay(self):
        # SteamSpy's frozen page: the old name and a different company list.
        frozen_page = {
            **CANNED_TOP_GAMES,
            "730": {
                **CANNED_TOP_GAMES["730"],
                "name": "Counter-Strike: Global Offensive",
                "developer": "Valve, Hidden Path Entertainment",
            },
        }

        # The recorded Store details: "Counter-Strike 2", developer Valve.
        self.run_catalog_and_refresh({"steamspy_all": frozen_page},
                                     now_fn=at(1))

        self.assertEqual(self.game(730)["name"], "Counter-Strike 2")
        self.assertEqual(self.game(730)["name_source"], "store")
        self.assertEqual(self.companies("game_developers", 730), ["Valve"])

        # The next SteamSpy page changes neither.
        self.run_job("catalog_daily", {"steamspy_all": frozen_page},
                     now_fn=at(2))

        self.assertEqual(self.game(730)["name"], "Counter-Strike 2")
        self.assertEqual(self.companies("game_developers", 730), ["Valve"])

    @patch.dict(os.environ, {"CATALOG_FROM_VALVE_CHARTS": "true"})
    def test_chart_games_join_the_catalog_and_get_store_details(self):
        self.run_job("catalog_daily", now_fn=at(1))
        players = self.run_job("players_hourly", now_fn=at(1))

        # Before ADR 5 this game was skipped; now it is measured.
        self.assertEqual(self.game(NEW_GAME), {
            "name": f"App {NEW_GAME}",
            "name_source": "placeholder",
            "catalog_source": "valve_chart",
        })
        (observed,) = self.query(
            "SELECT current_players FROM player_observations"
            " WHERE run_id = %s AND appid = %s",
            (players, NEW_GAME)
        )
        self.assertEqual(observed["current_players"], 50)

        # The next refresh run fetches its Store details first.
        refresh = self.run_job("refresh_hourly", NEW_GAME_DETAILS,
                               now_fn=at(2))

        self.assertEqual(self.game(NEW_GAME)["name"], "New Hit")
        self.assertEqual(self.game(NEW_GAME)["name_source"], "store")
        self.assertEqual(
            self.companies("game_publishers", NEW_GAME), ["Indie Studio"]
        )
        # SteamSpy does not know it: no tags and no warning, asked again
        # only after TAG_REFRESH_DAYS.
        (details,) = self.query(
            "SELECT tags_fetched_at FROM game_details WHERE appid = %s",
            (NEW_GAME,)
        )
        self.assertIsNotNone(details["tags_fetched_at"])
        self.assertNotIn(
            f"app {NEW_GAME}",
            self.run_row(refresh)["warning_message"] or ""
        )

    @patch.dict(os.environ, {"CATALOG_FROM_VALVE_CHARTS": "true"})
    def test_new_chart_games_get_store_details_before_older_games(self):
        # 570 (a SteamSpy game) has never had details either, but games
        # added from the charts come first.
        with patch.dict(os.environ, {"STORE_DETAILS_PER_RUN": "0"}):
            self.run_job("catalog_daily", now_fn=at(1))

        self.run_job("players_hourly", now_fn=at(1))

        with patch.dict(os.environ, {"STORE_DETAILS_PER_RUN": "2"}):
            run_id = self.run_job("refresh_hourly", now_fn=at(2))

        requested = [
            int(row["appid"])
            for row in self.query(
                "SELECT JSON_UNQUOTE(JSON_EXTRACT(request_params,"
                " '$.appids')) AS appid FROM raw_api_responses"
                " WHERE run_id = %s AND source = 'store_appdetails'",
                (run_id,)
            )
        ]
        sources = {
            row["catalog_source"]
            for row in self.query(
                "SELECT catalog_source FROM games WHERE appid IN (%s, %s)",
                tuple(requested)
            )
        }

        self.assertEqual(len(requested), 2)
        self.assertEqual(sources, {"valve_chart"})
        self.assertNotIn(570, requested)

    def test_charts_off_keeps_skipping_unknown_games(self):
        # The baseline settings switch CATALOG_FROM_VALVE_CHARTS off.
        self.run_job("catalog_daily", now_fn=at(1))
        self.run_job("players_hourly", now_fn=at(1))

        self.assertEqual(
            self.query("SELECT appid FROM games WHERE appid = %s",
                       (NEW_GAME,)),
            []
        )

    @patch.dict(os.environ, {"STEAMSPY_PAGE_INTERVAL_DAYS": "7"})
    def test_the_steamspy_page_is_fetched_weekly(self):
        first = self.run_job("catalog_daily", now_fn=at(1))
        second = self.run_job("catalog_daily", now_fn=at(2))
        eighth = self.run_job("catalog_daily", now_fn=at(8))

        self.assertEqual(self.requests(first, "steamspy_all"), 1)
        self.assertEqual(self.requests(second, "steamspy_all"), 0)
        self.assertEqual(self.requests(eighth, "steamspy_all"), 1)
        # Days without the page still check prices.
        self.assertGreater(self.requests(second, "store_prices"), 0)
        # The SteamSpy views keep showing the latest snapshot.
        (latest,) = self.query("SELECT run_id FROM latest_successful_run")
        self.assertEqual(latest["run_id"], eighth)
        # Three page games plus the backfilled tracked app.
        self.assertEqual(self.count("current_game_metrics"), 4)

    @patch.dict(os.environ, {"STEAMSPY_PAGE_INTERVAL_DAYS": "7"})
    def test_a_run_without_the_page_reprocesses_to_the_same_rows(self):
        self.run_job("catalog_daily", now_fn=at(1))
        second = self.run_job("catalog_daily", now_fn=at(2))
        before = self.query(
            "SELECT appid, details_fetched_at FROM game_details"
            " ORDER BY appid"
        )

        reprocess_run(second)

        self.assertEqual(self.requests(second, "steamspy_all"), 0)
        self.assertEqual(
            self.query(
                "SELECT appid, details_fetched_at FROM game_details"
                " ORDER BY appid"
            ),
            before
        )

    @patch.dict(os.environ, {"REVIEW_SUMMARIES_PER_RUN": "1"})
    def test_review_summaries_rotate_through_the_catalog(self):
        self.run_job("catalog_daily", now_fn=at(1))
        summaries = {"steam_review_summary:570": review_summary(800, 200)}

        run_id = self.run_job("refresh_hourly", summaries, now_fn=at(1))

        reviewed = sorted(
            row["appid"]
            for row in self.query(
                "SELECT appid FROM review_observations WHERE run_id = %s",
                (run_id,)
            )
        )
        # One rotated catalog game; tracked games get theirs from
        # reviews_daily.
        self.assertEqual(reviewed, [570])
        # Review history stays with the tracked games.
        self.assertEqual(
            self.query(
                "SELECT appid FROM review_rollups WHERE appid = 570"
            ),
            []
        )
        # The genre mart uses the Steam review share.
        (dota,) = self.query(
            "SELECT avg_review_score FROM mart_genre_summary"
            " WHERE genre = 'Free To Play'"
        )
        self.assertIsNotNone(dota["avg_review_score"])

        # Reprocessing fetches the same rotated game again.
        reprocess_run(run_id)
        self.assertEqual(
            sorted(
                row["appid"]
                for row in self.query(
                    "SELECT appid FROM review_observations"
                    " WHERE run_id = %s",
                    (run_id,)
                )
            ),
            [570]
        )


if __name__ == "__main__":
    unittest.main()
