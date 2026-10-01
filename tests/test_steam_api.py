import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

import requests

from api.steam_api import (
    STEAMSPY_URL,
    STEAM_CURRENT_PLAYERS_URL,
    STEAM_REVIEWS_URL,
    STEAM_TOP_PLAYERS_URL,
    create_session,
    get_steam_measurements,
    get_top_games,
)


class SteamApiTests(unittest.TestCase):
    def test_all_mode_sends_page_and_returns_data(self):
        config = {
            "mode": "all",
            "page": 0
        }

        response = MagicMock()
        response.json.return_value = {
            "1": {"name": "Test Game"}
        }

        with (
            patch(
                "api.steam_api.get_steamspy_config",
                return_value=config
            ),
            patch("api.steam_api.create_session") as mocked_session
        ):
            session = mocked_session.return_value.__enter__.return_value
            session.get.return_value = response

            result = get_top_games()

        session.get.assert_called_once_with(
            STEAMSPY_URL,
            params={"request": "all", "page": 0},
            timeout=(5, 45)
        )
        response.raise_for_status.assert_called_once_with()
        self.assertEqual(result, {"1": {"name": "Test Game"}})

    def test_empty_response_raises_error(self):
        response = MagicMock()
        response.json.return_value = {}

        with (
            patch(
                "api.steam_api.get_steamspy_config",
                return_value={"mode": "all", "page": 0}
            ),
            patch("api.steam_api.create_session") as mocked_session
        ):
            session = mocked_session.return_value.__enter__.return_value
            session.get.return_value = response

            with self.assertRaisesRegex(
                ValueError,
                "empty or unexpected"
            ):
                get_top_games()

    def test_retry_policy_is_configured(self):
        session = create_session()

        try:
            retries = session.get_adapter("https://").max_retries

            self.assertEqual(retries.total, 4)
            self.assertEqual(retries.backoff_factor, 2)
            self.assertIn(429, retries.status_forcelist)
            self.assertIn(500, retries.status_forcelist)
            self.assertIn("GET", retries.allowed_methods)
        finally:
            session.close()

    def test_gets_authoritative_measurements_for_tracked_games(self):
        charts_response = MagicMock()
        charts_response.json.return_value = {
            "response": {
                "last_update": 1_789_560_000,
                "ranks": [{
                    "rank": 1,
                    "appid": 730,
                    "concurrent_in_game": 1234
                }]
            }
        }
        reviews_response = MagicMock()
        reviews_response.json.return_value = {
            "success": 1,
            "query_summary": {
                "total_positive": 90,
                "total_negative": 10,
                "total_reviews": 100
            }
        }

        with (
            patch("api.steam_api.get_tracked_appids", return_value=[730]),
            patch("api.steam_api.create_session") as mocked_session
        ):
            session = mocked_session.return_value.__enter__.return_value
            session.get.side_effect = [charts_response, reviews_response]

            result, warnings = get_steam_measurements()

        self.assertEqual(warnings, [])
        self.assertEqual(result, [{
            "appid": 730,
            "current_players": 1234,
            "player_rank": 1,
            "source_measured_at": datetime(2026, 9, 16, 12, 0),
            "positive_reviews": 90,
            "negative_reviews": 10,
            "total_reviews": 100,
            "review_score_percent": 90.0
        }])
        session.get.assert_any_call(
            STEAM_TOP_PLAYERS_URL,
            timeout=(5, 45)
        )
        session.get.assert_any_call(
            STEAM_REVIEWS_URL.format(appid=730),
            params={
                "json": 1,
                "filter": "recent",
                "language": "all",
                "review_type": "all",
                "purchase_type": "all",
                "num_per_page": 1,
                "filter_offtopic_activity": 0
            },
            timeout=(5, 45)
        )

    def test_inconsistent_review_summary_keeps_row_with_null_reviews(self):
        charts_response = MagicMock()
        charts_response.json.return_value = {
            "response": {
                "last_update": 1_789_560_000,
                "ranks": [{
                    "rank": 1,
                    "appid": 730,
                    "concurrent_in_game": 1234
                }]
            }
        }
        reviews_response = MagicMock()
        reviews_response.json.return_value = {
            "success": 1,
            "query_summary": {
                "total_positive": 90,
                "total_negative": 10,
                "total_reviews": 101
            }
        }

        with (
            patch("api.steam_api.get_tracked_appids", return_value=[730]),
            patch("api.steam_api.create_session") as mocked_session
        ):
            session = mocked_session.return_value.__enter__.return_value
            session.get.side_effect = [charts_response, reviews_response]

            result, warnings = get_steam_measurements()

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["current_players"], 1234)
        self.assertIsNone(result[0]["total_reviews"])
        self.assertIsNone(result[0]["review_score_percent"])
        self.assertEqual(len(warnings), 1)
        self.assertIn("inconsistent for app 730", warnings[0])

    def test_uses_direct_player_lookup_when_tracked_game_is_outside_top_100(self):
        charts_response = MagicMock()
        charts_response.json.return_value = {
            "response": {
                "last_update": 1_789_560_000,
                "ranks": [{
                    "rank": 1,
                    "appid": 730,
                    "concurrent_in_game": 1234
                }]
            }
        }
        players_response = MagicMock()
        players_response.json.return_value = {
            "response": {"player_count": 50, "result": 1}
        }
        reviews_response = MagicMock()
        reviews_response.json.return_value = {
            "success": 1,
            "query_summary": {
                "total_positive": 9,
                "total_negative": 1,
                "total_reviews": 10
            }
        }

        with (
            patch("api.steam_api.get_tracked_appids", return_value=[620]),
            patch("api.steam_api.create_session") as mocked_session
        ):
            session = mocked_session.return_value.__enter__.return_value
            session.get.side_effect = [
                charts_response,
                players_response,
                reviews_response
            ]

            result, warnings = get_steam_measurements()

        self.assertEqual(warnings, [])
        portal = next(row for row in result if row["appid"] == 620)
        self.assertEqual(portal["current_players"], 50)
        self.assertIsNone(portal["player_rank"])
        self.assertIsNone(portal["source_measured_at"])
        session.get.assert_any_call(
            STEAM_CURRENT_PLAYERS_URL,
            params={"appid": 620},
            timeout=(5, 45)
        )

    def chart_response(self, appid=730, players=1234):
        response = MagicMock()
        response.json.return_value = {
            "response": {
                "last_update": 1_789_560_000,
                "ranks": [{
                    "rank": 1,
                    "appid": appid,
                    "concurrent_in_game": players
                }]
            }
        }
        return response

    def players_response(self, player_count):
        response = MagicMock()
        response.json.return_value = {
            "response": {"player_count": player_count, "result": 1}
        }
        return response

    def reviews_response(self, positive, negative):
        response = MagicMock()
        response.json.return_value = {
            "success": 1,
            "query_summary": {
                "total_positive": positive,
                "total_negative": negative,
                "total_reviews": positive + negative
            }
        }
        return response

    def test_chart_failure_falls_back_to_direct_player_lookups(self):
        with (
            patch(
                "api.steam_api.get_tracked_appids",
                return_value=[730, 620]
            ),
            patch("api.steam_api.create_session") as mocked_session
        ):
            session = mocked_session.return_value.__enter__.return_value
            session.get.side_effect = [
                requests.ConnectionError("chart down"),
                self.players_response(1_000_000),
                self.reviews_response(90, 10),
                self.players_response(50),
                self.reviews_response(9, 1)
            ]

            result, warnings = get_steam_measurements()

        self.assertEqual(
            [(row["appid"], row["current_players"]) for row in result],
            [(730, 1_000_000), (620, 50)]
        )
        self.assertTrue(all(row["player_rank"] is None for row in result))
        self.assertTrue(
            all(row["source_measured_at"] is None for row in result)
        )
        self.assertEqual(result[0]["total_reviews"], 100)
        self.assertEqual(len(warnings), 1)
        self.assertIn("top-100 chart unavailable", warnings[0])
        self.assertIn("chart down", warnings[0])
        session.get.assert_any_call(
            STEAM_CURRENT_PLAYERS_URL,
            params={"appid": 730},
            timeout=(5, 45)
        )

    def test_malformed_chart_row_falls_back_to_direct_lookups(self):
        malformed_chart = self.chart_response(players=None)

        with (
            patch("api.steam_api.get_tracked_appids", return_value=[730]),
            patch("api.steam_api.create_session") as mocked_session
        ):
            session = mocked_session.return_value.__enter__.return_value
            session.get.side_effect = [
                malformed_chart,
                self.players_response(1_000_000),
                self.reviews_response(90, 10)
            ]

            result, warnings = get_steam_measurements()

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["current_players"], 1_000_000)
        self.assertIsNone(result[0]["player_rank"])
        self.assertEqual(len(warnings), 1)
        self.assertIn("top-100 chart unavailable", warnings[0])

    def test_review_failure_keeps_row_with_null_reviews(self):
        failed_reviews = MagicMock()
        failed_reviews.raise_for_status.side_effect = requests.HTTPError(
            "502 Server Error"
        )

        with (
            patch("api.steam_api.get_tracked_appids", return_value=[730]),
            patch("api.steam_api.create_session") as mocked_session
        ):
            session = mocked_session.return_value.__enter__.return_value
            session.get.side_effect = [self.chart_response(), failed_reviews]

            result, warnings = get_steam_measurements()

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["current_players"], 1234)
        self.assertEqual(result[0]["player_rank"], 1)
        for field in (
            "positive_reviews",
            "negative_reviews",
            "total_reviews",
            "review_score_percent"
        ):
            self.assertIsNone(result[0][field])
        self.assertEqual(len(warnings), 1)
        self.assertIn("review lookup failed for app 730", warnings[0])
        self.assertIn("502 Server Error", warnings[0])

    def test_player_failure_skips_the_row(self):
        missing_count = MagicMock()
        missing_count.json.return_value = {"response": {"result": 1}}

        with (
            patch(
                "api.steam_api.get_tracked_appids",
                return_value=[620, 427520]
            ),
            patch("api.steam_api.create_session") as mocked_session
        ):
            session = mocked_session.return_value.__enter__.return_value
            session.get.side_effect = [
                self.chart_response(),
                missing_count,
                self.players_response(800),
                self.reviews_response(8, 2)
            ]

            result, warnings = get_steam_measurements()

        self.assertEqual(
            sorted(row["appid"] for row in result),
            [730, 427520]
        )
        self.assertEqual(len(warnings), 1)
        self.assertIn("player lookup failed for app 620", warnings[0])
        review_urls = [
            call.args[0]
            for call in session.get.call_args_list
            if call.args[0].startswith("https://store.steampowered.com")
        ]
        self.assertEqual(
            review_urls,
            [STEAM_REVIEWS_URL.format(appid=427520)]
        )


if __name__ == "__main__":
    unittest.main()
