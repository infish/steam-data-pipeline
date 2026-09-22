import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

from api.steam_api import (
    STEAMSPY_URL,
    STEAM_CURRENT_PLAYERS_URL,
    STEAM_REVIEWS_URL,
    STEAM_TOP_PLAYERS_URL,
    create_session,
    create_bulk_session,
    get_steamspy_page,
    get_steam_measurements,
    get_top_games,
)


class SteamApiTests(unittest.TestCase):
    def test_bulk_session_has_no_hidden_retries(self):
        session = create_bulk_session()
        try:
            self.assertEqual(session.get_adapter("https://").max_retries.total, 0)
        finally:
            session.close()

    def test_bulk_page_allows_empty_for_caller_confirmation(self):
        session = MagicMock()
        response = MagicMock()
        response.json.return_value = {}
        session.get.return_value = response
        self.assertEqual(get_steamspy_page(7, session=session), {})
        session.get.assert_called_once_with(
            STEAMSPY_URL,
            params={"request": "all", "page": 7},
            timeout=(5, 45),
        )

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

            result = get_steam_measurements()

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

    def test_rejects_inconsistent_steam_review_summary(self):
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

            with self.assertRaisesRegex(ValueError, "inconsistent"):
                get_steam_measurements()

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

            result = get_steam_measurements()

        portal = next(row for row in result if row["appid"] == 620)
        self.assertEqual(portal["current_players"], 50)
        self.assertIsNone(portal["player_rank"])
        self.assertIsNone(portal["source_measured_at"])
        session.get.assert_any_call(
            STEAM_CURRENT_PLAYERS_URL,
            params={"appid": 620},
            timeout=(5, 45)
        )


if __name__ == "__main__":
    unittest.main()
