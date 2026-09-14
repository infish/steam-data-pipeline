import unittest
from unittest.mock import MagicMock, patch

from api.steam_api import (
    STEAMSPY_URL,
    STEAM_CURRENT_PLAYERS_URL,
    STEAM_REVIEWS_URL,
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
        players_response = MagicMock()
        players_response.json.return_value = {
            "response": {"player_count": 1234, "result": 1}
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
            session.get.side_effect = [players_response, reviews_response]

            result = get_steam_measurements()

        self.assertEqual(result, [{
            "appid": 730,
            "current_players": 1234,
            "positive_reviews": 90,
            "negative_reviews": 10,
            "total_reviews": 100,
            "review_score_percent": 90.0
        }])
        session.get.assert_any_call(
            STEAM_CURRENT_PLAYERS_URL,
            params={"appid": 730},
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
        players_response = MagicMock()
        players_response.json.return_value = {
            "response": {"player_count": 1234, "result": 1}
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
            session.get.side_effect = [players_response, reviews_response]

            with self.assertRaisesRegex(ValueError, "inconsistent"):
                get_steam_measurements()


if __name__ == "__main__":
    unittest.main()
