import unittest
from unittest.mock import MagicMock, patch

from api.steam_api import STEAMSPY_URL, create_session, get_top_games


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


if __name__ == "__main__":
    unittest.main()