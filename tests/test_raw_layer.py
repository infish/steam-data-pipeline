import json
import unittest

import requests

from ingest.raw_store import canonical_params, request_hash
from ingest.replay import FixtureSession, route_for


class RequestHashTests(unittest.TestCase):
    def test_parameter_order_does_not_matter(self):
        self.assertEqual(
            request_hash("https://x.test/a", {"b": 1, "a": 2}),
            request_hash("https://x.test/a", {"a": 2, "b": 1})
        )

    def test_endpoint_is_part_of_the_hash(self):
        # appreviews/<appid> requests share their parameters.
        self.assertNotEqual(
            request_hash("https://x.test/appreviews/730", {"json": 1}),
            request_hash("https://x.test/appreviews/620", {"json": 1})
        )

    def test_missing_params_equal_empty_params(self):
        self.assertEqual(canonical_params(None), canonical_params({}))


class FixtureSessionTests(unittest.TestCase):
    def test_routes(self):
        api = "https://api.steampowered.com"
        store = "https://store.steampowered.com"
        spy = "https://steamspy.com/api.php"
        cases = [
            (spy, {"request": "all", "page": 0}, "steamspy_all"),
            (spy, {"request": "appdetails", "appid": 620},
             "steamspy_appdetails:620"),
            (f"{store}/appreviews/730", {"json": 1},
             "steam_review_summary:730"),
            (f"{store}/appreviewhistogram/730", {},
             "steam_review_histogram:730"),
            (f"{store}/api/appdetails", {"appids": 620, "cc": "us"},
             "store_appdetails:620"),
            (f"{store}/api/appdetails",
             {"appids": "620,730", "filters": "price_overview"},
             "store_prices:620,730"),
            (f"{api}/ISteamChartsService/GetGamesByConcurrentPlayers/v1/",
             None, "valve_top_concurrent"),
            (f"{api}/ISteamChartsService/GetMostPlayedGames/v1/",
             None, "valve_most_played"),
            (f"{api}/ISteamUserStats/GetNumberOfCurrentPlayers/v1/",
             {"appid": 620}, "valve_current_players:620"),
            (f"{api}/ISteamNews/GetNewsForApp/v2/", {"appid": 620},
             "steam_news:620"),
        ]

        for url, params, route in cases:
            with self.subTest(route=route):
                self.assertEqual(route_for(url, params), route)

    def test_serves_recorded_fixtures(self):
        session = FixtureSession()

        result = session.get(
            "https://steamspy.com/api.php",
            params={"request": "appdetails", "appid": 620}
        )

        self.assertEqual(result.status_code, 200)
        self.assertEqual(result.json()["name"], "Portal 2")
        self.assertEqual(session.calls, ["steamspy_appdetails:620"])

    def test_unknown_requests_are_404(self):
        result = FixtureSession().get(
            "https://steamspy.com/api.php",
            params={"request": "appdetails", "appid": 1}
        )

        self.assertEqual(result.status_code, 404)

    def test_overrides_payloads_statuses_and_errors(self):
        session = FixtureSession({
            "valve_top_concurrent": {"response": {}},
            "valve_current_players:620": (503, {"error": "busy"}),
            "steam_news:620": requests.ConnectionError("down"),
        })

        self.assertEqual(
            session.get(
                "https://api.steampowered.com/ISteamChartsService/"
                "GetGamesByConcurrentPlayers/v1/"
            ).json(),
            {"response": {}}
        )
        self.assertEqual(
            session.get(
                "https://api.steampowered.com/ISteamUserStats/"
                "GetNumberOfCurrentPlayers/v1/",
                params={"appid": 620}
            ).status_code,
            503
        )

        with self.assertRaises(requests.ConnectionError):
            session.get(
                "https://api.steampowered.com/ISteamNews/GetNewsForApp/v2/",
                params={"appid": 620}
            )

    def test_price_batches_are_built_from_store_details(self):
        batch = FixtureSession().get(
            "https://store.steampowered.com/api/appdetails",
            params={"appids": "730,620,1", "filters": "price_overview"}
        ).json()

        self.assertEqual(batch["730"], {"success": True, "data": []})
        self.assertEqual(
            batch["620"]["data"]["price_overview"]["currency"],
            "USD"
        )
        self.assertEqual(batch["1"], {"success": False})
        self.assertEqual(
            json.loads(json.dumps(batch)),
            batch
        )


if __name__ == "__main__":
    unittest.main()
