import json
import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

import requests

from api.steam_api import (
    CATALOG_ROW_FIELDS,
    STEAMSPY_REQUEST_INTERVAL_SECONDS,
    STEAMSPY_URL,
    STEAM_CURRENT_PLAYERS_URL,
    STEAM_REVIEWS_URL,
    STEAM_TOP_PLAYERS_URL,
    create_session,
    fetch_json,
    get_steam_measurements,
    get_top_games,
    get_tracked_app_details,
)
from ingest.raw_store import MemoryRawStore
from ingest.replay import make_response


def response(payload, status=200):
    return make_response("https://example.test", status, json.dumps(payload).encode())


def failing(status=502):
    return make_response("https://example.test", status, b"Bad gateway")


def chart(appid=730, players=1234):
    return response({
        "response": {
            "last_update": 1_789_560_000,
            "ranks": [{
                "rank": 1,
                "appid": appid,
                "concurrent_in_game": players
            }]
        }
    })


def players(count):
    return response({"response": {"player_count": count, "result": 1}})


def reviews(positive, negative, total=None):
    return response({
        "success": 1,
        "query_summary": {
            "total_positive": positive,
            "total_negative": negative,
            "total_reviews": positive + negative if total is None else total
        }
    })


def spy_details(appid, name="Factorio"):
    return response({
        "appid": appid,
        "name": name,
        "developer": "Wube Software LTD.",
        "publisher": "Wube Software LTD.",
        "owners": "5,000,000 .. 10,000,000",
        "positive": 200000,
        "negative": 5000,
        "average_forever": 9000,
        "average_2weeks": 600,
        "median_forever": 3000,
        "median_2weeks": 300,
        "ccu": 20000,
        "price": "3500",
        "initialprice": "3500",
        "discount": "0",
        "languages": "English, Czech",
        "genre": "Simulation, Strategy",
        "tags": {"Automation": 9000},
        "score_rank": ""
    })


def session_with(*responses):
    session = MagicMock()
    session.get.side_effect = list(responses)
    return session


class FetchJsonTests(unittest.TestCase):
    def test_records_the_response_before_parsing_and_parses_the_stored_copy(self):
        store = MemoryRawStore()
        store.read_payload = MagicMock(wraps=store.read_payload)
        session = session_with(response({"ok": 1}))

        data = fetch_json(session, store, "src", "https://x.test/a", {"q": 1})

        self.assertEqual(data, {"ok": 1})
        self.assertEqual(len(store.rows), 1)
        self.assertEqual(store.rows[0]["source"], "src")
        self.assertEqual(store.rows[0]["params"], {"q": 1})
        self.assertEqual(store.rows[0]["http_status"], 200)
        store.read_payload.assert_called_once_with(1)

    def test_http_errors_are_recorded_then_raised(self):
        store = MemoryRawStore()

        with self.assertRaises(requests.HTTPError):
            fetch_json(session_with(failing(503)), store, "src", "https://x.test")

        self.assertEqual(store.rows[0]["http_status"], 503)
        self.assertEqual(store.rows[0]["body"], b"Bad gateway")

    def test_connection_errors_are_recorded_then_raised(self):
        store = MemoryRawStore()

        with self.assertRaises(requests.ConnectionError):
            fetch_json(
                session_with(requests.ConnectionError("refused")),
                store,
                "src",
                "https://x.test"
            )

        self.assertIsNone(store.rows[0]["http_status"])
        self.assertEqual(store.rows[0]["error"], "refused")


class SteamSpyTests(unittest.TestCase):
    def test_all_mode_sends_page_and_returns_data(self):
        session = session_with(response({"1": {"name": "Test Game"}}))

        with patch(
            "api.steam_api.get_steamspy_config",
            return_value={"mode": "all", "page": 0}
        ):
            result = get_top_games(session)

        session.get.assert_called_once_with(
            STEAMSPY_URL,
            params={"request": "all", "page": 0},
            timeout=(5, 45)
        )
        self.assertEqual(result, {"1": {"name": "Test Game"}})

    def test_empty_response_raises_error(self):
        with (
            patch(
                "api.steam_api.get_steamspy_config",
                return_value={"mode": "all", "page": 0}
            ),
            self.assertRaisesRegex(ValueError, "empty or unexpected")
        ):
            get_top_games(session_with(response({})))

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

    def test_tracked_app_details_are_shaped_like_page_rows(self):
        sleeps = []
        session = session_with(
            spy_details(427520),
            spy_details(620, name="Portal 2")
        )

        rows, warnings = get_tracked_app_details(
            session,
            [427520, 620],
            sleep_fn=sleeps.append
        )

        self.assertEqual(warnings, [])
        self.assertEqual(list(rows), ["427520", "620"])
        self.assertEqual(tuple(rows["427520"]), CATALOG_ROW_FIELDS)
        self.assertEqual(rows["427520"]["owners"], "5,000,000 .. 10,000,000")
        self.assertEqual(rows["620"]["name"], "Portal 2")
        self.assertNotIn("tags", rows["427520"])
        self.assertEqual(sleeps, [STEAMSPY_REQUEST_INTERVAL_SECONDS] * 2)
        self.assertGreaterEqual(STEAMSPY_REQUEST_INTERVAL_SECONDS, 1)
        session.get.assert_any_call(
            STEAMSPY_URL,
            params={"request": "appdetails", "appid": 427520},
            timeout=(5, 45)
        )

    def test_tracked_app_details_failures_become_warnings(self):
        session = session_with(
            failing(503),
            spy_details(427520),
            spy_details(999, name=None)
        )

        rows, warnings = get_tracked_app_details(
            session,
            [111, 427520, 999],
            sleep_fn=lambda seconds: None
        )

        self.assertEqual(list(rows), ["427520"])
        self.assertEqual(len(warnings), 2)
        self.assertIn("failed for tracked app 111", warnings[0])
        self.assertIn("503", warnings[0])
        self.assertIn("no name for app 999", warnings[1])

    def test_no_tracked_app_details_requests_when_nothing_is_missing(self):
        sleeps = []
        session = MagicMock()

        rows, warnings = get_tracked_app_details(
            session,
            [],
            sleep_fn=sleeps.append
        )

        self.assertEqual((rows, warnings), ({}, []))
        self.assertEqual(sleeps, [])
        session.get.assert_not_called()


class SteamMeasurementTests(unittest.TestCase):
    def measure(self, tracked, *responses):
        session = session_with(*responses)

        with patch("api.steam_api.get_tracked_appids", return_value=tracked):
            result, warnings = get_steam_measurements(session)

        return session, result, warnings

    def test_gets_authoritative_measurements_for_tracked_games(self):
        session, result, warnings = self.measure(
            [730],
            chart(),
            reviews(90, 10)
        )

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
            params=None,
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
        _, result, warnings = self.measure(
            [730],
            chart(),
            reviews(90, 10, total=101)
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["current_players"], 1234)
        self.assertIsNone(result[0]["total_reviews"])
        self.assertIsNone(result[0]["review_score_percent"])
        self.assertEqual(len(warnings), 1)
        self.assertIn("inconsistent for app 730", warnings[0])

    def test_uses_direct_player_lookup_when_tracked_game_is_outside_top_100(self):
        session, result, warnings = self.measure(
            [620],
            chart(),
            players(50),
            reviews(9, 1)
        )

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

    def test_chart_failure_falls_back_to_direct_player_lookups(self):
        session, result, warnings = self.measure(
            [730, 620],
            requests.ConnectionError("chart down"),
            players(1_000_000),
            reviews(90, 10),
            players(50),
            reviews(9, 1)
        )

        self.assertEqual(
            [(row["appid"], row["current_players"]) for row in result],
            [(730, 1_000_000), (620, 50)]
        )
        self.assertTrue(all(row["player_rank"] is None for row in result))
        self.assertEqual(result[0]["total_reviews"], 100)
        self.assertEqual(len(warnings), 1)
        self.assertIn("top-100 chart unavailable", warnings[0])
        self.assertIn("chart down", warnings[0])

    def test_malformed_chart_row_falls_back_to_direct_lookups(self):
        _, result, warnings = self.measure(
            [730],
            chart(players=None),
            players(1_000_000),
            reviews(90, 10)
        )

        self.assertEqual(result[0]["current_players"], 1_000_000)
        self.assertIsNone(result[0]["player_rank"])
        self.assertEqual(len(warnings), 1)
        self.assertIn("top-100 chart unavailable", warnings[0])

    def test_review_failure_keeps_row_with_null_reviews(self):
        _, result, warnings = self.measure([730], chart(), failing(502))

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
        self.assertIn("502", warnings[0])

    def test_player_failure_skips_the_row(self):
        missing_count = response({"response": {"result": 1}})

        session, result, warnings = self.measure(
            [620, 427520],
            chart(),
            missing_count,
            players(800),
            reviews(8, 2)
        )

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
