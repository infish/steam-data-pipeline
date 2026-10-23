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
    STEAM_MOST_PLAYED_URL,
    STEAM_REVIEWS_URL,
    STEAM_TOP_PLAYERS_URL,
    create_session,
    fetch_json,
    get_most_played,
    get_player_measurements,
    get_review_summaries,
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
        "response": {
            "query_summary": {
                "total_positive": positive,
                "total_negative": negative,
                "total_reviews": (
                    positive + negative if total is None else total
                )
            }
        }
    })


def redirected(to="https://store.example.test/"):
    """A 200 page reached by following a 302, as requests returns it."""
    page = make_response(to, 200, b"<!DOCTYPE html><html>Welcome</html>")
    page.history = [make_response("https://x.test", 302, b"")]
    return page


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

    def test_a_redirect_is_recorded_as_a_failed_request(self):
        # The Store's appreviews redirected to the home page from
        # 2026-10-22; storing that page as a 200 hid the outage.
        store = MemoryRawStore()

        with self.assertRaisesRegex(requests.HTTPError, "redirected"):
            fetch_json(session_with(redirected()), store, "src",
                       "https://x.test")

        self.assertIsNone(store.rows[0]["http_status"])
        self.assertIsNone(store.rows[0]["body"])
        self.assertEqual(
            store.rows[0]["error"],
            "redirected (302) to https://store.example.test/"
        )


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
        paced = []
        session = session_with(
            spy_details(427520),
            spy_details(620, name="Portal 2")
        )

        rows, warnings = get_tracked_app_details(
            session,
            [427520, 620],
            pace=lambda: paced.append("steamspy")
        )

        self.assertEqual(warnings, [])
        self.assertEqual(list(rows), ["427520", "620"])
        self.assertEqual(tuple(rows["427520"]), CATALOG_ROW_FIELDS)
        self.assertEqual(rows["427520"]["owners"], "5,000,000 .. 10,000,000")
        self.assertEqual(rows["620"]["name"], "Portal 2")
        self.assertNotIn("tags", rows["427520"])
        # Paced before each SteamSpy request.
        self.assertEqual(paced, ["steamspy"] * 2)
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
            pace=lambda: None
        )

        self.assertEqual(list(rows), ["427520"])
        self.assertEqual(len(warnings), 2)
        self.assertIn("failed for tracked app 111", warnings[0])
        self.assertIn("503", warnings[0])
        self.assertIn("no name for app 999", warnings[1])

    def test_no_tracked_app_details_requests_when_nothing_is_missing(self):
        paced = []
        session = MagicMock()

        rows, warnings = get_tracked_app_details(
            session,
            [],
            pace=lambda: paced.append("steamspy")
        )

        self.assertEqual((rows, warnings), ({}, []))
        self.assertEqual(paced, [])
        session.get.assert_not_called()


class PlayerMeasurementTests(unittest.TestCase):
    def measure(self, tracked, *responses):
        session = session_with(*responses)

        with patch("api.steam_api.get_tracked_appids", return_value=tracked):
            result, warnings = get_player_measurements(session)

        return session, result, warnings

    def test_tracked_game_in_the_chart_needs_no_extra_request(self):
        session, result, warnings = self.measure([730], chart())

        self.assertEqual(warnings, [])
        self.assertEqual(result, [{
            "appid": 730,
            "current_players": 1234,
            "player_rank": 1,
            "source_measured_at": datetime(2026, 9, 16, 12, 0),
            "source": "top100"
        }])
        session.get.assert_called_once_with(
            STEAM_TOP_PLAYERS_URL,
            params=None,
            timeout=(5, 45)
        )

    def test_uses_direct_player_lookup_when_tracked_game_is_outside_top_100(self):
        session, result, warnings = self.measure([620], chart(), players(50))

        self.assertEqual(warnings, [])
        portal = next(row for row in result if row["appid"] == 620)
        self.assertEqual(portal["current_players"], 50)
        self.assertIsNone(portal["player_rank"])
        self.assertIsNone(portal["source_measured_at"])
        self.assertEqual(portal["source"], "direct")
        session.get.assert_any_call(
            STEAM_CURRENT_PLAYERS_URL,
            params={"appid": 620},
            timeout=(5, 45)
        )

    def test_chart_failure_falls_back_to_direct_player_lookups(self):
        _, result, warnings = self.measure(
            [730, 620],
            requests.ConnectionError("chart down"),
            players(1_000_000),
            players(50)
        )

        self.assertEqual(
            [(row["appid"], row["current_players"]) for row in result],
            [(730, 1_000_000), (620, 50)]
        )
        self.assertTrue(all(row["source"] == "direct" for row in result))
        self.assertEqual(len(warnings), 1)
        self.assertIn("top-100 chart unavailable", warnings[0])
        self.assertIn("chart down", warnings[0])

    def test_malformed_chart_row_falls_back_to_direct_lookups(self):
        _, result, warnings = self.measure(
            [730],
            chart(players=None),
            players(1_000_000)
        )

        self.assertEqual(result[0]["current_players"], 1_000_000)
        self.assertIsNone(result[0]["player_rank"])
        self.assertEqual(len(warnings), 1)
        self.assertIn("top-100 chart unavailable", warnings[0])

    def test_player_failure_skips_the_row(self):
        missing_count = response({"response": {"result": 1}})

        _, result, warnings = self.measure(
            [620, 427520],
            chart(),
            missing_count,
            players(800)
        )

        self.assertEqual(
            sorted(row["appid"] for row in result),
            [730, 427520]
        )
        self.assertEqual(len(warnings), 1)
        self.assertIn("player lookup failed for app 620", warnings[0])


class ReviewSummaryTests(unittest.TestCase):
    def test_returns_totals_and_steam_rating_label(self):
        summary = reviews(90, 10)
        body = json.loads(summary.content)
        body["response"]["query_summary"].update(
            review_score=8,
            review_score_desc="Very Positive"
        )
        session = session_with(response(body))

        result, warnings = get_review_summaries(session, [730])

        self.assertEqual(warnings, [])
        self.assertEqual(result, [{
            "appid": 730,
            "positive_reviews": 90,
            "negative_reviews": 10,
            "total_reviews": 100,
            "review_score_percent": 90.0,
            "review_score": 8,
            "review_score_desc": "Very Positive"
        }])
        session.get.assert_called_once_with(
            STEAM_REVIEWS_URL,
            params={
                "appid": 730,
                "num_per_page": 1,
                "languages[0]": "all",
                "purchase_type": 1,
                "filter_offtopic_activity": 0,
            },
            timeout=(5, 45)
        )

    def test_a_stored_appreviews_answer_still_parses(self):
        # Raw payloads from before v1.2.2 have the Store's shape.
        old = response({"success": 1, "query_summary": {
            "total_positive": 9, "total_negative": 1, "total_reviews": 10,
        }})

        result, warnings = get_review_summaries(session_with(old), [730])

        self.assertEqual(warnings, [])
        self.assertEqual(result[0]["total_reviews"], 10)

    def test_a_missing_summary_is_skipped_with_a_warning(self):
        result, warnings = get_review_summaries(
            session_with(response({"response": {}})),
            [730]
        )

        self.assertEqual(result, [])
        self.assertIn("summary is missing for app 730", warnings[0])

    def test_a_redirected_lookup_is_skipped_with_a_warning(self):
        result, warnings = get_review_summaries(
            session_with(redirected()), [730]
        )

        self.assertEqual(result, [])
        self.assertIn("redirected (302)", warnings[0])

    def test_inconsistent_summary_is_skipped_with_a_warning(self):
        result, warnings = get_review_summaries(
            session_with(reviews(90, 10, total=101), reviews(9, 1)),
            [730, 620]
        )

        self.assertEqual([row["appid"] for row in result], [620])
        self.assertEqual(len(warnings), 1)
        self.assertIn("inconsistent for app 730", warnings[0])

    def test_http_failure_is_skipped_with_a_warning(self):
        result, warnings = get_review_summaries(
            session_with(failing(502)),
            [730]
        )

        self.assertEqual(result, [])
        self.assertIn("review lookup failed for app 730", warnings[0])
        self.assertIn("502", warnings[0])


class MostPlayedTests(unittest.TestCase):
    def test_parses_the_daily_peak_leaderboard(self):
        session = session_with(response({
            "response": {
                "rollup_date": 1_790_726_400,
                "ranks": [
                    {"rank": 1, "appid": 730, "last_week_rank": 1,
                     "peak_in_game": 1_316_372},
                    {"rank": 2, "appid": 570, "last_week_rank": 0,
                     "peak_in_game": 809_533},
                    {"rank": 3, "appid": 3393110, "last_week_rank": -1,
                     "peak_in_game": 171_530},
                ]
            }
        }))

        day, rows = get_most_played(session)

        self.assertEqual(day.isoformat(), "2026-09-30")
        self.assertEqual(rows[0], {
            "appid": 730,
            "player_rank": 1,
            "last_week_rank": 1,
            "peak_players": 1_316_372
        })
        self.assertIsNone(rows[1]["last_week_rank"])
        self.assertIsNone(rows[2]["last_week_rank"])
        session.get.assert_called_once_with(
            STEAM_MOST_PLAYED_URL,
            params=None,
            timeout=(5, 45)
        )

    def test_incomplete_leaderboard_raises(self):
        with self.assertRaisesRegex(ValueError, "incomplete"):
            get_most_played(session_with(response({"response": {}})))


if __name__ == "__main__":
    unittest.main()
