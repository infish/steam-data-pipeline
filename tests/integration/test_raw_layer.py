import hashlib
import json
import os
import unittest
import zlib
from collections import Counter
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import patch

from urllib3.util.retry import RequestHistory

from database.mysql_database import get_connection
from ingest.raw_store import read_payload
from ingest.replay import FixtureSession, route_for
from jobs.base import utc_now
from reprocess import reprocess_run

from .base import (
    CANNED_RESPONSES,
    TRACKED_APPIDS,
    IntegrationTestCase,
    review_summary,
)


CHANGED_CHART = {
    "valve_top_concurrent": {
        "response": {
            "last_update": 1_790_000_000,
            "ranks": [{"rank": 1, "appid": 730, "concurrent_in_game": 1}],
        }
    }
}


class RetriedOnce(FixtureSession):
    """Answers like FixtureSession; the given routes look as if the HTTP
    session had retried them once (a 503) before this answer."""

    def __init__(self, routes, overrides):
        super().__init__(overrides)
        self.routes = routes

    def get(self, url, params=None, timeout=None):
        response = super().get(url, params, timeout)

        if route_for(url, params) in self.routes:
            response.raw = SimpleNamespace(retries=SimpleNamespace(
                history=(RequestHistory("GET", url, None, 503, None),)
            ))

        return response


class RawLayerIntegrationTests(IntegrationTestCase):
    def raw_rows(self, run_id=None):
        if run_id is None:
            return self.query(
                "SELECT * FROM raw_api_responses ORDER BY response_id"
            )

        return self.query(
            "SELECT * FROM raw_api_responses WHERE run_id = %s"
            " ORDER BY response_id",
            (run_id,)
        )

    def read(self, response_id):
        connection = get_connection()

        try:
            return read_payload(connection, response_id)
        finally:
            connection.close()

    def test_every_response_is_stored_before_parsing(self):
        runs = self.run_pipeline()

        self.assertEqual(
            {
                job: Counter(row["source"] for row in self.raw_rows(run_id))
                for job, run_id in runs.items()
            },
            {
                # Factorio's appdetails is fetched twice: by the catalog to
                # backfill its games row, by the refresh for its tags.
                "catalog_daily": {
                    "steamspy_all": 1,
                    "steamspy_appdetails": 1,
                    "store_prices": 1,
                },
                "players_hourly": {
                    "valve_top_concurrent": 1,
                    "valve_current_players": 2,
                    "valve_most_played": 1,
                },
                "refresh_hourly": {
                    "steamspy_appdetails": 4,
                    "store_appdetails": 4,
                },
                "reviews_daily": {
                    "steam_review_summary": 3,
                    "steam_review_histogram": 3,
                    "steam_news": 3,
                },
            }
        )

        rows = self.raw_rows()
        duplicates = [row for row in rows if row["is_duplicate_of"]]
        self.assertEqual(
            [row["source"] for row in duplicates],
            ["steamspy_appdetails"]
        )

        for row in rows:
            if row["is_duplicate_of"]:
                continue

            body = zlib.decompress(row["payload"])
            self.assertEqual(row["http_status"], 200)
            self.assertEqual(row["payload_bytes"], len(body))
            self.assertEqual(
                row["payload_sha256"],
                hashlib.sha256(body).hexdigest()
            )

        (spy,) = [
            row for row in self.raw_rows() if row["source"] == "steamspy_all"
        ]
        self.assertEqual(
            json.loads(zlib.decompress(spy["payload"])),
            CANNED_RESPONSES["steamspy_all"]
        )
        self.assertEqual(
            json.loads(spy["request_params"]),
            {"page": 0, "request": "all"}
        )

    @patch.dict(os.environ, {"TAG_REFRESH_DAYS": "0"})
    def test_an_identical_payload_is_stored_once(self):
        # TAG_REFRESH_DAYS 0: the second run repeats every request, tags
        # included, so every response can be compared with the first run's.
        self.run_pipeline()
        first_rows = self.raw_rows()
        # Body holders by request; a request repeated within the first run
        # is itself a duplicate.
        first = {
            row["request_params_hash"]: row
            for row in first_rows
            if row["is_duplicate_of"] is None
        }
        second_runs = self.run_pipeline()
        second = [
            row
            for run_id in second_runs.values()
            for row in self.raw_rows(run_id)
        ]

        self.assertEqual(len(second), len(first_rows))

        for row in second:
            original = first[row["request_params_hash"]]
            self.assertIsNone(row["payload"])
            self.assertEqual(row["is_duplicate_of"], original["response_id"])
            self.assertEqual(row["payload_sha256"], original["payload_sha256"])
            self.assertEqual(
                self.read(row["response_id"]),
                zlib.decompress(original["payload"])
            )

        # Both runs still loaded their data from the stored payloads.
        self.assertEqual(self.count("game_metric_snapshots"), 8)
        self.assertEqual(self.count("player_observations"), 6)

    def test_a_changed_payload_is_stored_again(self):
        self.run_pipeline()
        second_run = self.run_job("players_hourly", CHANGED_CHART)

        (chart,) = [
            row for row in self.raw_rows(second_run)
            if row["source"] == "valve_top_concurrent"
        ]
        self.assertIsNotNone(chart["payload"])
        self.assertIsNone(chart["is_duplicate_of"])

    def loaded_rows(self, runs):
        return (
            self.query(
                "SELECT * FROM game_metric_snapshots WHERE run_id = %s"
                " ORDER BY appid",
                (runs["catalog_daily"],)
            ),
            self.query(
                "SELECT run_id, appid, collected_at, source_measured_at,"
                " current_players, player_rank, source"
                " FROM player_observations WHERE run_id = %s ORDER BY appid",
                (runs["players_hourly"],)
            ),
            self.query(
                "SELECT appid, observation_date, peak_players, avg_players,"
                " min_players, observation_count, best_rank, updated_at"
                " FROM player_daily ORDER BY appid, observation_date"
            ),
            self.query(
                "SELECT run_id, appid, collected_at, positive_reviews,"
                " negative_reviews, total_reviews, review_score_percent,"
                " review_score, review_score_desc"
                " FROM review_observations WHERE run_id = %s ORDER BY appid",
                (runs["reviews_daily"],)
            ),
            self.query("SELECT * FROM games ORDER BY appid"),
            self.query("SELECT * FROM most_played_daily ORDER BY appid"),
            self.query("SELECT * FROM game_details ORDER BY appid"),
            self.query("SELECT * FROM game_price_history ORDER BY price_id"),
            self.query(
                "SELECT * FROM review_rollups"
                " ORDER BY appid, rollup_type, period_start"
            ),
            self.query(
                "SELECT run_id, appid, checked_at, histogram_total,"
                " summary_total, count_difference_percent,"
                " share_difference_points, within_tolerance"
                " FROM review_reconciliation ORDER BY run_id, appid"
            ),
            self.query("SELECT * FROM game_events ORDER BY gid"),
            self.query(
                "SELECT * FROM game_genres ORDER BY appid, genre_id"
            ),
            self.query(
                "SELECT appid, name, votes FROM game_tags JOIN tags"
                " USING (tag_id) ORDER BY appid, name"
            ),
            self.query(
                "SELECT appid, name, position FROM game_developers"
                " JOIN companies USING (company_id) ORDER BY appid, name"
            ),
            self.query(
                "SELECT run_id, rows_extracted, rows_loaded,"
                " measurements_loaded, rows_rejected, warning_message, status"
                " FROM pipeline_runs ORDER BY run_id"
            ),
        )

    @patch.dict(os.environ, {"REVIEW_SUMMARIES_PER_RUN": "5"})
    def test_reprocessing_a_run_with_retried_requests(self):
        # Since v1.3.0 each attempt the HTTP session retried is stored as a
        # failed row just before the answer. Replaying such a run must use
        # the answer, not fail the request on the retried row.
        self.run_job("catalog_daily")
        session = RetriedOnce(
            {"store_appdetails:570", "steam_review_summary:570"},
            {**CANNED_RESPONSES,
             "steam_review_summary:570": review_summary(800, 200)}
        )
        run_id = self.run_job("refresh_hourly", session=session)

        details = self.query("SELECT * FROM game_details WHERE appid = 570")
        reviews = self.query(
            "SELECT appid, total_reviews FROM review_observations"
            " WHERE run_id = %s", (run_id,)
        )
        (retried,) = self.query(
            "SELECT COUNT(*) AS n FROM raw_api_responses"
            " WHERE run_id = %s AND error_message LIKE 'retried:%%'",
            (run_id,)
        )
        self.assertEqual(retried["n"], 2)
        self.assertEqual(reviews, [{"appid": 570, "total_reviews": 1000}])

        with patch.dict(os.environ, {"STEAM_TRACKED_APPIDS": TRACKED_APPIDS}):
            reprocess_run(run_id)

        self.assertEqual(
            self.query("SELECT * FROM game_details WHERE appid = 570"),
            details
        )
        self.assertEqual(
            self.query(
                "SELECT appid, total_reviews FROM review_observations"
                " WHERE run_id = %s", (run_id,)
            ),
            reviews
        )
        self.assertIsNone(self.run_row(run_id)["warning_message"])

    def test_reprocessing_runs_rebuilds_identical_rows(self):
        runs = self.run_pipeline({
            "steam_review_summary:620": (502, {"error": "bad gateway"})
        })
        before = self.loaded_rows(runs)
        raw_count = self.count("raw_api_responses")

        with patch.dict(os.environ, {"STEAM_TRACKED_APPIDS": TRACKED_APPIDS}):
            for run_id in runs.values():
                reprocess_run(run_id)

            after_first = self.loaded_rows(runs)

            for run_id in runs.values():
                reprocess_run(run_id)

            after_second = self.loaded_rows(runs)

        self.assertEqual(len(before[0]), 4)
        self.assertEqual(len(before[1]), 3)
        self.assertEqual(len(before[3]), 2)
        self.assertIn(
            "app 620",
            self.run_row(runs["reviews_daily"])["warning_message"]
        )
        self.assertEqual(after_first, before)
        self.assertEqual(after_second, before)
        # Reprocessing reads the landing layer; it writes nothing to it.
        self.assertEqual(self.count("raw_api_responses"), raw_count)

    def test_reprocessing_needs_stored_raw_responses(self):
        catalog_run = self.execute(
            "INSERT INTO pipeline_runs (job_name, started_at, finished_at,"
            " status) VALUES ('catalog_daily', NOW(6), NOW(6), 'SUCCESS')"
        )
        legacy_run = self.execute(
            "INSERT INTO pipeline_runs (started_at, finished_at, status)"
            " VALUES (NOW(6), NOW(6), 'SUCCESS')"
        )

        with self.assertRaisesRegex(ValueError, "no stored raw responses"):
            reprocess_run(catalog_run)

        with self.assertRaisesRegex(ValueError, "'daily_legacy'"):
            reprocess_run(legacy_run)

    def test_retention_purges_unreferenced_old_bodies(self):
        long_ago = utc_now() - timedelta(days=40)
        old_runs = self.run_pipeline(now_fn=lambda: long_ago)

        # Retention runs at the end of the catalog job. Its own requests
        # repeat the old catalog bodies, so those stay; every other old body
        # has no recent duplicate yet and is purged.
        self.run_job("catalog_daily")

        # Duplicates never had a body of their own; compare body holders.
        old_rows = [
            row
            for run_id in old_runs.values()
            for row in self.raw_rows(run_id)
            if row["is_duplicate_of"] is None
        ]
        purged = Counter(
            row["source"] for row in old_rows if row["payload"] is None
        )
        kept = Counter(
            row["source"] for row in old_rows if row["payload"] is not None
        )

        # The catalog job repeats the page, Factorio's backfill and the
        # prices; Store details and tags come from refresh_hourly (ADR 6).
        self.assertEqual(
            kept,
            {
                "steamspy_all": 1,
                "steamspy_appdetails": 1,
                "store_prices": 1,
            }
        )
        self.assertEqual(
            purged,
            {
                "steamspy_appdetails": 3,
                "store_appdetails": 4,
                "valve_top_concurrent": 1,
                "valve_current_players": 2,
                "valve_most_played": 1,
                "steam_review_summary": 3,
                "steam_review_histogram": 3,
                "steam_news": 3,
            }
        )

        for row in old_rows:
            self.assertIsNotNone(row["payload_sha256"])
            self.assertEqual(row["http_status"], 200)

            if row["payload"] is None:
                self.assertIsNotNone(row["payload_purged_at"])

                with self.assertRaisesRegex(LookupError, "purged"):
                    self.read(row["response_id"])

        # An identical response whose original body was purged stores its
        # body again instead of pointing at the purged row.
        new_players = self.run_job("players_hourly")

        for row in self.raw_rows(new_players):
            self.assertIsNotNone(row["payload"])
            self.assertIsNone(row["is_duplicate_of"])


if __name__ == "__main__":
    unittest.main()
