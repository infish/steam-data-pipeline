import hashlib
import json
import os
import unittest
import zlib
from collections import Counter
from datetime import timedelta
from unittest.mock import patch

from database.mysql_database import get_connection
from ingest.raw_store import read_payload
from jobs.base import utc_now
from reprocess import reprocess_run

from .base import CANNED_RESPONSES, TRACKED_APPIDS, IntegrationTestCase


CHANGED_CHART = {
    "valve_top_concurrent": {
        "response": {
            "last_update": 1_790_000_000,
            "ranks": [{"rank": 1, "appid": 730, "concurrent_in_game": 1}],
        }
    }
}


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
                "catalog_daily": {"steamspy_all": 1, "steamspy_appdetails": 1},
                "players_hourly": {
                    "valve_top_concurrent": 1,
                    "valve_current_players": 2,
                    "valve_most_played": 1,
                },
                "reviews_daily": {"steam_review_summary": 3},
            }
        )

        for row in self.raw_rows():
            body = zlib.decompress(row["payload"])
            self.assertEqual(row["http_status"], 200)
            self.assertEqual(row["payload_bytes"], len(body))
            self.assertEqual(
                row["payload_sha256"],
                hashlib.sha256(body).hexdigest()
            )
            self.assertIsNone(row["is_duplicate_of"])

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

    def test_an_identical_payload_is_stored_once(self):
        self.run_pipeline()
        first = {row["request_params_hash"]: row for row in self.raw_rows()}
        second_runs = self.run_pipeline()
        second = [
            row
            for run_id in second_runs.values()
            for row in self.raw_rows(run_id)
        ]

        self.assertEqual(len(second), len(first))

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
            self.query(
                "SELECT run_id, rows_extracted, rows_loaded,"
                " measurements_loaded, rows_rejected, warning_message, status"
                " FROM pipeline_runs ORDER BY run_id"
            ),
        )

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

        old_rows = [
            row
            for run_id in old_runs.values()
            for row in self.raw_rows(run_id)
        ]
        purged = Counter(
            row["source"] for row in old_rows if row["payload"] is None
        )
        kept = Counter(
            row["source"] for row in old_rows if row["payload"] is not None
        )

        self.assertEqual(kept, {"steamspy_all": 1, "steamspy_appdetails": 1})
        self.assertEqual(
            purged,
            {
                "valve_top_concurrent": 1,
                "valve_current_players": 2,
                "valve_most_played": 1,
                "steam_review_summary": 3,
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
