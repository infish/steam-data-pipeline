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
    def raw_rows(self, run_id):
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

    def test_a_run_stores_every_response_before_parsing(self):
        run_id = self.run_pipeline()
        rows = self.raw_rows(run_id)

        self.assertEqual(
            Counter(row["source"] for row in rows),
            {
                "steamspy_all": 1,
                "valve_top_concurrent": 1,
                "valve_current_players": 2,
                "steam_review_summary": 3,
                "steamspy_appdetails": 1,
            }
        )

        for row in rows:
            body = zlib.decompress(row["payload"])
            self.assertEqual(row["http_status"], 200)
            self.assertEqual(row["payload_bytes"], len(body))
            self.assertEqual(
                row["payload_sha256"],
                hashlib.sha256(body).hexdigest()
            )
            self.assertIsNone(row["is_duplicate_of"])

        (spy,) = [row for row in rows if row["source"] == "steamspy_all"]
        self.assertEqual(
            json.loads(zlib.decompress(spy["payload"])),
            CANNED_RESPONSES["steamspy_all"]
        )
        self.assertEqual(
            json.loads(spy["request_params"]),
            {"page": 0, "request": "all"}
        )

    def test_an_identical_payload_is_stored_once(self):
        first_run = self.run_pipeline()
        second_run = self.run_pipeline()
        first = {row["request_params_hash"]: row
                 for row in self.raw_rows(first_run)}
        second = self.raw_rows(second_run)

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

    def test_a_changed_payload_is_stored_again(self):
        self.run_pipeline()
        second_run = self.run_pipeline(CHANGED_CHART)

        (chart,) = [
            row for row in self.raw_rows(second_run)
            if row["source"] == "valve_top_concurrent"
        ]
        self.assertIsNotNone(chart["payload"])
        self.assertIsNone(chart["is_duplicate_of"])

    def loaded_rows(self, run_id):
        return (
            self.query(
                "SELECT * FROM game_metric_snapshots WHERE run_id = %s"
                " ORDER BY appid",
                (run_id,)
            ),
            self.query(
                "SELECT run_id, appid, collected_at, source_measured_at,"
                " current_players, player_rank, positive_reviews,"
                " negative_reviews, total_reviews, review_score_percent"
                " FROM steam_game_measurements WHERE run_id = %s"
                " ORDER BY appid",
                (run_id,)
            ),
            self.query("SELECT * FROM games ORDER BY appid"),
            self.query(
                "SELECT rows_extracted, rows_loaded, measurements_loaded,"
                " rows_rejected, warning_message, status"
                " FROM pipeline_runs WHERE run_id = %s",
                (run_id,)
            ),
        )

    def test_reprocessing_a_run_rebuilds_identical_rows(self):
        run_id = self.run_pipeline({
            "steam_review_summary:620": (502, {"error": "bad gateway"})
        })
        before = self.loaded_rows(run_id)
        raw_count = self.count("raw_api_responses")

        with patch.dict(os.environ, {"STEAM_TRACKED_APPIDS": TRACKED_APPIDS}):
            reprocess_run(run_id)
            after_first = self.loaded_rows(run_id)
            reprocess_run(run_id)
            after_second = self.loaded_rows(run_id)

        self.assertEqual(len(before[0]), 4)
        self.assertEqual(len(before[1]), 3)
        self.assertIn("app 620", before[3][0]["warning_message"])
        self.assertEqual(after_first, before)
        self.assertEqual(after_second, before)
        # Reprocessing reads the landing layer; it writes nothing to it.
        self.assertEqual(self.count("raw_api_responses"), raw_count)

    def test_reprocessing_needs_stored_raw_responses(self):
        run_id = self.execute(
            "INSERT INTO pipeline_runs (started_at, finished_at, status)"
            " VALUES (NOW(6), NOW(6), 'SUCCESS')"
        )

        with self.assertRaisesRegex(ValueError, "no stored raw responses"):
            reprocess_run(run_id)

    def test_old_bodies_are_purged_unless_a_recent_duplicate_uses_them(self):
        long_ago = utc_now() - timedelta(days=40)
        old_run = self.run_pipeline(now_fn=lambda: long_ago)
        # Same payloads except the chart: the new run's duplicates point at
        # the old bodies, so only the old chart body is unreferenced.
        self.run_pipeline(CHANGED_CHART)

        old_rows = self.raw_rows(old_run)
        purged = [row for row in old_rows if row["payload"] is None]
        kept = [row for row in old_rows if row["payload"] is not None]

        self.assertEqual(
            [row["source"] for row in purged],
            ["valve_top_concurrent"]
        )
        self.assertIsNotNone(purged[0]["payload_purged_at"])
        self.assertIsNotNone(purged[0]["payload_sha256"])
        self.assertEqual(purged[0]["http_status"], 200)
        self.assertEqual(len(kept), len(old_rows) - 1)

        with self.assertRaisesRegex(LookupError, "purged"):
            self.read(purged[0]["response_id"])


if __name__ == "__main__":
    unittest.main()
