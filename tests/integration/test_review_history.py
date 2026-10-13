import json
import os
import unittest
from datetime import date, datetime
from pathlib import Path
from unittest.mock import patch

from backfill import backfill
from reprocess import reprocess_run

from .base import TRACKED_APPIDS, IntegrationTestCase


FIXTURES = Path(__file__).parents[1] / "fixtures" / "sources"


def fixture(path):
    return json.loads((FIXTURES / path).read_text())


def at(day, hour=18):
    moment = datetime(2026, 10, day, hour, 0)
    return lambda: moment


class ReviewHistoryIntegrationTests(IntegrationTestCase):
    def setUp(self):
        super().setUp()
        self.run_job("catalog_daily", now_fn=at(1, 17))

    def counts(self):
        return {
            table: self.count(table)
            for table in ("review_rollups", "game_events")
        }

    def test_reviews_run_loads_rollups_reconciliation_and_events(self):
        run_id = self.run_job("reviews_daily", now_fn=at(1))

        rollups = {
            (row["appid"], row["rollup_type"]): row["n"]
            for row in self.query(
                "SELECT appid, rollup_type, COUNT(*) AS n"
                " FROM review_rollups GROUP BY appid, rollup_type"
            )
        }
        self.assertEqual(rollups, {
            (730, "month"): 172, (730, "day"): 30,
            (620, "month"): 187, (620, "day"): 30,
            (427520, "month"): 129, (427520, "day"): 30,
        })
        (first_month,) = self.query(
            "SELECT * FROM review_rollups WHERE appid = 730"
            " AND rollup_type = 'month' ORDER BY period_start LIMIT 1"
        )
        self.assertEqual(first_month["period_start"], date(2012, 5, 1))
        partial = self.query(
            "SELECT appid, rollup_type FROM review_rollups WHERE is_partial"
        )
        self.assertEqual(len(partial), 6)

        reconciliation = self.query(
            "SELECT * FROM review_reconciliation WHERE run_id = %s",
            (run_id,)
        )
        self.assertEqual(len(reconciliation), 3)
        self.assertTrue(all(row["within_tolerance"] for row in reconciliation))

        events = self.query("SELECT * FROM game_events")
        # Portal 2's feed has two press items filed under another app (70);
        # only items for the requested app are kept.
        self.assertEqual(len(events), 58)
        self.assertEqual(
            {row["appid"] for row in events},
            {730, 620, 427520}
        )
        self.assertEqual(sum(row["is_patch_notes"] for row in events), 33)
        (update,) = self.query(
            "SELECT * FROM game_events WHERE gid = '1845383656379136'"
        )
        self.assertEqual(update["appid"], 730)
        self.assertEqual(update["event_time"], datetime(2026, 9, 30, 22, 46, 19))
        self.assertEqual(update["first_seen_at"], at(1)())

        self.assertIsNone(self.run_row(run_id)["warning_message"])

    def test_running_twice_creates_no_duplicates(self):
        self.run_job("reviews_daily", now_fn=at(1))
        before = self.counts()

        second = self.run_job("reviews_daily", now_fn=at(2))

        self.assertEqual(self.counts(), before)
        self.assertEqual(
            self.query(
                "SELECT COUNT(DISTINCT run_id) AS n FROM review_rollups"
            )[0]["n"],
            1
        )
        # The newer fetch owns the rollups; events keep their first sighting.
        self.assertEqual(
            {row["run_id"] for row in self.query(
                "SELECT run_id FROM review_rollups"
            )},
            {second}
        )
        self.assertEqual(
            {row["first_seen_at"] for row in self.query(
                "SELECT first_seen_at FROM game_events"
            )},
            {at(1)()}
        )
        self.assertEqual(self.count("review_reconciliation"), 6)

    def test_backfill_twice_creates_no_duplicates(self):
        options = {
            "session_factory": lambda: self.session(),
            "sleep_fn": lambda seconds: None,
        }

        first = backfill("review_histogram", [730, 620], **options)
        rollups = self.count("review_rollups")
        backfill("review_histogram", [730, 620], **options)
        backfill("news", [730], count=100, **options)
        events = self.count("game_events")
        backfill("news", [730], count=100, **options)

        self.assertEqual(rollups, 172 + 30 + 187 + 30)
        self.assertEqual(self.count("review_rollups"), rollups)
        self.assertEqual(events, 20)
        self.assertEqual(self.count("game_events"), events)
        self.assertEqual(self.run_row(first)["job_name"], "backfill")
        # Backfill compares nothing with summaries.
        self.assertEqual(self.count("review_reconciliation"), 0)

    def test_events_are_deduplicated_by_gid(self):
        self.run_job("reviews_daily", now_fn=at(1))
        news = fixture("news/730.json")
        news["appnews"]["newsitems"].insert(0, {
            **news["appnews"]["newsitems"][0],
            "gid": "9999999999999999",
            "title": "Counter-Strike 2 Hotfix",
            "date": 1_790_900_000,
        })

        with self.assertLogs(level="INFO") as logs:
            self.run_job(
                "reviews_daily",
                {"steam_news:730": news},
                now_fn=at(2)
            )

        self.assertIn("Stored 1 new game event(s)", "\n".join(logs.output))
        self.assertEqual(self.count("game_events"), 59)
        (hotfix,) = self.query(
            "SELECT * FROM game_events WHERE gid = '9999999999999999'"
        )
        self.assertEqual(hotfix["first_seen_at"], at(2)())

    def test_reconciliation_outside_tolerance_is_a_warning(self):
        summary = {
            "success": 1,
            "query_summary": {
                "total_positive": 50,
                "total_negative": 50,
                "total_reviews": 100,
            },
        }

        with self.assertLogs(level="WARNING"):
            run_id = self.run_job(
                "reviews_daily",
                {"steam_review_summary:620": summary},
                now_fn=at(1)
            )

        (row,) = self.query(
            "SELECT * FROM review_reconciliation WHERE appid = 620"
        )
        self.assertFalse(row["within_tolerance"])
        self.assertEqual(float(row["summary_positive_share"]), 50.0)
        self.assertIn(
            "Review histogram for app 620 disagrees with the review summary",
            self.run_row(run_id)["warning_message"]
        )

    def test_reprocessing_an_older_run_keeps_newer_rollups(self):
        first = self.run_job("reviews_daily", now_fn=at(1))
        changed = fixture("review_histogram/730.json")
        changed["results"]["rollups"][-1]["recommendations_up"] += 500
        self.run_job(
            "reviews_daily",
            {"steam_review_histogram:730": changed},
            now_fn=at(2)
        )
        query = (
            "SELECT * FROM review_rollups WHERE appid = 730"
            " AND rollup_type = 'month' AND period_start = '2026-10-01'"
        )
        (newest,) = self.query(query)

        with patch.dict(os.environ, {"STEAM_TRACKED_APPIDS": TRACKED_APPIDS}):
            reprocess_run(first)

        self.assertEqual(self.query(query), [newest])
        self.assertEqual(newest["fetched_at"], at(2)())


if __name__ == "__main__":
    unittest.main()
