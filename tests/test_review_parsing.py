import json
import unittest
from datetime import date, datetime
from pathlib import Path

from parsing.reviews import (
    SHARE_TOLERANCE_POINTS,
    parse_histogram,
    parse_news,
    reconcile,
)


FIXTURES = Path(__file__).parent / "fixtures" / "sources"
FETCHED_AT = datetime(2026, 10, 1, 17, 46)


def fixture(path):
    return json.loads((FIXTURES / path).read_text())


class HistogramTests(unittest.TestCase):
    def test_recorded_histogram(self):
        rows = parse_histogram(
            fixture("review_histogram/730.json"),
            FETCHED_AT
        )
        months = [row for row in rows if row["rollup_type"] == "month"]
        days = [row for row in rows if row["rollup_type"] == "day"]

        self.assertEqual(len(months), 172)
        self.assertEqual(len(days), 30)
        self.assertEqual(months[0]["period_start"], date(2012, 5, 1))
        self.assertEqual(months[0]["recommendations_up"], 1)

        # Only the buckets containing the fetch date are partial.
        self.assertEqual(
            [row["period_start"] for row in rows if row["is_partial"]],
            [date(2026, 10, 1), date(2026, 10, 1)]
        )
        self.assertFalse(months[-2]["is_partial"])

    def test_unexpected_rollup_type_is_rejected(self):
        payload = fixture("review_histogram/730.json")
        payload["results"]["rollup_type"] = "week"

        with self.assertRaisesRegex(ValueError, "rollup_type 'week'"):
            parse_histogram(payload, FETCHED_AT)

    def test_unsuccessful_response_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "not successful"):
            parse_histogram({"success": 2}, FETCHED_AT)


class ReconciliationTests(unittest.TestCase):
    def summary(self, appid):
        query = fixture(f"review_summary/{appid}.json")["query_summary"]
        return {
            "positive_reviews": query["total_positive"],
            "total_reviews": query["total_reviews"],
        }

    def test_recorded_responses_agree_on_share_not_count(self):
        result = reconcile(
            parse_histogram(fixture("review_histogram/427520.json"), FETCHED_AT),
            self.summary(427520)
        )

        self.assertEqual(result["histogram_total"], 204_368)
        self.assertEqual(result["summary_total"], 239_968)
        self.assertEqual(result["count_difference_percent"], -14.84)
        self.assertEqual(result["histogram_positive_share"], 96.88)
        self.assertEqual(result["summary_positive_share"], 97.2)
        self.assertEqual(result["share_difference_points"], 0.32)
        self.assertEqual(result["tolerance_points"], SHARE_TOLERANCE_POINTS)
        self.assertTrue(result["within_tolerance"])

    def test_share_outside_the_tolerance(self):
        result = reconcile(
            parse_histogram(fixture("review_histogram/620.json"), FETCHED_AT),
            {"positive_reviews": 50, "total_reviews": 100}
        )

        self.assertFalse(result["within_tolerance"])
        self.assertGreater(result["share_difference_points"], 40)


class NewsTests(unittest.TestCase):
    def test_recorded_news(self):
        events = parse_news(fixture("news/730.json"))

        self.assertEqual(len(events), 20)
        self.assertEqual(sum(event["is_patch_notes"] for event in events), 15)
        self.assertEqual(
            sum(event["is_developer_post"] for event in events),
            17
        )
        first = events[0]
        self.assertEqual(first["gid"], "1845383656379136")
        self.assertEqual(first["title"], "Counter-Strike 2 Update")
        self.assertEqual(first["event_time"], datetime(2026, 9, 30, 22, 46, 19))
        self.assertTrue(first["is_patch_notes"])

    def test_items_without_tags_are_not_patch_notes(self):
        events = parse_news({"appnews": {"newsitems": [{
            "gid": 1, "appid": 620, "title": "Hi", "date": 0, "feed_type": 0
        }]}})

        self.assertFalse(events[0]["is_patch_notes"])
        self.assertFalse(events[0]["is_developer_post"])

    def test_missing_appnews_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "appnews"):
            parse_news({})


if __name__ == "__main__":
    unittest.main()
