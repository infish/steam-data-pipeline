import unittest

from benchmarks.generate import APPID_BASE, batch_checksum, batches, synthetic_row


class BenchmarkGeneratorTests(unittest.TestCase):
    def test_same_key_is_deterministic(self):
        first = synthetic_row(20260922, 123, 17)
        second = synthetic_row(20260922, 123, 17)
        self.assertEqual(first, second)
        self.assertEqual(batch_checksum([first]), batch_checksum([second]))

    def test_batch_size_does_not_change_records(self):
        small = [row for batch in batches(20260922, 19, 3, 4) for row in batch]
        large = [row for batch in batches(20260922, 19, 3, 19) for row in batch]
        self.assertEqual(small, large)

    def test_record_respects_schema_invariants(self):
        row = synthetic_row(20260922, 100000, 399)
        self.assertEqual(row["appid"], APPID_BASE + 100000)
        self.assertLessEqual(len(row["name"]), 255)
        self.assertLessEqual(row["owners_low"], row["owners_high"])
        self.assertEqual(row["positive_reviews"] + row["negative_reviews"], row["total_reviews"])
        self.assertGreaterEqual(row["review_score_percent"], 0)
        self.assertLessEqual(row["review_score_percent"], 100)
        self.assertGreaterEqual(row["discount_percent"], 0)
        self.assertLessEqual(row["discount_percent"], 100)

    def test_rejects_unbounded_batch(self):
        with self.assertRaisesRegex(ValueError, "between 1 and 1000"):
            list(batches(1, 10, 0, 1001))


if __name__ == "__main__":
    unittest.main()
