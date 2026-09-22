import unittest

from catalog_import import retry_after_seconds, validate_record


class CatalogValidationTests(unittest.TestCase):
    def test_accepts_hidden_id_record(self):
        row = validate_record("999999", {"appid": 999999, "name": "Hidden", "owners": "0 .. 20,000"})
        self.assertEqual(row["appid"], 999999)

    def test_rejects_overlength_without_truncation(self):
        with self.assertRaisesRegex(ValueError, "exceeds"):
            validate_record("1", {"appid": 1, "name": "x" * 256, "owners": "0 .. 20,000"})

    def test_rejects_unexpected_record_type(self):
        with self.assertRaisesRegex(ValueError, "not an object"):
            validate_record("1", [])

    def test_rejects_reversed_owner_range(self):
        with self.assertRaisesRegex(ValueError, "reversed"):
            validate_record("1", {"appid": 1, "name": "Bad", "owners": "20,000 .. 0"})

    def test_retry_after_delta_seconds(self):
        self.assertEqual(retry_after_seconds("90"), 90)
        self.assertEqual(retry_after_seconds("invalid"), 0)


if __name__ == "__main__":
    unittest.main()
