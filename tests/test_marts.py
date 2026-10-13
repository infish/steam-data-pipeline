import unittest
from pathlib import Path

from marts.build import TRANSFORM_DIR, split_statements, transform_paths
from marts.tests import MART_TESTS, describe_range, range_condition


class SplitStatementsTests(unittest.TestCase):
    def test_comments_are_dropped_and_statements_split(self):
        sql = (
            "-- header\n"
            "DELETE FROM mart_x;\n"
            "\n"
            "INSERT INTO mart_x (a)\n"
            "-- inline comment line\n"
            "SELECT 1;\n"
        )

        self.assertEqual(
            split_statements(sql),
            ["DELETE FROM mart_x", "INSERT INTO mart_x (a)\nSELECT 1"]
        )

    def test_unterminated_statement_is_an_error(self):
        with self.assertRaisesRegex(ValueError, "terminating"):
            split_statements("DELETE FROM mart_x")


class TransformFileTests(unittest.TestCase):
    def test_every_transform_documents_and_rebuilds_one_tested_mart(self):
        paths = transform_paths()
        marts = []

        for path in paths:
            with self.subTest(file=path.name):
                text = path.read_text()
                mart = path.stem.split("_", 1)[1]
                statements = split_statements(text)

                self.assertTrue(text.startswith(f"-- {mart}\n"))
                for heading in ("-- Grain:", "-- Sources:", "-- Refresh:"):
                    self.assertIn(heading, text)
                self.assertEqual(statements[0], f"DELETE FROM {mart}")
                self.assertTrue(
                    statements[1].startswith(f"INSERT INTO {mart} (")
                )
                self.assertEqual(len(statements), 2)
                marts.append(mart)

        self.assertEqual(sorted(marts), sorted(MART_TESTS))
        self.assertEqual(TRANSFORM_DIR, Path(paths[0]).parent)


class RangeTests(unittest.TestCase):
    def test_conditions(self):
        self.assertEqual(range_condition("c", 0, 100), "c < 0 OR c > 100")
        self.assertEqual(range_condition("c", 0, None), "c < 0")
        self.assertEqual(describe_range(0, None), ">= 0")
        self.assertEqual(describe_range(0, 23), "0..23")


if __name__ == "__main__":
    unittest.main()
