import unittest

from benchmarks.load import validate_target


class FakeCursor:
    def execute(self, query):
        self.query = query

    def fetchone(self):
        return ("steam-data-pipeline-isolated-benchmark",)

    def close(self):
        pass


class FakeConnection:
    def __init__(self, database):
        self.database = database

    def cursor(self):
        return FakeCursor()


class BenchmarkGuardTests(unittest.TestCase):
    def test_refuses_production_database_before_marker_query(self):
        with self.assertRaisesRegex(RuntimeError, "refusing"):
            validate_target(FakeConnection("steam_pipeline"))

    def test_accepts_named_marked_benchmark_database(self):
        validate_target(FakeConnection("steam_pipeline_bench"))


if __name__ == "__main__":
    unittest.main()
