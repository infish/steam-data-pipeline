import unittest
from datetime import datetime

import pandas as pd

from dashboard_data import build_measurement_series


def runs(*run_ids):
    return pd.DataFrame({
        "run_id": list(run_ids),
        "finished_at": [datetime(2026, 9, run_id, 18, 5) for run_id in run_ids]
    })


def measurement(run_id, players, source=None, collected=None):
    return {
        "run_id": run_id,
        "collected_at": collected,
        "source_measured_at": source,
        "current_players": players,
        "total_reviews": None,
        "positive_reviews": None,
        "negative_reviews": None,
        "review_score_percent": None,
    }


class BuildMeasurementSeriesTests(unittest.TestCase):
    def test_missing_run_produces_a_nan_row(self):
        measurements = pd.DataFrame([
            measurement(1, 100, collected=datetime(2026, 9, 1, 18, 1)),
            measurement(3, 300, collected=datetime(2026, 9, 3, 18, 1)),
        ])

        series = build_measurement_series(runs(1, 2, 3), measurements, 1)

        self.assertEqual(list(series["run_id"]), [1, 2, 3])
        self.assertEqual(series.loc[0, "current_players"], 100)
        self.assertTrue(pd.isna(series.loc[1, "current_players"]))
        self.assertEqual(series.loc[2, "current_players"], 300)
        self.assertEqual(
            series.loc[1, "measurement_time"],
            datetime(2026, 9, 2, 18, 5)
        )

    def test_measurement_time_prefers_source_then_collected_then_finished(self):
        source = datetime(2026, 9, 1, 17, 50)
        collected = datetime(2026, 9, 2, 18, 1)
        measurements = pd.DataFrame([
            measurement(1, 100, source=source, collected=datetime(2026, 9, 1, 18, 1)),
            measurement(2, 200, collected=collected),
        ])

        series = build_measurement_series(runs(1, 2, 3), measurements, 1)

        self.assertEqual(
            list(series["measurement_time"]),
            [source, collected, datetime(2026, 9, 3, 18, 5)]
        )

    def test_runs_before_first_measured_run_are_left_out(self):
        measurements = pd.DataFrame([
            measurement(4, 400, collected=datetime(2026, 9, 4, 18, 1)),
        ])

        series = build_measurement_series(runs(1, 2, 3, 4, 5), measurements, 3)

        self.assertEqual(list(series["run_id"]), [3, 4, 5])
        self.assertTrue(pd.isna(series.loc[0, "current_players"]))
        self.assertEqual(series.loc[1, "current_players"], 400)
        self.assertTrue(pd.isna(series.loc[2, "current_players"]))

    def test_rows_are_ordered_by_run(self):
        measurements = pd.DataFrame([
            measurement(2, 200, collected=datetime(2026, 9, 2, 18, 1)),
            measurement(1, 100, collected=datetime(2026, 9, 1, 18, 1)),
        ])

        series = build_measurement_series(runs(2, 1), measurements, 1)

        self.assertEqual(list(series["current_players"]), [100, 200])

    def test_no_measurements_at_all_gives_an_empty_series(self):
        series = build_measurement_series(
            runs(1, 2),
            pd.DataFrame(columns=["run_id", "collected_at"]),
            None
        )

        self.assertTrue(series.empty)
        self.assertIn("measurement_time", series.columns)


if __name__ == "__main__":
    unittest.main()
