import unittest
from datetime import datetime

import pandas as pd

from dashboard_data import (
    build_measurement_series,
    price_steps,
    request_budget_usage,
    with_missing_days,
)


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

    def test_review_series_without_source_time_uses_collection_time(self):
        reviews = pd.DataFrame([
            {"run_id": 1, "collected_at": datetime(2026, 9, 1, 18, 1),
             "total_reviews": 100},
        ])

        series = build_measurement_series(runs(1, 2), reviews, 1)

        self.assertEqual(list(series["total_reviews"].fillna(-1)), [100, -1])
        self.assertEqual(
            list(series["measurement_time"]),
            [datetime(2026, 9, 1, 18, 1), datetime(2026, 9, 2, 18, 5)]
        )

    def test_no_measurements_at_all_gives_an_empty_series(self):
        series = build_measurement_series(
            runs(1, 2),
            pd.DataFrame(columns=["run_id", "collected_at"]),
            None
        )

        self.assertTrue(series.empty)
        self.assertIn("measurement_time", series.columns)



class MissingDaysTests(unittest.TestCase):
    def test_gap_days_become_nan_rows(self):
        daily = pd.DataFrame({
            "observation_date": [datetime(2026, 10, 1), datetime(2026, 10, 3)],
            "peak_players": [100, 300],
        })

        filled = with_missing_days(daily)

        self.assertEqual(len(filled), 3)
        self.assertTrue(pd.isna(filled.loc[1, "peak_players"]))
        self.assertEqual(filled.loc[2, "peak_players"], 300)


class PriceStepsTests(unittest.TestCase):
    def versions(self, *rows):
        return pd.DataFrame(
            rows,
            columns=["valid_from", "price_state", "currency", "final_price"]
        )

    def test_current_price_is_extended_and_cents_become_units(self):
        steps = price_steps(
            self.versions(
                (datetime(2026, 10, 1), "priced", "USD", 3500),
                (datetime(2026, 10, 5), "priced", "USD", 1750),
            ),
            datetime(2026, 10, 12)
        )

        self.assertEqual(list(steps["price"]), [35.0, 17.5, 17.5])
        self.assertEqual(steps["time"].iloc[-1], datetime(2026, 10, 12))

    def test_unpriced_and_other_currency_versions_break_the_line(self):
        steps = price_steps(
            self.versions(
                (datetime(2026, 10, 1), "priced", "GBP", 2500),
                (datetime(2026, 10, 2), "unavailable", None, None),
                (datetime(2026, 10, 3), "priced", "USD", 1999),
            ),
            datetime(2026, 10, 4)
        )

        self.assertTrue(pd.isna(steps["price"].iloc[0]))
        self.assertTrue(pd.isna(steps["price"].iloc[1]))
        self.assertEqual(steps["price"].iloc[2], 19.99)


class RequestBudgetTests(unittest.TestCase):
    def test_sums_sources_and_finds_the_busiest_window(self):
        budget = [{
            "budget": "Store",
            "sources": ("a", "b"),
            "planned_per_day": 100,
            "limit": "150 per 5 min",
            "window_limit": 150,
        }]

        usage = request_budget_usage(
            {"a": 30, "b": 20, "c": 999},
            {"a": {1: 10, 2: 20}, "b": {2: 5, 3: 15}},
            budget
        ).iloc[0]

        self.assertEqual(usage["requests_last_24h"], 50)
        self.assertEqual(usage["used_pct_of_plan"], 50.0)
        self.assertEqual(usage["busiest_5_min"], 25)
        self.assertTrue(usage["within_limit"])


if __name__ == "__main__":
    unittest.main()
