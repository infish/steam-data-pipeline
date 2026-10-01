"""Pure data shaping for dashboard charts (no Streamlit or database access)."""

import pandas as pd


MEASUREMENT_COLUMNS = [
    "collected_at",
    "source_measured_at",
    "current_players",
    "total_reviews",
    "positive_reviews",
    "negative_reviews",
    "review_score_percent",
]


def build_measurement_series(runs, measurements, first_measured_run_id):
    """Return one row per successful run, with NaN where the game was missed.

    runs: successful pipeline runs with run_id and finished_at.
    measurements: the selected game's rows from steam_game_measurements,
        with run_id and MEASUREMENT_COLUMNS.
    first_measured_run_id: the first run that stored any Valve measurement;
        earlier runs predate collection and are left out.

    A run without a measurement for the game keeps NaN metric values, so a
    line chart breaks there instead of drawing across the gap. The x value,
    measurement_time, falls back from Valve's source time to collection time
    to the run's finish time.
    """
    columns = ["run_id", "finished_at", *MEASUREMENT_COLUMNS, "measurement_time"]

    if first_measured_run_id is None or pd.isna(first_measured_run_id):
        return pd.DataFrame(columns=columns)

    series_runs = runs.loc[
        runs["run_id"] >= first_measured_run_id,
        ["run_id", "finished_at"]
    ]
    series = series_runs.merge(
        measurements[["run_id", *MEASUREMENT_COLUMNS]],
        on="run_id",
        how="left"
    )
    for column in ("source_measured_at", "collected_at", "finished_at"):
        series[column] = pd.to_datetime(series[column])

    series["measurement_time"] = (
        series["source_measured_at"]
        .fillna(series["collected_at"])
        .fillna(series["finished_at"])
    )

    return series.sort_values("run_id").reset_index(drop=True)[columns]
