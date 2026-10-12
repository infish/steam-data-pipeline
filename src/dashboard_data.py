"""Pure data shaping for dashboard charts (no Streamlit or database access)."""

import pandas as pd


TIME_COLUMNS = ("source_measured_at", "collected_at", "finished_at")


def build_measurement_series(runs, measurements, first_measured_run_id):
    """Return one row per successful run, with NaN where the game was missed.

    runs: successful runs of the job that collects the metric, with run_id
        and finished_at.
    measurements: the selected game's rows for that metric, with run_id,
        collected_at, optionally source_measured_at, and value columns.
    first_measured_run_id: the first run that stored any such measurement;
        earlier runs predate collection and are left out.

    A run without a measurement for the game keeps NaN values, so a line
    chart breaks there instead of drawing across the gap. The x value,
    measurement_time, falls back from Valve's source time to collection time
    to the run's finish time.
    """
    measurements = measurements.copy()

    if "source_measured_at" not in measurements.columns:
        measurements["source_measured_at"] = pd.NaT

    value_columns = [
        column for column in measurements.columns
        if column not in ("run_id", "source_measured_at", "collected_at")
    ]
    columns = [
        "run_id", "finished_at", "collected_at", "source_measured_at",
        *value_columns, "measurement_time"
    ]

    if first_measured_run_id is None or pd.isna(first_measured_run_id):
        return pd.DataFrame(columns=columns)

    series_runs = runs.loc[
        runs["run_id"] >= first_measured_run_id,
        ["run_id", "finished_at"]
    ]
    series = series_runs.merge(measurements, on="run_id", how="left")

    for column in TIME_COLUMNS:
        series[column] = pd.to_datetime(series[column])

    series["measurement_time"] = (
        series["source_measured_at"]
        .fillna(series["collected_at"])
        .fillna(series["finished_at"])
    )

    return series.sort_values("run_id").reset_index(drop=True)[columns]
