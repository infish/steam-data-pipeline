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


def with_missing_days(daily, date_column="observation_date"):
    """Add a NaN row for every calendar day without data, so lines break."""
    if daily.empty:
        return daily

    daily = daily.copy()
    daily[date_column] = pd.to_datetime(daily[date_column])
    days = pd.date_range(daily[date_column].min(), daily[date_column].max())
    return (
        daily.set_index(date_column)
        .reindex(days)
        .rename_axis(date_column)
        .reset_index()
    )


def price_steps(versions, until):
    """Points for a step chart of a game's price history in one currency.

    versions: game_price_history rows (valid_from, price_state, currency,
    final_price) for one game and country, oldest first. Each version starts
    a step at valid_from; the current one is extended to `until`. Versions
    without a price, or in another currency than the latest priced one, are
    NaN so the line breaks instead of mixing currencies.
    """
    columns = ["time", "price", "price_state"]

    if versions.empty:
        return pd.DataFrame(columns=columns)

    priced = versions[versions["price_state"] == "priced"]
    currency = priced["currency"].iloc[-1] if not priced.empty else None
    rows = []

    for version in versions.itertuples(index=False):
        price = (
            version.final_price / 100
            if version.price_state == "priced"
            and version.currency == currency
            else float("nan")
        )
        rows.append((version.valid_from, price, version.price_state))

    last = rows[-1]
    rows.append((until, last[1], last[2]))
    steps = pd.DataFrame(rows, columns=columns)
    steps["time"] = pd.to_datetime(steps["time"])
    return steps


def request_budget_usage(counts, windows, budget):
    """Requests in the last 24 hours against each planned budget.

    counts: {source: requests in the last 24 h}; windows: {source:
    {5-minute window: requests}}. Returns one row per budget.
    """
    rows = []

    for entry in budget:
        used = sum(counts.get(source, 0) for source in entry["sources"])
        window_totals = {}

        for source in entry["sources"]:
            for window, count in windows.get(source, {}).items():
                window_totals[window] = window_totals.get(window, 0) + count

        rows.append({
            "budget": entry["budget"],
            "requests_last_24h": used,
            "planned_per_day": entry["planned_per_day"],
            "used_pct_of_plan": round(
                used / entry["planned_per_day"] * 100, 1
            ),
            "busiest_5_min": max(window_totals.values(), default=0),
            "limit": entry["limit"],
            "within_limit": (
                entry["window_limit"] is None
                or max(window_totals.values(), default=0)
                <= entry["window_limit"]
            ),
        })

    return pd.DataFrame(rows)


def latest_values_identical(values, count=3):
    """True when the last `count` measured values are all the same.

    Missing values (runs that did not measure) are skipped, so a gap is not
    mistaken for a repeat. This is the stale-source warning from the
    September 2026 incident.
    """
    recent = pd.Series(values).dropna().tail(count)
    return len(recent) == count and recent.nunique() == 1
