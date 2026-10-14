"""Database access and formatting shared by the dashboard pages."""

import pandas as pd
import streamlit as st

from database.mysql_database import get_connection


QUERY_CACHE_SECONDS = 300
PRAGUE = "Europe/Prague"

# Planned requests per day and the limits they are measured against
# (docs/request_budget.md).
REQUEST_BUDGET = [
    {
        "budget": "Valve Web API",
        "sources": ("valve_top_concurrent", "valve_current_players",
                    "valve_most_played", "steam_news"),
        "planned_per_day": 123,
        "limit": "no published keyless limit; keep modest",
        "window_limit": None,
    },
    {
        "budget": "SteamSpy request=all",
        "sources": ("steamspy_all",),
        "planned_per_day": 1,
        "limit": "1 per 60 s",
        "window_limit": None,
    },
    {
        "budget": "SteamSpy other requests",
        "sources": ("steamspy_appdetails",),
        "planned_per_day": 123,
        "limit": "1 per s (paused 1.1 s)",
        "window_limit": None,
    },
    {
        "budget": "Steam Store",
        "sources": ("store_appdetails", "store_prices",
                    "steam_review_summary", "steam_review_histogram"),
        "planned_per_day": 146,
        "limit": "150 per 5 min",
        "window_limit": 150,
    },
]


def read_sql(query, params=()):
    connection = get_connection()

    try:
        cursor = connection.cursor()
        # No params means no %-substitution, so LIKE '%x%' stays literal.
        cursor.execute(query, tuple(params) or None)
        columns = [column[0] for column in cursor.description]
        return pd.DataFrame.from_records(
            cursor.fetchall(),
            columns=columns,
            coerce_float=True
        )
    finally:
        connection.close()


@st.cache_data(ttl=QUERY_CACHE_SECONDS, show_spinner=False)
def cached_read_sql(query, params=()):
    return read_sql(query, tuple(params))


def format_number(value):
    if value is None or pd.isna(value):
        return "N/A"

    return f"{int(value):,}"


def format_prague_time(value):
    if value is None or pd.isna(value):
        return "N/A"

    timestamp = pd.Timestamp(value)

    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")

    return timestamp.tz_convert(PRAGUE).strftime("%Y-%m-%d %H:%M %Z")


def to_prague(series):
    """Convert a naive-UTC datetime column for display in Prague time."""
    return (
        pd.to_datetime(series)
        .dt.tz_localize("UTC")
        .dt.tz_convert(PRAGUE)
    )


def cache_note():
    st.caption(
        f"Query results are cached for {QUERY_CACHE_SECONDS // 60} minutes."
        " Times are shown in Prague time unless a chart says otherwise."
    )


# Jobs the scheduler runs today. Older job names (daily_legacy, from before
# the split into jobs) stay in the run history but get no status badge.
CURRENT_JOBS = ("catalog_daily", "players_hourly", "reviews_daily")


def latest_runs():
    """The latest run of every current job, newest first."""
    return cached_read_sql(f"""
        SELECT runs.run_id, runs.job_name, runs.status, runs.rows_loaded,
               runs.started_at, runs.finished_at, runs.warning_message
        FROM pipeline_runs AS runs
        JOIN (
            SELECT job_name, MAX(run_id) AS run_id
            FROM pipeline_runs
            WHERE job_name IN ({", ".join(["%s"] * len(CURRENT_JOBS))})
            GROUP BY job_name
        ) AS latest
            ON latest.run_id = runs.run_id
        ORDER BY runs.run_id DESC
    """, CURRENT_JOBS)


def current_check_results():
    """The latest result of every data check and target.

    Checks run after the job that owns them, so their latest result is their
    current state; counting rows instead would count one problem once per
    run.
    """
    return cached_read_sql("""
        SELECT results.check_name, results.target, results.status,
               results.observed_value, results.threshold, results.details,
               results.run_id, results.checked_at,
               (SELECT COUNT(*) FROM data_quality_results AS earlier
                WHERE earlier.check_name = results.check_name
                  AND earlier.target = results.target
                  AND earlier.status = results.status
                  AND earlier.checked_at
                      >= UTC_TIMESTAMP() - INTERVAL 7 DAY) AS times_7d
        FROM data_quality_results AS results
        JOIN (
            SELECT check_name, target, MAX(result_id) AS result_id
            FROM data_quality_results
            GROUP BY check_name, target
        ) AS latest
            ON latest.result_id = results.result_id
        ORDER BY FIELD(results.status, 'fail', 'warn', 'pass'),
                 results.check_name, results.target
    """)


def show_run_warnings(runs):
    """One compact notice for every job whose latest run has warnings."""
    warned = runs[
        runs["warning_message"].notna() & (runs["warning_message"] != "")
    ]

    if warned.empty:
        return

    jobs = ", ".join(f"`{name}`" for name in warned["job_name"])
    st.warning(
        f"Latest run of {jobs} finished with warnings: some data may be "
        "missing, or a data-quality check failed.",
        icon=":material/warning:",
    )

    with st.expander("Show run warnings"):
        for _, run in warned.iterrows():
            st.markdown(f"**{run['job_name']}** (run {run['run_id']})")
            st.text(run["warning_message"])
