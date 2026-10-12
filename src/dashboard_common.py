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


def latest_runs():
    """The latest run of every job, newest first."""
    return cached_read_sql("""
        SELECT runs.run_id, runs.job_name, runs.status, runs.rows_loaded,
               runs.started_at, runs.finished_at, runs.warning_message
        FROM pipeline_runs AS runs
        JOIN (
            SELECT job_name, MAX(run_id) AS run_id
            FROM pipeline_runs
            GROUP BY job_name
        ) AS latest
            ON latest.run_id = runs.run_id
        ORDER BY runs.run_id DESC
    """)


def show_run_warnings(runs):
    for _, run in runs.iterrows():
        if pd.notna(run["warning_message"]) and run["warning_message"]:
            st.warning(
                f"The latest {run['job_name']} run completed with warnings."
                " Some data may be missing from it, or a data-quality check"
                " failed."
            )

            with st.expander(f"{run['job_name']} run warnings"):
                st.text(run["warning_message"])
