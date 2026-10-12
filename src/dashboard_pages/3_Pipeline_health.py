"""Pipeline health: runs, freshness, staleness, quality, request budget."""

import pandas as pd
import plotly.express as px
import streamlit as st

from dashboard_common import (
    REQUEST_BUDGET,
    cache_note,
    cached_read_sql,
    latest_runs,
    show_run_warnings,
    to_prague,
)
from dashboard_data import request_budget_usage


st.title("Pipeline health")
cache_note()
show_run_warnings(latest_runs())

# ---- Runs -------------------------------------------------------------------

st.subheader("Runs in the last 7 days")

runs = cached_read_sql("""
    SELECT run_id, job_name, status, started_at, finished_at,
           TIMESTAMPDIFF(MICROSECOND, started_at, finished_at) / 1e6
               AS duration_seconds,
           rows_extracted, rows_loaded, measurements_loaded, rows_rejected,
           warning_message IS NOT NULL AS has_warnings, error_message
    FROM pipeline_runs
    WHERE started_at >= UTC_TIMESTAMP() - INTERVAL 7 DAY
    ORDER BY run_id DESC
""")

if runs.empty:
    st.info("No runs in the last 7 days.")
else:
    runs["started"] = to_prague(runs["started_at"])
    figure = px.scatter(
        runs,
        x="started",
        y="duration_seconds",
        color="job_name",
        symbol="status",
        hover_data=["run_id", "rows_loaded", "measurements_loaded"],
        labels={"started": "Started (Prague time)",
                "duration_seconds": "Duration (s)"},
    )
    figure.update_layout(height=350)
    st.plotly_chart(figure, use_container_width=True)
    st.caption(
        "Each point is one job run (pipeline_runs): start time and how long "
        "it took, coloured by job; failed runs use a different symbol."
    )
    summary = (
        runs.groupby(["job_name", "status"])
        .agg(runs=("run_id", "count"),
             median_seconds=("duration_seconds", "median"))
        .reset_index()
    )
    st.dataframe(summary, hide_index=True, use_container_width=True)

    with st.expander("Run list"):
        st.dataframe(
            runs.drop(columns=["started"]),
            hide_index=True,
            use_container_width=True
        )

# ---- Freshness --------------------------------------------------------------

st.subheader("Freshness per source")

freshness = cached_read_sql("""
    SELECT results.target AS source, results.status,
           results.observed_value AS age, results.threshold,
           results.checked_at
    FROM data_quality_results AS results
    JOIN (
        SELECT target, MAX(result_id) AS result_id
        FROM data_quality_results
        WHERE check_name = 'freshness'
        GROUP BY target
    ) AS latest
        ON latest.result_id = results.result_id
    ORDER BY results.status DESC, results.target
""")

if freshness.empty:
    st.info("No freshness results yet.")
else:
    freshness["checked_at"] = to_prague(freshness["checked_at"])
    st.dataframe(freshness, hide_index=True, use_container_width=True)
    st.caption(
        "Age of each source's newest successful response at the last check; "
        "it fails beyond 1.5 times the expected interval "
        "(docs/observability.md)."
    )

# ---- Staleness --------------------------------------------------------------

st.subheader("Staleness")

staleness = cached_read_sql("""
    SELECT checked_at, observed_value, status
    FROM data_quality_results
    WHERE check_name = 'steamspy_unchanged_fields'
    ORDER BY checked_at
""")

if staleness.empty:
    st.info(
        "The unchanged-field rate needs two catalog runs to compare."
    )
else:
    staleness["unchanged_pct"] = (
        staleness["observed_value"].str.rstrip("%").astype(float)
    )
    staleness["checked_at"] = to_prague(staleness["checked_at"])
    figure = px.line(
        staleness,
        x="checked_at",
        y="unchanged_pct",
        markers=True,
        labels={"checked_at": "Checked (Prague time)",
                "unchanged_pct": "SteamSpy values unchanged (%)"},
    )
    figure.add_hline(y=95, line_dash="dot", line_color="orange")
    figure.add_hline(y=99, line_dash="dot", line_color="red")
    figure.update_layout(height=300)
    st.plotly_chart(figure, use_container_width=True)
    st.caption(
        "Share of SteamSpy values (owners, reviews, playtimes, ccu, prices) "
        "unchanged between consecutive catalog runs. Dotted lines: warning "
        "(95%) and failure (99%). The September 2026 stale-data incident "
        "would sit at about 100%."
    )

duplicates = cached_read_sql("""
    SELECT results.target AS source, results.observed_value AS rate,
           results.status, results.threshold
    FROM data_quality_results AS results
    JOIN (
        SELECT target, MAX(result_id) AS result_id
        FROM data_quality_results
        WHERE check_name = 'raw_duplicate_rate'
        GROUP BY target
    ) AS latest
        ON latest.result_id = results.result_id
    ORDER BY results.target
""")

if not duplicates.empty:
    st.dataframe(duplicates, hide_index=True, use_container_width=True)
    st.caption(
        "Share of each source's responses in the last 24 hours that "
        "repeated the previous body byte for byte (raw landing layer)."
    )

# ---- Rejected rows ----------------------------------------------------------

st.subheader("Rejected SteamSpy rows")

rejected = cached_read_sql("""
    SELECT run_id, finished_at, rows_extracted, rows_rejected
    FROM pipeline_runs
    WHERE status = 'SUCCESS'
      AND job_name IN ('catalog_daily', 'daily_legacy')
    ORDER BY run_id DESC
    LIMIT 60
""")

if rejected.empty:
    st.info("No catalog runs yet.")
else:
    rejected["finished_at"] = to_prague(rejected["finished_at"])
    figure = px.bar(
        rejected,
        x="finished_at",
        y="rows_rejected",
        hover_data=["run_id", "rows_extracted"],
        labels={"finished_at": "Catalog run (Prague time)",
                "rows_rejected": "Rows rejected"},
    )
    figure.update_layout(height=280)
    st.plotly_chart(figure, use_container_width=True)
    st.caption(
        "SteamSpy rows set aside by row-level validation in each catalog "
        "run; a run fails instead above PIPELINE_MAX_REJECTED_PERCENT."
    )

# ---- Data quality -----------------------------------------------------------

st.subheader("Recent data-quality problems")

problems = cached_read_sql("""
    SELECT checked_at, status, check_name, target, observed_value,
           threshold, details, run_id
    FROM data_quality_results
    WHERE status <> 'pass'
      AND checked_at >= UTC_TIMESTAMP() - INTERVAL 7 DAY
    ORDER BY checked_at DESC, status
    LIMIT 200
""")

if problems.empty:
    st.success("No failed or warning checks in the last 7 days.")
else:
    problems["checked_at"] = to_prague(problems["checked_at"])
    show = st.radio(
        "Show",
        ["fail", "fail and warn"],
        horizontal=True
    )
    shown = problems if show == "fail and warn" else (
        problems[problems["status"] == "fail"]
    )
    st.dataframe(shown, hide_index=True, use_container_width=True)
    st.caption(
        "Checks stored in data_quality_results after every run. A failure "
        "never deletes data; it adds a warning to the run "
        "(docs/observability.md)."
    )

# ---- Request budget ---------------------------------------------------------

st.subheader("Request budget, last 24 hours")

requests = cached_read_sql("""
    SELECT source,
           FLOOR(UNIX_TIMESTAMP(fetched_at) / 300) AS window_5_min,
           COUNT(*) AS requests
    FROM raw_api_responses
    WHERE fetched_at >= UTC_TIMESTAMP() - INTERVAL 24 HOUR
    GROUP BY source, window_5_min
""")
counts = requests.groupby("source")["requests"].sum().to_dict()
windows = {
    source: dict(zip(group["window_5_min"], group["requests"]))
    for source, group in requests.groupby("source")
}
usage = request_budget_usage(counts, windows, REQUEST_BUDGET)
figure = px.bar(
    usage,
    x="used_pct_of_plan",
    y="budget",
    orientation="h",
    hover_data=["requests_last_24h", "planned_per_day", "busiest_5_min",
                "limit"],
    labels={"used_pct_of_plan": "Requests in the last 24 h (% of plan)",
            "budget": ""},
)
figure.add_vline(x=100, line_dash="dot", line_color="gray")
figure.update_layout(height=280)
st.plotly_chart(figure, use_container_width=True)
st.dataframe(usage, hide_index=True, use_container_width=True)
st.caption(
    "Requests recorded in the raw landing layer over the last 24 hours, "
    "against the planned daily volume (docs/request_budget.md). For the "
    "Steam Store, the busiest 5-minute window must stay within 150 requests."
)
