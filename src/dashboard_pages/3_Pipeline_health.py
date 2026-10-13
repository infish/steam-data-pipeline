"""Pipeline health: runs, freshness, data quality, staleness, budget."""

import plotly.graph_objects as go
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
from dashboard_style import (
    card,
    job_color,
    page_header,
    show,
    status_label,
    style,
    tokens,
)


page_header(
    "Pipeline health",
    "Is the pipeline running, is every source fresh, and do the data checks "
    "pass?"
)

runs_now = latest_runs()
show_run_warnings(runs_now)

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
    ORDER BY FIELD(results.status, 'fail', 'warn', 'pass'), results.target
""")
problems = cached_read_sql("""
    SELECT checked_at, status, check_name, target, observed_value,
           threshold, details, run_id
    FROM data_quality_results
    WHERE status <> 'pass'
      AND checked_at >= UTC_TIMESTAMP() - INTERVAL 7 DAY
    ORDER BY checked_at DESC, FIELD(status, 'fail', 'warn')
    LIMIT 300
""")
requests = cached_read_sql("""
    SELECT source,
           FLOOR(UNIX_TIMESTAMP(fetched_at) / 300) AS window_5_min,
           COUNT(*) AS requests
    FROM raw_api_responses
    WHERE fetched_at >= UTC_TIMESTAMP() - INTERVAL 24 HOUR
    GROUP BY source, window_5_min
""")
usage = request_budget_usage(
    requests.groupby("source")["requests"].sum().to_dict(),
    {
        source: dict(zip(group["window_5_min"], group["requests"]))
        for source, group in requests.groupby("source")
    },
    REQUEST_BUDGET,
)

no_warnings = (
    runs_now["warning_message"].isna() | (runs_now["warning_message"] == "")
)
healthy_jobs = (
    ((runs_now["status"] == "SUCCESS") & no_warnings).sum()
    if not runs_now.empty else 0
)
fails_24h = cached_read_sql("""
    SELECT COUNT(*) AS failed
    FROM data_quality_results
    WHERE status = 'fail'
      AND checked_at >= UTC_TIMESTAMP() - INTERVAL 24 HOUR
""").iloc[0]["failed"]
store_peak = int(
    usage.loc[usage["budget"] == "Steam Store", "busiest_5_min"].iloc[0]
)

jobs, fresh, checks, budget = st.columns(4)
jobs.metric(
    "Jobs healthy",
    f"{healthy_jobs} / {len(runs_now)}",
    border=True,
    help="Jobs whose latest run succeeded without warnings.",
)
fresh.metric(
    "Sources fresh",
    f"{(freshness['status'] == 'pass').sum()} / {len(freshness)}"
    if not freshness.empty else "N/A",
    border=True,
    help="Newest successful response within 1.5 times the expected "
         "interval.",
)
checks.metric(
    "Failed checks",
    f"{int(fails_24h or 0)}",
    border=True,
    help="Data-quality checks with status fail in the last 24 hours.",
)
budget.metric(
    "Store peak, 5 min",
    f"{store_peak} / 150",
    border=True,
    help="Steam Store requests in the busiest 5 minutes of the last 24 "
         "hours; the Store allows about 150 per 5 minutes.",
)

runs_tab, fresh_tab, quality_tab, stale_tab, budget_tab = st.tabs([
    ":material/play_circle: Runs",
    ":material/schedule: Freshness",
    ":material/fact_check: Data quality",
    ":material/hourglass_empty: Staleness",
    ":material/speed: Request budget",
])

with runs_tab:
    runs = cached_read_sql("""
        SELECT run_id, job_name, status, started_at, finished_at,
               TIMESTAMPDIFF(MICROSECOND, started_at, finished_at) / 1e6
                   AS duration_seconds,
               rows_extracted, rows_loaded, measurements_loaded,
               rows_rejected, warning_message, error_message
        FROM pipeline_runs
        WHERE started_at >= UTC_TIMESTAMP() - INTERVAL 7 DAY
        ORDER BY run_id DESC
    """)

    if runs.empty:
        st.info("No runs in the last 7 days.")
    else:
        runs["started"] = to_prague(runs["started_at"])

        with card(
            "Run durations, last 7 days",
            "Each point is one run, colored by job; failed runs are "
            "crosses."
        ):
            figure = go.Figure()

            for job_name, group in runs.groupby("job_name"):
                figure.add_trace(go.Scatter(
                    x=group["started"],
                    y=group["duration_seconds"],
                    name=job_name,
                    mode="markers",
                    marker={
                        "color": job_color(job_name),
                        "symbol": [
                            "x" if status == "FAILED" else "circle"
                            for status in group["status"]
                        ],
                    },
                    customdata=group[["run_id", "status"]],
                    hovertemplate=(
                        "run %{customdata[0]} · %{customdata[1]}"
                        "<br>%{x|%Y-%m-%d %H:%M} · %{y:.1f} s"
                        f"<extra>{job_name}</extra>"
                    ),
                ))

            style(figure, height=320, y_title="Duration (s)")
            figure.update_xaxes(tickformat="%b %-d<br>%H:%M",
                                hoverformat="%Y-%m-%d %H:%M")
            show(figure)

        with card("Run list"):
            shown = runs.drop(columns=["started_at", "finished_at"])
            shown["status"] = shown["status"].map({
                "SUCCESS": "✅ success",
                "FAILED": "❌ failed",
                "RUNNING": "⏳ running",
            }).fillna(shown["status"])
            shown[["warning_message", "error_message"]] = (
                shown[["warning_message", "error_message"]].fillna("")
            )
            st.dataframe(
                shown[[
                    "run_id", "job_name", "status", "started",
                    "duration_seconds", "rows_loaded", "measurements_loaded",
                    "rows_rejected", "warning_message", "error_message",
                ]],
                hide_index=True,
                use_container_width=True,
                column_config={
                    "run_id": "Run",
                    "job_name": "Job",
                    "status": "Status",
                    "started": st.column_config.DatetimeColumn(
                        "Started", format="YYYY-MM-DD HH:mm"),
                    "duration_seconds": st.column_config.NumberColumn(
                        "Seconds", format="%.1f"),
                    "rows_loaded": "Rows",
                    "measurements_loaded": "Observations",
                    "rows_rejected": "Rejected",
                    "warning_message": "Warnings",
                    "error_message": "Error",
                },
            )

with fresh_tab:
    with card(
        "Freshness per source",
        "Age of each source's newest successful response at the last "
        "check; it fails beyond 1.5 times the expected interval."
    ):
        if freshness.empty:
            st.info("No freshness results yet.")
        else:
            shown = freshness.copy()
            shown["status"] = shown["status"].map(status_label)
            shown["checked_at"] = to_prague(shown["checked_at"])
            st.dataframe(
                shown,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "source": "Source",
                    "status": "Status",
                    "age": "Age",
                    "threshold": "Expected",
                    "checked_at": st.column_config.DatetimeColumn(
                        "Checked", format="YYYY-MM-DD HH:mm"),
                },
            )

with quality_tab:
    with card(
        "Checks that did not pass, last 7 days",
        "Stored after every run. A failure never deletes data; it adds a "
        "warning to the run."
    ):
        if problems.empty:
            st.success("Every check passed in the last 7 days.",
                       icon=":material/check_circle:")
        else:
            show_warnings = st.toggle("Include warnings", value=False)
            shown = problems if show_warnings else (
                problems[problems["status"] == "fail"]
            )
            shown = shown.copy()
            shown["status"] = shown["status"].map(status_label)
            shown["checked_at"] = to_prague(shown["checked_at"])
            st.dataframe(
                shown,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "checked_at": st.column_config.DatetimeColumn(
                        "Checked", format="YYYY-MM-DD HH:mm"),
                    "status": "Status",
                    "check_name": "Check",
                    "target": "Target",
                    "observed_value": "Observed",
                    "threshold": "Expected",
                    "details": "Details",
                    "run_id": "Run",
                },
            )

with stale_tab:
    staleness = cached_read_sql("""
        SELECT checked_at, observed_value
        FROM data_quality_results
        WHERE check_name = 'steamspy_unchanged_fields'
        ORDER BY checked_at
    """)

    with card(
        "SteamSpy values unchanged between catalog runs",
        "The September 2026 stale-data incident would sit near 100%. "
        "Lines mark the warning (95%) and failure (99%) levels."
    ):
        if staleness.empty:
            st.info("This needs two catalog runs to compare.")
        else:
            t = tokens()
            staleness["unchanged"] = (
                staleness["observed_value"].str.rstrip("%").astype(float)
            )
            staleness["checked_at"] = to_prague(staleness["checked_at"])
            figure = go.Figure(go.Scatter(
                x=staleness["checked_at"],
                y=staleness["unchanged"],
                mode="lines+markers",
                line={"color": t.series[0]},
                hovertemplate="%{x|%Y-%m-%d}: %{y:.1f}% unchanged"
                              "<extra></extra>",
            ))

            for level, label in ((95, "warn"), (99, "fail")):
                figure.add_hline(
                    y=level,
                    line_width=1,
                    line_color=t.muted,
                    annotation_text=label,
                    annotation_font_color=t.muted,
                )

            style(figure, height=300, legend=False,
                  y_title="Values unchanged (%)")
            figure.update_yaxes(range=[0, 101], ticksuffix="%")
            show(figure)

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
        with card(
            "Repeated responses, last 24 hours",
            "Share of each source's responses that repeated the previous "
            "body byte for byte."
        ):
            duplicates["status"] = duplicates["status"].map(status_label)
            st.dataframe(
                duplicates,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "source": "Source",
                    "rate": "Repeated",
                    "status": "Status",
                    "threshold": "Expected",
                },
            )

with budget_tab:
    with card(
        "Requests in the last 24 hours",
        "Against the planned daily volume per API (docs/request_budget.md)."
    ):
        t = tokens()
        figure = go.Figure(go.Bar(
            x=usage["used_pct_of_plan"],
            y=usage["budget"],
            orientation="h",
            marker={"color": t.series[0]},
            customdata=usage[["requests_last_24h", "planned_per_day"]],
            hovertemplate=(
                "<b>%{y}</b><br>%{customdata[0]} of %{customdata[1]} "
                "planned (%{x:.0f}%)<extra></extra>"
            ),
        ))
        figure.add_vline(x=100, line_width=1, line_color=t.muted,
                         annotation_text="plan",
                         annotation_font_color=t.muted)
        style(figure, height=240, legend=False,
              x_title="Requests in the last 24 h (% of plan)")
        figure.update_xaxes(showgrid=True, ticksuffix="%")
        show(figure)

        shown = usage.copy()
        shown["within_limit"] = shown["within_limit"].map(
            {True: "✅ yes", False: "❌ no"}
        )
        st.dataframe(
            shown,
            hide_index=True,
            use_container_width=True,
            column_config={
                "budget": "API",
                "requests_last_24h": "Requests (24 h)",
                "planned_per_day": "Planned per day",
                "used_pct_of_plan": st.column_config.NumberColumn(
                    "% of plan", format="%.0f%%"),
                "busiest_5_min": "Busiest 5 min",
                "limit": "Limit",
                "within_limit": "Within limit",
            },
        )

cache_note()
