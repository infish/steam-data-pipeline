"""Pipeline: runs, steps, freshness, data quality, staleness, budget.
"""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from dashboard_common import (
    CURRENT_JOBS,
    REQUEST_BUDGET,
    cache_note,
    cached_read_sql,
    current_check_results,
    latest_runs,
    show_run_warnings,
    to_prague,
)
from dashboard_data import request_budget_usage
from dashboard_style import (
    JOB_SYMBOLS,
    card,
    job_color,
    line,
    page_header,
    show,
    status_label,
    style,
    tokens,
)


page_header(
    "Pipeline",
    "Is the pipeline running, is every source fresh, and do the data checks "
    "pass?"
)

runs_now = latest_runs()
show_run_warnings(runs_now)

current_checks = current_check_results()
freshness = current_checks[current_checks["check_name"] == "freshness"][
    ["target", "status", "observed_value", "threshold", "checked_at"]
].rename(columns={"target": "source", "observed_value": "age"})
problems = current_checks[current_checks["status"] != "pass"]
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
failing_now = int((current_checks["status"] == "fail").sum())
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
    "Checks failing",
    f"{failing_now} / {len(current_checks)}",
    border=True,
    help="Data-quality checks whose latest result is fail. Each check runs "
         "after the job that produces its data; freshness runs after every "
         "job.",
)
budget.metric(
    "Store peak, 5 min",
    f"{store_peak} / 150",
    border=True,
    help="Steam Store requests (details, prices, histograms) in the "
         "busiest 5 minutes of the last 24 hours; the Store allows about "
         "150 per 5 minutes. Review summaries come from the Web API, with "
         "their own limit (Request budget tab).",
)

(
    runs_tab, steps_tab, fresh_tab, quality_tab, stale_tab, budget_tab
) = st.tabs([
    ":material/play_circle: Runs",
    ":material/timer: Steps",
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
            "Each point is one run, colored by job (the hourly refresh is "
            "a diamond in the catalog's color); failed runs are crosses."
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
                            "x" if status == "FAILED"
                            else JOB_SYMBOLS.get(job_name, "circle")
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

with steps_tab:
    job_name = st.segmented_control(
        "Job",
        CURRENT_JOBS,
        default=CURRENT_JOBS[0],
        label_visibility="collapsed",
    ) or CURRENT_JOBS[0]
    steps = cached_read_sql("""
        SELECT steps.run_id, steps.position, steps.step, steps.parent_step,
               steps.status, steps.started_at, steps.duration_ms,
               steps.wait_ms, steps.requests, steps.rows_affected
        FROM run_steps AS steps
        WHERE steps.run_id = (
            SELECT MAX(timed.run_id)
            FROM run_steps AS timed
            JOIN pipeline_runs AS runs ON runs.run_id = timed.run_id
            WHERE runs.job_name = %s
        )
        ORDER BY steps.position
    """, (job_name,))

    if steps.empty:
        st.info(
            f"No step timings for {job_name} yet. They are stored from the "
            "first run after upgrading to v1.1.0."
        )
    else:
        phases = steps[steps["parent_step"].isna()]
        total_s = phases["duration_ms"].sum() / 1000
        waited_s = steps.loc[
            steps["parent_step"] == "job", "wait_ms"
        ].sum() / 1000
        # The chart shows what ran: the job's steps, then the other phases.
        shown_steps = steps[
            (steps["parent_step"] == "job")
            | (steps["parent_step"].isna() & (steps["step"] != "job"))
        ].copy()
        shown_steps["seconds"] = shown_steps["duration_ms"] / 1000
        shown_steps["waiting"] = shown_steps["wait_ms"] / 1000
        shown_steps["working"] = (
            shown_steps["seconds"] - shown_steps["waiting"]
        )
        slowest = shown_steps.loc[shown_steps["seconds"].idxmax()]
        run_id = int(steps["run_id"].iloc[0])
        started = to_prague(steps["started_at"]).iloc[0]

        total, waiting, calls, slow = st.columns(4)
        total.metric(
            "Run time", f"{total_s:,.1f} s", border=True,
            help="Sum of the run's phases: setup, job, finish, marts and "
                 "checks.",
        )
        waiting.metric(
            "Waiting",
            f"{waited_s:,.1f} s",
            delta=f"{waited_s / total_s:.0%} of the run" if total_s else None,
            delta_color="off",
            border=True,
            help="Time spent in deliberate pauses between API requests, to "
                 "stay within rate limits (docs/request_budget.md).",
        )
        requests_made = int(
            steps.loc[steps["step"] == "job", "requests"].sum()
        )
        calls.metric(
            "API requests", f"{requests_made:,}", border=True,
            help="API responses recorded during the job's steps.",
        )
        slow.metric(
            "Slowest step",
            f"{slowest['seconds']:,.0f} s",
            delta=slowest["step"],
            delta_color="off",
            border=True,
            help=f"{slowest['step']}: {slowest['seconds']:,.1f} s, of which "
                 f"{slowest['waiting']:,.1f} s waiting.",
        )

        with card(
            f"Where the time went: run {run_id}, "
            f"{started:%Y-%m-%d %H:%M}",
            "The job's steps in the order they ran, then finishing, marts "
            "and checks. Blue is time spent waiting between API requests "
            "on purpose; orange is everything else."
        ):
            t = tokens()
            figure = go.Figure()

            for index, (column, name) in enumerate((
                ("working", "Working"),
                ("waiting", "Waiting for rate limits"),
            )):
                figure.add_trace(go.Bar(
                    x=shown_steps[column],
                    y=shown_steps["step"],
                    name=name,
                    orientation="h",
                    marker={
                        "color": t.series[index],
                        "line": {"color": t.surface, "width": 2},
                    },
                    customdata=shown_steps[
                        ["seconds", "requests", "rows_affected"]
                    ],
                    hovertemplate=(
                        "<b>%{y}</b><br>" + name.lower() + " %{x:,.1f} s"
                        " of %{customdata[0]:,.1f} s"
                        "<br>%{customdata[1]} requests<extra></extra>"
                    ),
                ))

            style(figure, height=max(240, 30 * len(shown_steps) + 60),
                  x_title="Seconds")
            figure.update_layout(barmode="stack", legend_traceorder="normal")
            figure.update_traces(
                marker_line={"color": t.surface, "width": 2},
                selector={"type": "bar"},
            )
            figure.update_xaxes(showgrid=True)
            figure.update_yaxes(autorange="reversed", showgrid=False)
            show(figure)

        history = cached_read_sql("""
            SELECT runs.run_id, runs.started_at,
                   SUM(CASE WHEN steps.step IN ('setup', 'job', 'finish')
                            THEN steps.duration_ms END) / 1000 AS collect_s,
                   SUM(CASE WHEN steps.step = 'marts'
                            THEN steps.duration_ms END) / 1000 AS marts_s,
                   SUM(CASE WHEN steps.step = 'checks'
                            THEN steps.duration_ms END) / 1000 AS checks_s
            FROM run_steps AS steps
            JOIN pipeline_runs AS runs ON runs.run_id = steps.run_id
            WHERE steps.parent_step IS NULL
              AND runs.job_name = %s
              AND runs.started_at >= UTC_TIMESTAMP() - INTERVAL 14 DAY
            GROUP BY runs.run_id, runs.started_at
            ORDER BY runs.started_at
        """, (job_name,))

        with card(
            "Run time by phase, last 14 days",
            "Collect and load includes setup and finishing. A rising line "
            "points at the phase to look at."
        ):
            history["started"] = to_prague(history["started_at"])
            show(line(
                history,
                "started",
                {
                    "collect_s": "Collect and load",
                    "marts_s": "Marts",
                    "checks_s": "Checks",
                },
                height=300,
                y_title="Seconds",
                y_format=",.1~f",
            ))

        with card(
            "Every step of the latest run",
            "Indented steps ran inside the step above them."
        ):
            table = steps.copy()
            table["step"] = [
                ("    " if parent else "") + step
                for step, parent in zip(table["step"], table["parent_step"])
            ]
            table["seconds"] = table["duration_ms"] / 1000
            table["waiting"] = table["wait_ms"] / 1000
            table["share"] = (
                table["seconds"] / total_s * 100 if total_s else 0
            )
            table["status"] = table["status"].map(
                {"ok": "✅ ok", "failed": "❌ failed"}
            ).fillna(table["status"])
            # Only some steps report rows; leave the others blank.
            table["rows_affected"] = [
                "" if pd.isna(value) else f"{int(value):,}"
                for value in table["rows_affected"]
            ]
            st.dataframe(
                table[[
                    "step", "seconds", "share", "waiting", "requests",
                    "rows_affected", "status",
                ]],
                hide_index=True,
                use_container_width=True,
                column_config={
                    "step": "Step",
                    "seconds": st.column_config.NumberColumn(
                        "Seconds", format="%.2f"),
                    "share": st.column_config.ProgressColumn(
                        "Share of run", format="%.0f%%",
                        min_value=0, max_value=100),
                    "waiting": st.column_config.NumberColumn(
                        "Waiting (s)", format="%.2f"),
                    "requests": "Requests",
                    "rows_affected": "Rows",
                    "status": "Status",
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
        "Checks not passing now",
        "Each check's latest result. Checks run after the job that produces "
        "their data, and a failure adds a warning to that job's run; it "
        "never deletes data. \"Times, 7 d\" counts results with this status "
        "in the last 7 days."
    ):
        if problems.empty:
            st.success("Every check passes.", icon=":material/check_circle:")
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
                    "times_7d": "Times, 7 d",
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
        "SteamSpy has served a frozen snapshot since at least September "
        "2026. At 95% or more (the line) it is reported as frozen; a drop "
        "below means SteamSpy is updating again."
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

            figure.add_hline(
                y=95,
                line_width=1,
                line_color=t.muted,
                annotation_text="frozen",
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
