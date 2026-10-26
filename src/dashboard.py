"""Streamlit entry point: page configuration, navigation and data status.

The pages themselves are in src/dashboard_pages/ (not pages/, which
Streamlit would also pick up on its own).
"""

from datetime import datetime, timezone

import pandas as pd
import streamlit as st

from dashboard_common import (
    DashboardBusy,
    current_check_results,
)
from dashboard_motion import (
    CHECK_SECONDS,
    dot,
    inject_motion_css,
    is_interrupted,
    job_bubbles,
    latest_runs_now,
    running_jobs,
)
from dashboard_style import run_badge, time_ago


st.set_page_config(
    page_title="Steam Data Pipeline",
    page_icon=":material/sports_esports:",
    layout="wide",
)

navigation = st.navigation({
    "Explore": [
        st.Page("dashboard_pages/0_Overview.py", title="Overview",
                icon=":material/dashboard:", default=True),
        st.Page("dashboard_pages/1_Game_detail.py", title="Game detail",
                icon=":material/sports_esports:"),
        st.Page("dashboard_pages/2_Market_explorer.py",
                title="Market explorer", icon=":material/category:"),
    ],
    "Operate": [
        st.Page("dashboard_pages/3_Pipeline_health.py", title="Pipeline",
                icon=":material/monitor_heart:"),
        st.Page("dashboard_pages/4_SQL_explorer.py", title="Explorer",
                icon=":material/database:"),
    ],
})

BUSY_NOTE = (
    "Many people are looking at the dashboard right now; try again in a "
    "minute."
)


@st.fragment(run_every=CHECK_SECONDS)
def data_status():
    """The sidebar's latest run per job and failing checks. It refreshes
    with the job bubbles, so a running job's dot breathes while it runs.
    A fragment re-runs on its own, so it handles a busy database itself."""
    st.markdown("**Data status**")

    try:
        show_data_status()
    except DashboardBusy:
        st.caption(BUSY_NOTE)


def show_data_status():
    runs = latest_runs_now()
    running = running_jobs()

    if runs.empty:
        st.caption("No runs yet.")
    else:
        now = datetime.now(timezone.utc).replace(tzinfo=None)

        for _, run in runs.sort_values("job_name").iterrows():
            finished = run["finished_at"]
            badge = run_badge(
                run["status"],
                run["warning_message"],
                None if pd.isna(finished) else finished,
                run["job_name"],
            )

            if is_interrupted(run["job_name"], run["status"],
                              run["started_at"], now):
                # Still marked RUNNING, but past any possible run time: the
                # process ended (an app restart); the next run tidies it.
                badge = ":orange-badge[:material/pause_circle: interrupted]"
            when = time_ago(
                run["started_at"] if pd.isna(finished) else finished
            )
            st.markdown(
                f"{dot(run['job_name'], live=run['job_name'] in running)}"
                f"{badge} `{run['job_name']}`  \n"
                f"<span style='font-size:0.8rem;opacity:0.7'>"
                f"last run {when}</span>",
                unsafe_allow_html=True,
            )

    checks = current_check_results()
    failing = 0 if checks.empty else int((checks["status"] == "fail").sum())

    if failing:
        st.page_link(
            "dashboard_pages/3_Pipeline_health.py",
            label=f"{failing} failing data check(s)",
            icon=":material/error:",
        )


inject_motion_css()

with st.sidebar:
    st.markdown("### :orange[λ] Steam Data Pipeline")
    st.caption("SteamSpy catalog, Valve players, Steam Store and reviews.")

    data_status()

try:
    navigation.run()
except DashboardBusy:
    st.info(BUSY_NOTE, icon=":material/hourglass_top:")

# The job dock checks every few seconds; its bubbles float at the top right.
# It is mounted in the page, not the sidebar: a collapsed sidebar (phones,
# narrow windows) renders nothing, so the dock would vanish with it.
job_bubbles()
