"""Streamlit entry point: page configuration, navigation and data status.

The pages themselves are in src/dashboard_pages/ (not pages/, which
Streamlit would also pick up on its own).
"""

import pandas as pd
import streamlit as st

from dashboard_common import cached_read_sql, latest_runs
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
        st.Page("dashboard_pages/3_Pipeline_health.py",
                title="Pipeline health", icon=":material/monitor_heart:"),
        st.Page("dashboard_pages/4_SQL_explorer.py", title="SQL explorer",
                icon=":material/database:"),
    ],
})

with st.sidebar:
    st.markdown("### :material/sports_esports: Steam Data Pipeline")
    st.caption("SteamSpy catalog, Valve players, Steam Store and reviews.")
    st.markdown("**Data status**")

    runs = latest_runs()

    if runs.empty:
        st.caption("No runs yet.")
    else:
        for _, run in runs.sort_values("job_name").iterrows():
            finished = run["finished_at"]
            badge = run_badge(
                run["status"],
                run["warning_message"],
                None if pd.isna(finished) else finished,
                run["job_name"],
            )
            when = time_ago(
                run["started_at"] if pd.isna(finished) else finished
            )
            st.markdown(
                f"{badge} `{run['job_name']}`  \n"
                f"<span style='font-size:0.8rem;opacity:0.7'>"
                f"last run {when}</span>",
                unsafe_allow_html=True,
            )

    failing = cached_read_sql("""
        SELECT COUNT(*) AS failing
        FROM data_quality_results
        WHERE status = 'fail'
          AND run_id = (SELECT MAX(run_id) FROM data_quality_results)
    """)

    if not failing.empty and int(failing.iloc[0]["failing"] or 0):
        st.page_link(
            "dashboard_pages/3_Pipeline_health.py",
            label=f"{int(failing.iloc[0]['failing'])} failing data check(s)",
            icon=":material/error:",
        )

navigation.run()
