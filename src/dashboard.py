"""Streamlit entry point: page configuration and navigation.

The pages themselves are in src/dashboard_pages/ (not pages/, which
Streamlit would also pick up on its own).
"""

import streamlit as st


st.set_page_config(page_title="Steam Data Pipeline", layout="wide")

navigation = st.navigation([
    st.Page("dashboard_pages/0_Overview.py", title="Overview", default=True),
    st.Page("dashboard_pages/1_Game_detail.py", title="Game detail"),
    st.Page("dashboard_pages/2_Market_explorer.py", title="Market explorer"),
    st.Page("dashboard_pages/3_Pipeline_health.py", title="Pipeline health"),
    st.Page("dashboard_pages/4_SQL_explorer.py", title="SQL explorer"),
])
navigation.run()
