"""Overview: headline figures, latest runs, top movers, most played."""

import pandas as pd
import plotly.express as px
import streamlit as st

from dashboard_common import (
    cache_note,
    cached_read_sql,
    format_number,
    format_prague_time,
    latest_runs,
    show_run_warnings,
)


st.title("Steam Data Pipeline")

runs = latest_runs()

if runs.empty:
    st.info("No pipeline runs are available yet.")
    st.stop()

kpis = cached_read_sql("""
    SELECT
        (SELECT COUNT(*) FROM current_game_metrics) AS games_tracked,
        (SELECT SUM(current_players) FROM player_observations
         WHERE run_id = (SELECT MAX(run_id) FROM player_observations))
            AS players_now,
        (SELECT MAX(COALESCE(source_measured_at, collected_at))
         FROM player_observations
         WHERE run_id = (SELECT MAX(run_id) FROM player_observations))
            AS players_measured_at
""").iloc[0]
freshness = cached_read_sql("""
    SELECT results.target, results.status
    FROM data_quality_results AS results
    JOIN (
        SELECT target, MAX(result_id) AS result_id
        FROM data_quality_results
        WHERE check_name = 'freshness'
        GROUP BY target
    ) AS latest
        ON latest.result_id = results.result_id
""")

games, players, sources, latest = st.columns(4)
games.metric("Games in catalog", format_number(kpis["games_tracked"]))
players.metric("Players now (Valve)", format_number(kpis["players_now"]))
sources.metric(
    "Sources healthy",
    f"{(freshness['status'] == 'pass').sum()} of {len(freshness)}"
    if not freshness.empty else "N/A"
)
latest.metric(
    f"Latest run ({runs.iloc[0]['job_name']})",
    runs.iloc[0]["status"]
)
st.caption(
    "Games: the latest SteamSpy catalog page plus tracked games. Players: "
    "the sum over games in the latest hourly Valve measurement, measured at "
    f"{format_prague_time(kpis['players_measured_at'])}. Sources healthy: "
    "sources whose newest successful response is fresh (see Pipeline health)."
)
st.caption(
    "Latest runs: "
    + "; ".join(
        f"{run['job_name']} {run['status']} at "
        f"{format_prague_time(run['finished_at'])}"
        for _, run in runs.iterrows()
    )
)
show_run_warnings(runs)
cache_note()

st.subheader("Top movers, week over week")

movers = cached_read_sql("""
    SELECT name, appid, peak_players, peak_change_pct_wow, observation_date
    FROM mart_player_trends
    WHERE observation_date = (SELECT MAX(observation_date)
                              FROM mart_player_trends)
      AND peak_change_pct_wow IS NOT NULL
""")

if movers.empty:
    st.info(
        "Week-over-week changes need a daily peak from the same day a week "
        "earlier; they appear after eight days of hourly collection."
    )
else:
    movers = pd.concat([
        movers.nlargest(10, "peak_change_pct_wow"),
        movers.nsmallest(10, "peak_change_pct_wow"),
    ]).drop_duplicates("appid").sort_values("peak_change_pct_wow")
    figure = px.bar(
        movers,
        x="peak_change_pct_wow",
        y="name",
        orientation="h",
        color="peak_change_pct_wow",
        color_continuous_scale="RdBu",
        color_continuous_midpoint=0,
        hover_data=["peak_players", "observation_date"],
        labels={
            "peak_change_pct_wow": "Daily peak vs a week earlier (%)",
            "name": "",
        },
    )
    figure.update_layout(height=600, coloraxis_showscale=False)
    st.plotly_chart(figure, use_container_width=True)
    st.caption(
        f"Valve daily peak players on {movers['observation_date'].max()} "
        "(UTC day) compared with the same day one week earlier, from "
        "mart_player_trends. Ten largest rises and falls."
    )

st.subheader("Most played right now")

current = cached_read_sql("""
    SELECT games.name, observations.current_players,
           observations.player_rank, observations.source,
           COALESCE(observations.source_measured_at,
                    observations.collected_at) AS measured_at
    FROM player_observations AS observations
    JOIN games ON games.appid = observations.appid
    WHERE observations.run_id = (SELECT MAX(run_id) FROM player_observations)
    ORDER BY observations.current_players DESC
    LIMIT 25
""")

if not current.empty:
    figure = px.bar(
        current.sort_values("current_players"),
        x="current_players",
        y="name",
        orientation="h",
        hover_data=["player_rank", "source"],
        labels={"current_players": "Current players", "name": ""},
    )
    figure.update_layout(height=700)
    st.plotly_chart(figure, use_container_width=True)
    st.caption(
        "Valve's concurrent players from the latest hourly run, for catalog "
        "games in the top-100 chart (source time "
        f"{format_prague_time(current['measured_at'].max())}) and tracked "
        "games looked up directly (collection time)."
    )
