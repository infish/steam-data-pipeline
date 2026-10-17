"""Overview: what players are doing now, what moved, and where to go next."""

import plotly.graph_objects as go
import streamlit as st

from dashboard_common import (
    cache_note,
    cached_read_sql,
    format_number,
    format_prague_time,
    latest_runs,
    show_run_warnings,
)
from dashboard_style import (
    card,
    compact,
    hbar,
    page_header,
    show,
    style,
    tokens,
)


page_header(
    "Overview",
    "Who is playing what on Steam right now, what changed this week, and "
    "how fresh the data is."
)

runs = latest_runs()

if runs.empty:
    st.info(
        "No pipeline runs yet. The first runs fill this page; to try it "
        "without the APIs, seed from the recorded responses with "
        "`python src/reprocess.py --seed-fixtures`."
    )
    st.stop()

show_run_warnings(runs)

kpis = cached_read_sql("""
    WITH player_runs AS (
        SELECT DISTINCT run_id FROM player_observations
        ORDER BY run_id DESC LIMIT 2
    )
    SELECT
        (SELECT COUNT(*) FROM current_game_metrics) AS games_tracked,
        (SELECT COUNT(*) FROM game_details) AS games_with_details,
        (SELECT SUM(current_players) FROM player_observations
         WHERE run_id = (SELECT MAX(run_id) FROM player_runs))
            AS players_now,
        (SELECT SUM(current_players) FROM player_observations
         WHERE run_id = (SELECT MIN(run_id) FROM player_runs)
           AND (SELECT COUNT(*) FROM player_runs) = 2)
            AS players_before,
        (SELECT MAX(COALESCE(source_measured_at, collected_at))
         FROM player_observations
         WHERE run_id = (SELECT MAX(run_id) FROM player_runs))
            AS players_measured_at
""").iloc[0]
freshness = cached_read_sql("""
    SELECT results.status
    FROM data_quality_results AS results
    JOIN (
        SELECT target, MAX(result_id) AS result_id
        FROM data_quality_results
        WHERE check_name = 'freshness'
        GROUP BY target
    ) AS latest
        ON latest.result_id = results.result_id
""")

players, games, details, sources = st.columns(4)
delta = None

if kpis["players_now"] and kpis["players_before"]:
    change = int(kpis["players_now"] - kpis["players_before"])
    delta = f"{change:+,} vs previous hour"

players.metric(
    "Players right now",
    compact(kpis["players_now"]),
    delta=delta,
    border=True,
    help=(
        f"{format_number(kpis['players_now'])} players: the sum of Valve's "
        "current-player counts for the catalog games in the latest hourly "
        "run, measured at "
        f"{format_prague_time(kpis['players_measured_at'])}."
    ),
)
games.metric(
    "Games in catalog",
    format_number(kpis["games_tracked"]),
    border=True,
    help="The latest SteamSpy catalog page plus tracked games. While "
         "SteamSpy is frozen this is a fixed list of games.",
)
details.metric(
    "With Store details",
    format_number(kpis["games_with_details"]),
    border=True,
    help="About 120 games' Steam Store details are refreshed per day.",
)
sources.metric(
    "Sources fresh",
    f"{(freshness['status'] == 'pass').sum()} / {len(freshness)}"
    if not freshness.empty else "N/A",
    border=True,
    help=(
        "Sources whose newest successful response is within 1.5 times the "
        "expected interval. Details on Pipeline health."
    ),
)

left, right = st.columns(2, gap="medium")

with left:
    with card(
        "Most played right now",
        "Valve concurrent players in the latest hourly run (top 15)."
    ):
        current = cached_read_sql("""
            SELECT games.name, observations.current_players,
                   observations.player_rank
            FROM player_observations AS observations
            JOIN games ON games.appid = observations.appid
            WHERE observations.run_id = (
                SELECT MAX(run_id) FROM player_observations
            )
            ORDER BY observations.current_players DESC
            LIMIT 15
        """)

        if current.empty:
            st.info("No player data yet.")
        else:
            show(hbar(
                current,
                "current_players",
                "name",
                x_title="Current players",
                hover_columns=("player_rank",),
            ))

with right:
    with card(
        "Biggest moves this week",
        "Daily peak players compared with the same day a week earlier."
    ):
        movers = cached_read_sql("""
            SELECT name, peak_players, peak_change_pct_wow, observation_date
            FROM mart_player_trends
            WHERE observation_date = (
                SELECT MAX(observation_date) FROM mart_player_trends
            )
              AND peak_change_pct_wow IS NOT NULL
        """)

        if movers.empty:
            st.info(
                "Week-over-week changes appear after eight days of hourly "
                "collection."
            )
        else:
            t = tokens()
            movers = (
                movers.reindex(
                    movers["peak_change_pct_wow"].abs()
                    .sort_values(ascending=False).index
                )
                .head(15)
                .sort_values("peak_change_pct_wow")
            )
            figure = go.Figure(go.Bar(
                x=movers["peak_change_pct_wow"],
                y=movers["name"],
                orientation="h",
                marker={"color": [
                    t.rise if value >= 0 else t.fall
                    for value in movers["peak_change_pct_wow"]
                ]},
                customdata=movers[["peak_players"]],
                hovertemplate=(
                    "<b>%{y}</b><br>%{x:+.1f}% vs a week earlier"
                    "<br>daily peak %{customdata[0]:,.0f}<extra></extra>"
                ),
            ))
            style(figure, height=max(220, 30 * len(movers) + 40),
                  legend=False, x_title="Change in daily peak (%)")
            figure.update_xaxes(showgrid=True, zeroline=True,
                                zerolinecolor=t.axis, ticksuffix="%")
            show(figure)
            st.caption(
                f"UTC day {movers['observation_date'].max()}. Blue: rose; "
                "red: fell."
            )

st.markdown("#### Explore further")
links = st.columns(4)

with links[0]:
    st.page_link("dashboard_pages/1_Game_detail.py",
                 label="Game detail", icon=":material/sports_esports:")
    st.caption("One game's players, reviews, price and updates.")

with links[1]:
    st.page_link("dashboard_pages/2_Market_explorer.py",
                 label="Market explorer", icon=":material/category:")
    st.caption("Genres, tags, release years, free versus paid.")

with links[2]:
    st.page_link("dashboard_pages/3_Pipeline_health.py",
                 label="Pipeline health", icon=":material/monitor_heart:")
    st.caption("Runs, freshness, data-quality checks, request budget.")

with links[3]:
    st.page_link("dashboard_pages/4_SQL_explorer.py",
                 label="SQL explorer", icon=":material/database:")
    st.caption("Browse tables and marts, run example queries.")

cache_note()
