import plotly.express as px
import streamlit as st
import pandas as pd

from dashboard_data import build_measurement_series
from database.mysql_database import get_connection


st.set_page_config(
    page_title="Steam Data Pipeline Dashboard",
    layout="wide"
)


VIEW_OPTIONS = {
    "Top games by estimated owners": {
        "query": """
            SELECT
                name,
                developer,
                publisher,
                estimated_owners
            FROM top_games_by_owners
            ORDER BY estimated_owners DESC
            LIMIT 25
        """,
        "x": "estimated_owners",
        "y": "name",
        "title": "Top Games By Estimated Owners"
    },
    "Most active games by Valve current players": {
        "query": """
            SELECT
                games.name,
                measurements.current_players,
                measurements.player_rank,
                measurements.collected_at,
                measurements.source_measured_at
            FROM steam_game_measurements AS measurements
            JOIN games ON games.appid = measurements.appid
            WHERE measurements.run_id = (
                SELECT MAX(run_id) FROM steam_game_measurements
            )
            ORDER BY measurements.current_players DESC
            LIMIT 25
        """,
        "x": "current_players",
        "y": "name",
        "title": "Most Active Games By Valve Current Players"
    },
    "Top publishers by estimated owners": {
        "query": """
            SELECT
                publisher,
                game_count,
                avg_estimated_owners,
                total_estimated_owners
            FROM publisher_summary
            ORDER BY total_estimated_owners DESC
            LIMIT 25
        """,
        "x": "total_estimated_owners",
        "y": "publisher",
        "title": "Top Publishers By Estimated Owners"
    }
}


EXPLORER_OBJECTS = {
    "daily_pipeline_summary",
    "flyway_schema_history",
    "games",
    "latest_successful_run",
    "pipeline_runs",
    "steam_game_measurements"
}


QUERY_CACHE_SECONDS = 300


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


def get_database_objects():
    objects = cached_read_sql("""
        SHOW FULL TABLES
    """)
    object_name_column = objects.columns[0]
    return objects[
        objects[object_name_column].isin(EXPLORER_OBJECTS)
    ].reset_index(drop=True)


def get_columns(object_name):
    return cached_read_sql(f"""
        SHOW COLUMNS FROM `{object_name}`
    """)

def format_number(value):
    if pd.isna(value):
        return "N/A"

    return f"{int(value):,}"

def format_prague_time(value):
    if pd.isna(value):
        return "N/A"

    timestamp = pd.Timestamp(value)

    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")

    return timestamp.tz_convert("Europe/Prague").strftime(
        "%Y-%m-%d %H:%M %Z"
    )


def get_kpis():
    return cached_read_sql("""
        SELECT
            COUNT(*) AS total_games,
            (
                SELECT COUNT(*)
                FROM steam_game_measurements
                WHERE run_id = (
                    SELECT MAX(run_id) FROM steam_game_measurements
                )
            ) AS valve_measured_games,
            (
                SELECT SUM(current_players)
                FROM steam_game_measurements
                WHERE run_id = (
                    SELECT MAX(run_id) FROM steam_game_measurements
                )
            ) AS valve_current_players
        FROM current_game_metrics

    """).iloc[0]


def get_latest_run():
    runs = cached_read_sql("""
        SELECT run_id, status, rows_loaded, finished_at, warning_message
        FROM pipeline_runs
        ORDER BY run_id DESC
        LIMIT 1
    """)

    if runs.empty:
        return None

    return runs.iloc[0]


def get_measurement_series(appid):
    runs = cached_read_sql("""
        SELECT run_id, finished_at
        FROM pipeline_runs
        WHERE status = 'SUCCESS'
        ORDER BY run_id
    """)
    first_measured_run_id = cached_read_sql("""
        SELECT MIN(run_id) AS run_id
        FROM steam_game_measurements
    """).iloc[0]["run_id"]
    measurements = cached_read_sql(
        """
            SELECT
                run_id,
                collected_at,
                source_measured_at,
                current_players,
                total_reviews,
                positive_reviews,
                negative_reviews,
                review_score_percent
            FROM steam_game_measurements
            WHERE appid = %s
        """,
        params=(appid,)
    )

    return build_measurement_series(
        runs,
        measurements,
        first_measured_run_id
    )


def get_steam_measurement_games():
    return cached_read_sql("""
        SELECT DISTINCT measurements.appid, games.name
        FROM steam_game_measurements AS measurements
        JOIN games ON measurements.appid = games.appid
        ORDER BY games.name
    """)


st.title("Steam Data Pipeline Dashboard")

latest_run = get_latest_run()

if latest_run is None:
    st.info("No pipeline runs are available yet.")
    st.stop()

kpis = get_kpis()

metric_1, metric_2, metric_3, metric_4 = st.columns(4)

metric_1.metric("Games Loaded", format_number(kpis["total_games"]))
metric_2.metric("Valve-Measured Games", format_number(kpis["valve_measured_games"]))
metric_3.metric("Valve Current Players", format_number(kpis["valve_current_players"]))
metric_4.metric("Latest Run", f"{latest_run['status']} ({latest_run['rows_loaded']})")

st.caption(
    f"Latest run: {format_prague_time(latest_run['finished_at'])}"
)

if pd.notna(latest_run["warning_message"]) and latest_run["warning_message"]:
    st.warning(
        "The latest run completed with warnings. Some Valve measurements "
        "or SteamSpy rows may be missing from this run."
    )

    with st.expander("Run warnings"):
        st.text(latest_run["warning_message"])

st.caption(
    f"Query results are cached for {QUERY_CACHE_SECONDS // 60} minutes."
)

selected_view = st.selectbox(
    "Analysis view",
    list(VIEW_OPTIONS.keys())
)

view_config = VIEW_OPTIONS[selected_view]
df = cached_read_sql(view_config["query"])

fig = px.bar(
    df.sort_values(view_config["x"], ascending=True),
    x=view_config["x"],
    y=view_config["y"],
    orientation="h",
    title=view_config["title"],
    text=view_config["x"],
    hover_data=df.columns
)

fig.update_traces(
    texttemplate="%{text:,}",
    textposition="outside"
)

fig.update_layout(
    height=750,
    margin=dict(l=20, r=80, t=70, b=40),
    xaxis_title=view_config["x"].replace("_", " ").title(),
    yaxis_title=""
)

st.plotly_chart(fig, use_container_width=True)

st.dataframe(
    df,
    use_container_width=True,
    hide_index=True
)

st.divider()

st.subheader("Valve Steam Measurements Over Time")

st.caption(
    "Current players come from Valve's top-100 concurrent-player feed, with "
    "direct Valve lookups for configured games outside that list. Review "
    "totals are collected only for configured tracked games. Charts use the "
    "source measurement time when Valve provides it and otherwise show the "
    "collection time. Runs that did not measure the selected game leave a "
    "gap in the line instead of a line drawn across the missing run."
)

measurement_games = get_steam_measurement_games()

if measurement_games.empty:
    st.info(
        "No Valve Steam measurements yet. Run the updated pipeline once to "
        "start collecting them."
    )
else:
    game_names = dict(zip(
        measurement_games["appid"].astype(int),
        measurement_games["name"]
    ))
    selected_measurement_appid = st.selectbox(
        "Game",
        list(game_names),
        format_func=lambda appid: f"{game_names[appid]} ({appid})",
        key="steam_measurement_game"
    )
    selected_measurement_game = game_names[selected_measurement_appid]

    measurement_labels = {
        "current_players": "Current Players",
        "total_reviews": "Steam User Reviews",
        "positive_reviews": "Positive Steam User Reviews",
        "negative_reviews": "Negative Steam User Reviews",
        "review_score_percent": "Positive Steam Review Share"
    }

    measurement_df = get_measurement_series(selected_measurement_appid)

    available_metrics = [
        metric
        for metric in measurement_labels
        if measurement_df[metric].notna().any()
    ]
    selected_measurement = st.selectbox(
        "Metric",
        available_metrics,
        format_func=measurement_labels.get,
        key="steam_measurement_metric"
    )

    measurement_fig = px.line(
        measurement_df,
        x="measurement_time",
        y=selected_measurement,
        markers=True,
        title=(
            f"{selected_measurement_game}: "
            f"{measurement_labels[selected_measurement]} over time"
        )
    )
    measurement_fig.update_layout(
        height=450,
        xaxis_title="Measurement Time (UTC; collection, then run fallback)",
        yaxis_title=measurement_labels[selected_measurement]
    )
    st.plotly_chart(measurement_fig, use_container_width=True)

    recent_values = measurement_df[selected_measurement].dropna().tail(3)

    if len(recent_values) == 3:
        if recent_values.nunique() == 1:
            st.warning(
                "The latest three collected values are identical. The source "
                "may be unchanged or cached; verify it before treating the "
                "latest collection as a new measurement."
            )

    st.dataframe(
        measurement_df[[
            "run_id",
            "measurement_time",
            "collected_at",
            "source_measured_at",
            selected_measurement
        ]],
        use_container_width=True,
        hide_index=True
    )

st.divider()

with st.expander("Legacy SteamSpy snapshots", expanded=False):
    st.warning(
        "Legacy SteamSpy snapshots are retained for audit and SQL access, but "
        "they are not charted because repeated collection times disguised "
        "unchanged upstream values as a time series."
    )

st.divider()

st.subheader("SQL Explorer")

with st.expander("Available tables, views, and columns", expanded=False):
    database_objects = get_database_objects()
    object_name_column = database_objects.columns[0]
    object_type_column = database_objects.columns[1]

    selected_object = st.selectbox(
        "Database object",
        database_objects[object_name_column].tolist(),
        format_func=lambda object_name: (
            f"{object_name} "
            f"({database_objects.loc[database_objects[object_name_column] == object_name, object_type_column].iloc[0]})"
        )
    )

    columns_df = get_columns(selected_object)

    st.dataframe(
        columns_df[["Field", "Type", "Null", "Key"]],
        use_container_width=True,
        hide_index=True
    )

example_queries = {
    "Latest pipeline runs": """
SELECT run_id, started_at, finished_at, status, rows_loaded,
       measurements_loaded, error_message, warning_message
FROM pipeline_runs
ORDER BY run_id DESC
LIMIT 10;
""",
    "Publisher summary": """
SELECT publisher, game_count, total_estimated_owners, avg_estimated_owners
FROM publisher_summary
LIMIT 10;
""",
    "Latest Valve player ranking": """
SELECT games.name, measurements.player_rank,
       measurements.current_players, measurements.source_measured_at
FROM steam_game_measurements AS measurements
JOIN games ON games.appid = measurements.appid
WHERE measurements.run_id = (
    SELECT MAX(run_id) FROM steam_game_measurements
)
ORDER BY measurements.current_players DESC
LIMIT 25;
""",
    "Pipeline runs by day": """
SELECT run_date, successful_runs, rows_loaded
FROM daily_pipeline_summary
ORDER BY run_date;
""",
    "Game metric trend": """
SELECT games.name, measurements.source_measured_at,
       measurements.collected_at, measurements.current_players
FROM steam_game_measurements AS measurements
JOIN games ON games.appid = measurements.appid
WHERE games.name LIKE '%Factorio%'
ORDER BY measurements.collected_at;
"""
}

selected_example = st.selectbox(
    "Example SQL",
    list(example_queries.keys())
)

selected_query = example_queries[selected_example].strip()

st.code(selected_query, language="sql")

if st.button("Run example"):
    result_df = read_sql(selected_query)

    st.dataframe(
        result_df,
        use_container_width=True,
        hide_index=True
    )
