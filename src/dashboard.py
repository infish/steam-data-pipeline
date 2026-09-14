import plotly.express as px
import streamlit as st
import pandas as pd

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
                estimated_owners,
                review_score_percent,
                ccu,
                price_cents
            FROM top_games_by_owners
            ORDER BY estimated_owners DESC
            LIMIT 25
        """,
        "x": "estimated_owners",
        "y": "name",
        "title": "Top Games By Estimated Owners"
    },
    "Top games by review score": {
        "query": """
            SELECT
                name,
                developer,
                publisher,
                total_reviews,
                review_score_percent,
                estimated_owners,
                ccu
            FROM top_games_by_review_score
            ORDER BY review_score_percent DESC, total_reviews DESC
            LIMIT 25
        """,
        "x": "review_score_percent",
        "y": "name",
        "title": "Top Games By Review Score"
    },
    "Most active games by current players": {
        "query": """
            SELECT
                name,
                developer,
                publisher,
                ccu,
                estimated_owners,
                review_score_percent
            FROM most_active_games_by_ccu
            ORDER BY ccu DESC
            LIMIT 25
        """,
        "x": "ccu",
        "y": "name",
        "title": "Most Active Games By SteamSpy-Reported CCU"
    },
    "Top publishers by estimated owners": {
        "query": """
            SELECT
                publisher,
                game_count,
                avg_estimated_owners,
                total_estimated_owners,
                avg_review_score_percent,
                total_ccu
            FROM publisher_summary
            ORDER BY total_estimated_owners DESC
            LIMIT 25
        """,
        "x": "total_estimated_owners",
        "y": "publisher",
        "title": "Top Publishers By Estimated Owners"
    },
    "Free vs paid summary": {
        "query": """
            SELECT
                price_type,
                game_count,
                avg_estimated_owners,
                avg_review_score_percent,
                avg_ccu
            FROM free_vs_paid_summary
        """,
        "x": "game_count",
        "y": "price_type",
        "title": "Free vs Paid Games"
    }
}


def read_sql(query, params=None):
    connection = get_connection()

    try:
        return pd.read_sql(query, connection, params=params)
    finally:
        connection.close()


def get_database_objects():
    return read_sql("""
        SHOW FULL TABLES
    """)


def get_columns(object_name):
    return read_sql(f"""
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
    return read_sql("""
        SELECT
            COUNT(*) AS total_games,
            ROUND(AVG(review_score_percent), 2) AS avg_review_score_percent,
            SUM(ccu) AS total_steamspy_ccu
        FROM current_game_metrics

    """).iloc[0]


def get_latest_run():
    runs = read_sql("""
        SELECT run_id, status, rows_loaded, finished_at
        FROM pipeline_runs
        ORDER BY run_id DESC
        LIMIT 1
    """)

    if runs.empty:
        return None

    return runs.iloc[0]


def get_snapshot_games():
    return read_sql("""
        SELECT DISTINCT appid, name
        FROM game_metric_trends
        ORDER BY name
    """)


def get_steam_measurement_games():
    return read_sql("""
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
metric_2.metric("Avg Review Score", f"{kpis['avg_review_score_percent']:.2f}%")
metric_3.metric("SteamSpy-Reported CCU", format_number(kpis["total_steamspy_ccu"]))
metric_4.metric("Latest Run", f"{latest_run['status']} ({latest_run['rows_loaded']})")

st.caption(
    f"Latest run: {format_prague_time(latest_run['finished_at'])}"
)
selected_view = st.selectbox(
    "Analysis view",
    list(VIEW_OPTIONS.keys())
)

view_config = VIEW_OPTIONS[selected_view]
df = read_sql(view_config["query"])

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
    "Current players come from Steam's Web API. Review totals come from "
    "Steam Store reviews for all languages and purchase types, including "
    "off-topic activity. These endpoints do not return a source measurement "
    "timestamp, so points use and are explicitly labeled by collection time."
)

measurement_games = get_steam_measurement_games()

if measurement_games.empty:
    st.info(
        "No Valve Steam measurements yet. Run the updated pipeline once to "
        "start collecting them."
    )
else:
    measurement_col_1, measurement_col_2 = st.columns([2, 1])

    with measurement_col_1:
        selected_measurement_game = st.selectbox(
            "Game",
            measurement_games["name"].tolist(),
            key="steam_measurement_game"
        )

    measurement_labels = {
        "current_players": "Current Players",
        "total_reviews": "Steam User Reviews",
        "positive_reviews": "Positive Steam User Reviews",
        "negative_reviews": "Negative Steam User Reviews",
        "review_score_percent": "Positive Steam Review Share"
    }

    with measurement_col_2:
        selected_measurement = st.selectbox(
            "Metric",
            list(measurement_labels),
            format_func=measurement_labels.get,
            key="steam_measurement_metric"
        )

    selected_measurement_appid = int(
        measurement_games.loc[
            measurement_games["name"] == selected_measurement_game,
            "appid"
        ].iloc[0]
    )

    measurement_df = read_sql(
        f"""
            SELECT
                collected_at,
                source_measured_at,
                {selected_measurement}
            FROM steam_game_measurements
            WHERE appid = %s
            ORDER BY collected_at
        """,
        params=(selected_measurement_appid,)
    )

    measurement_fig = px.line(
        measurement_df,
        x="collected_at",
        y=selected_measurement,
        markers=True,
        title=(
            f"{selected_measurement_game}: "
            f"{measurement_labels[selected_measurement]} by collection time"
        )
    )
    measurement_fig.update_layout(
        height=450,
        xaxis_title="Collected At (UTC)",
        yaxis_title=measurement_labels[selected_measurement]
    )
    st.plotly_chart(measurement_fig, use_container_width=True)

    if len(measurement_df) >= 3:
        recent_values = measurement_df[selected_measurement].tail(3)
        if recent_values.nunique(dropna=False) == 1:
            st.warning(
                "The latest three collected values are identical. The source "
                "may be unchanged or cached; verify it before treating the "
                "latest collection as a new measurement."
            )

    st.dataframe(measurement_df, use_container_width=True, hide_index=True)

st.divider()

with st.expander("Legacy SteamSpy snapshots", expanded=False):
    st.caption(
        "These historical fields are SteamSpy values collected by the old "
        "pipeline. SteamSpy provides no source measurement timestamp, and the "
        "stored collection time does not prove the underlying value was fresh."
    )

    snapshot_games = get_snapshot_games()

    if snapshot_games.empty:
        st.info("No SteamSpy snapshots are available.")
    else:
        trend_col_1, trend_col_2 = st.columns([2, 1])

        with trend_col_1:
            selected_game_name = st.selectbox(
                "Game",
                snapshot_games["name"].tolist(),
                key="steamspy_snapshot_game"
            )

        with trend_col_2:
            selected_metric = st.selectbox(
                "SteamSpy field",
                [
                    "ccu",
                    "estimated_owners",
                    "total_reviews",
                    "positive_reviews",
                    "negative_reviews",
                    "review_score_percent",
                    "price_cents",
                    "discount_percent"
                ],
                key="steamspy_snapshot_metric"
            )

        selected_appid = int(
            snapshot_games.loc[
                snapshot_games["name"] == selected_game_name,
                "appid"
            ].iloc[0]
        )

        trend_df = read_sql(
            f"""
                SELECT snapshot_time AS collected_at, name, {selected_metric}
                FROM game_metric_trends
                WHERE appid = %s
                ORDER BY snapshot_time
            """,
            params=(selected_appid,)
        )

        trend_fig = px.line(
            trend_df,
            x="collected_at",
            y=selected_metric,
            markers=True,
            title=(
                f"{selected_game_name}: SteamSpy "
                f"{selected_metric.replace('_', ' ').title()} by collection time"
            )
        )

        trend_fig.update_layout(
            height=450,
            xaxis_title="Collected At (UTC)",
            yaxis_title=f"SteamSpy {selected_metric.replace('_', ' ').title()}"
        )

        st.plotly_chart(trend_fig, use_container_width=True)

        st.dataframe(
            trend_df,
            use_container_width=True,
            hide_index=True
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
SELECT run_id, started_at, finished_at, status, rows_loaded, error_message
FROM pipeline_runs
ORDER BY run_id DESC
LIMIT 10;
""",
    "Publisher summary": """
SELECT publisher, game_count, total_estimated_owners, avg_review_score_percent, total_ccu
FROM publisher_summary
LIMIT 10;
""",
    "Best reviewed games": """
SELECT name, total_reviews, review_score_percent, estimated_owners
FROM top_games_by_review_score
LIMIT 10;
""",
    "Free vs paid": """
SELECT *
FROM free_vs_paid_summary;
""",
    "Pipeline runs by day": """
SELECT run_date, successful_runs, rows_loaded
FROM daily_pipeline_summary
ORDER BY run_date;
""",
    "Game metric trend": """
SELECT snapshot_time, name, ccu, review_score_percent, total_reviews
FROM game_metric_trends
WHERE name LIKE '%Factorio%'
ORDER BY snapshot_time;
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
