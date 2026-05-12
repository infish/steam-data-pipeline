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
            LIMIT 25
        """,
        "x": "ccu",
        "y": "name",
        "title": "Most Active Games By Current Players"
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


def read_sql(query):
    connection = get_connection()

    try:
        return pd.read_sql(query, connection)
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


def is_safe_read_query(query):
    normalized_query = query.strip().lower()
    return (
        normalized_query.startswith("select")
        or normalized_query.startswith("with")
        or normalized_query.startswith("show")
    )


def format_number(value):
    if pd.isna(value):
        return "N/A"

    return f"{int(value):,}"


def get_kpis():
    return read_sql("""
        SELECT
            COUNT(*) AS total_games,
            ROUND(AVG(review_score_percent), 2) AS avg_review_score_percent,
            SUM(ccu) AS total_current_players
        FROM games
    """).iloc[0]


def get_latest_run():
    return read_sql("""
        SELECT run_id, status, rows_loaded, finished_at
        FROM pipeline_runs
        ORDER BY run_id DESC
        LIMIT 1
    """).iloc[0]


st.title("Steam Data Pipeline Dashboard")

kpis = get_kpis()
latest_run = get_latest_run()

metric_1, metric_2, metric_3, metric_4 = st.columns(4)

metric_1.metric("Games Loaded", format_number(kpis["total_games"]))
metric_2.metric("Avg Review Score", f"{kpis['avg_review_score_percent']:.2f}%")
metric_3.metric("Current Players", format_number(kpis["total_current_players"]))
metric_4.metric("Latest Run", f"{latest_run['status']} ({latest_run['rows_loaded']})")

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
"""
}

selected_example = st.selectbox(
    "Example SQL",
    list(example_queries.keys())
)

sql_query = st.text_area(
    "SQL query",
    value=example_queries[selected_example],
    height=180
)

if st.button("Run SQL"):
    if not is_safe_read_query(sql_query):
        st.error("Only SELECT, WITH, and SHOW queries are allowed in this dashboard.")
    else:
        result_df = read_sql(sql_query)
        st.dataframe(
            result_df,
            use_container_width=True,
            hide_index=True
        )
