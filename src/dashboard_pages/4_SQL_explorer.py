"""Explorer: browse tables and views, run predefined example queries."""

import streamlit as st

from dashboard_common import cached_read_sql, read_sql
from dashboard_style import card, page_header


page_header(
    "Explorer",
    "Browse the tables, views and marts, and run predefined example "
    "queries. Read-only: the dashboard's database account can only SELECT."
)

EXPLORER_OBJECTS = {
    "current_game_metrics",
    "daily_pipeline_summary",
    "data_quality_results",
    "flyway_schema_history",
    "game_details",
    "game_events",
    "game_price_history",
    "game_price_summary",
    "games",
    "latest_successful_run",
    "mart_engagement",
    "mart_event_impact",
    "mart_genre_summary",
    "mart_player_peak_hours",
    "mart_player_trends",
    "mart_price_status",
    "mart_release_year_summary",
    "mart_review_trends",
    "mart_tag_summary",
    "most_played_daily",
    "pipeline_runs",
    "player_daily",
    "player_observations",
    "review_observations",
    "review_reconciliation",
    "review_rollups",
    "steam_game_measurements",
}

EXAMPLE_QUERIES = {
    "Latest pipeline runs": """
SELECT run_id, job_name, started_at, finished_at, status, rows_loaded,
       measurements_loaded, error_message, warning_message
FROM pipeline_runs
ORDER BY run_id DESC
LIMIT 10;
""",
    "Biggest weekly movers": """
SELECT name, observation_date, peak_players, peak_change_pct_wow
FROM mart_player_trends
WHERE observation_date = (SELECT MAX(observation_date)
                          FROM mart_player_trends)
  AND peak_change_pct_wow IS NOT NULL
ORDER BY ABS(peak_change_pct_wow) DESC
LIMIT 20;
""",
    "Games on sale at their lowest price": """
SELECT name, currency, current_final_price / 100 AS price,
       discount_percent, discount_events
FROM mart_price_status
WHERE on_sale AND above_lowest_pct = 0
ORDER BY discount_percent DESC;
""",
    "Sentiment drift": """
SELECT name, observation_date, positive_share_lifetime, positive_share_30d,
       sentiment_drift_points, review_score_desc
FROM mart_review_trends
ORDER BY observation_date DESC, ABS(sentiment_drift_points) DESC
LIMIT 20;
""",
    "Player change around patches": """
SELECT name, event_time, title, avg_players_7d_before,
       avg_players_7d_after, change_pct
FROM mart_event_impact
WHERE days_before > 0 AND days_after > 0
ORDER BY event_time DESC
LIMIT 20;
""",
    "Failed data-quality checks": """
SELECT checked_at, check_name, target, observed_value, threshold, details
FROM data_quality_results
WHERE status = 'fail'
ORDER BY checked_at DESC
LIMIT 20;
""",
    "Daily players for Factorio": """
SELECT games.name, daily.observation_date, daily.peak_players,
       daily.avg_players, daily.min_players, daily.observation_count
FROM player_daily AS daily
JOIN games ON games.appid = daily.appid
WHERE games.name LIKE '%Factorio%'
ORDER BY daily.observation_date;
""",
    "Runs per day and job": """
SELECT run_date, job_name, successful_runs, rows_loaded,
       measurements_loaded
FROM daily_pipeline_summary
ORDER BY run_date, job_name;
""",
}

browser, examples = st.columns([2, 3], gap="medium")

with browser:
    with card("Tables, views and marts", "Pick one to see its columns."):
        objects = cached_read_sql("SHOW FULL TABLES")
        name_column, type_column = objects.columns[0], objects.columns[1]
        objects = objects[objects[name_column].isin(EXPLORER_OBJECTS)]
        types = dict(zip(objects[name_column], objects[type_column]))
        selected = st.selectbox(
            "Database object",
            sorted(types),
            format_func=lambda name: (
                f"{name} ({'view' if types[name] == 'VIEW' else 'table'})"
            ),
            label_visibility="collapsed",
        )
        columns = cached_read_sql(f"SHOW COLUMNS FROM `{selected}`")
        st.dataframe(
            columns[["Field", "Type", "Null", "Key"]],
            use_container_width=True,
            hide_index=True,
        )
        st.caption(
            "Every table and column is described in docs/data_dictionary.md."
        )

with examples:
    with card("Example queries"):
        example = st.selectbox(
            "Example",
            list(EXAMPLE_QUERIES),
            label_visibility="collapsed",
        )
        query = EXAMPLE_QUERIES[example].strip()
        st.code(query, language="sql")

        if st.button("Run example", type="primary",
                     icon=":material/play_arrow:"):
            # Deliberately uncached: the result reflects the database now.
            st.dataframe(
                read_sql(query),
                use_container_width=True,
                hide_index=True,
            )
