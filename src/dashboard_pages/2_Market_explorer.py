"""Market explorer: genres, tags, release years, free versus paid."""

import plotly.express as px
import streamlit as st

from dashboard_common import cache_note, cached_read_sql


st.title("Market explorer")
cache_note()
st.caption(
    "Store data (genres, release dates, platforms, free or paid) covers a "
    "game once its Store details have been refreshed; about 120 games are "
    "refreshed per day, so coverage grows over the first days. Review "
    "scores are SteamSpy's from the latest catalog snapshot, for broad "
    "comparison only."
)

coverage = cached_read_sql("""
    SELECT
        (SELECT COUNT(*) FROM current_game_metrics) AS catalog_games,
        (SELECT COUNT(*) FROM game_details
         WHERE appid IN (SELECT appid FROM current_game_metrics))
            AS with_details
""").iloc[0]
st.metric(
    "Catalog games with Store details",
    f"{int(coverage['with_details'] or 0)} of "
    f"{int(coverage['catalog_games'] or 0)}"
)

st.subheader("Genres")

genres = cached_read_sql(
    "SELECT * FROM mart_genre_summary ORDER BY game_count DESC"
)

if genres.empty:
    st.info("No genre data yet.")
else:
    figure = px.bar(
        genres.sort_values("game_count"),
        x="game_count",
        y="genre",
        orientation="h",
        color="avg_review_score",
        color_continuous_scale="RdYlGn",
        hover_data=["top100_games", "top100_current_players",
                    "avg_metacritic", "free_games", "paid_games"],
        labels={"game_count": "Games", "genre": "",
                "avg_review_score": "SteamSpy score"},
    )
    figure.update_layout(height=max(300, 28 * len(genres)))
    st.plotly_chart(figure, use_container_width=True)
    st.caption(
        "Games per Steam Store genre (mart_genre_summary), coloured by the "
        "average SteamSpy review score; top-100 figures use the latest "
        "hourly Valve run."
    )
    st.dataframe(genres, hide_index=True, use_container_width=True)

st.subheader("Tags")

tags = cached_read_sql(
    "SELECT * FROM mart_tag_summary ORDER BY game_count DESC, total_votes"
    " DESC LIMIT 25"
)

if tags.empty:
    st.info("No tag data yet.")
else:
    figure = px.bar(
        tags.sort_values("game_count"),
        x="game_count",
        y="tag",
        orientation="h",
        hover_data=["total_votes", "top100_games", "avg_review_score"],
        labels={"game_count": "Games", "tag": ""},
    )
    figure.update_layout(height=700)
    st.plotly_chart(figure, use_container_width=True)
    st.caption(
        "The 25 SteamSpy user tags on the most games (mart_tag_summary); "
        "total_votes sums the tag's votes across those games."
    )

st.subheader("Release years")

years = cached_read_sql(
    "SELECT * FROM mart_release_year_summary ORDER BY release_year"
)

if years.empty:
    st.info("No release dates yet.")
else:
    figure = px.bar(
        years.melt(
            id_vars="release_year",
            value_vars=["free_games", "paid_games"],
            var_name="model",
            value_name="games"
        ),
        x="release_year",
        y="games",
        color="model",
        labels={"release_year": "Release year", "games": "Games"},
    )
    st.plotly_chart(figure, use_container_width=True)
    st.caption(
        "Games per Store release year (complete dates only), split into "
        "free-to-play and paid (mart_release_year_summary)."
    )

st.subheader("Free versus paid")

models = cached_read_sql("""
    SELECT
        CASE
            WHEN details.is_free = 1 THEN 'Free to play'
            WHEN details.is_free = 0 THEN 'Paid'
            ELSE 'Unknown'
        END AS model,
        COUNT(*) AS games,
        SUM(COALESCE(prices.on_sale, 0)) AS on_sale_now
    FROM current_game_metrics AS games
    LEFT JOIN game_details AS details ON details.appid = games.appid
    LEFT JOIN mart_price_status AS prices ON prices.appid = games.appid
    GROUP BY model
""")
st.dataframe(models, hide_index=True, use_container_width=True)
st.caption(
    "Catalog games by the Store's free-to-play flag; 'Unknown' means Store "
    "details have not been fetched yet. On sale now: games whose current "
    "Store price has a discount (mart_price_status)."
)
