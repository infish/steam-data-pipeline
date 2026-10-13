"""Market explorer: genres, tags, release years, free versus paid."""

import plotly.graph_objects as go
import streamlit as st

from dashboard_common import cache_note, cached_read_sql
from dashboard_style import (
    card,
    count_ticks,
    hbar,
    page_header,
    show,
    style,
    tokens,
)


page_header(
    "Market explorer",
    "How the catalog breaks down by genre, user tag, release year and "
    "pricing."
)

coverage = cached_read_sql("""
    SELECT
        (SELECT COUNT(*) FROM current_game_metrics) AS catalog_games,
        (SELECT COUNT(*) FROM game_details
         WHERE appid IN (SELECT appid FROM current_game_metrics))
            AS with_details,
        (SELECT COUNT(*) FROM game_details WHERE is_free) AS free_games,
        (SELECT COUNT(*) FROM mart_price_status WHERE on_sale) AS on_sale
""").iloc[0]
catalog_games = int(coverage["catalog_games"] or 0)
with_details = int(coverage["with_details"] or 0)

games, covered, free, sale = st.columns(4)
games.metric("Catalog games", f"{catalog_games:,}", border=True)
covered.metric(
    "With Store details",
    f"{with_details:,}",
    delta=(f"{with_details / catalog_games:.0%} of catalog"
           if catalog_games else None),
    delta_color="off",
    border=True,
    help=(
        "Genres, release dates, platforms and the free flag come from Store "
        "details; about 120 games are refreshed per day, so coverage grows "
        "over the first days."
    ),
)
free.metric("Free to play", f"{int(coverage['free_games'] or 0):,}",
            border=True, help="Games the Store marks as free.")
sale.metric("On sale now", f"{int(coverage['on_sale'] or 0):,}",
            border=True, help="Games whose current Store price is discounted.")

genre_tab, tag_tab, year_tab, price_tab = st.tabs([
    ":material/category: Genres",
    ":material/sell: Tags",
    ":material/calendar_month: Release years",
    ":material/payments: Free vs paid",
])

score_column = st.column_config.ProgressColumn(
    "SteamSpy score",
    help="Average SteamSpy positive share (latest snapshot); broad "
         "comparison only.",
    format="%.1f%%",
    min_value=0,
    max_value=100,
)

with genre_tab:
    genres = cached_read_sql(
        "SELECT * FROM mart_genre_summary ORDER BY game_count DESC"
    )

    if genres.empty:
        st.info("No genre data yet.")
    else:
        with card(
            "Games per genre",
            "Steam Store genres; a game can have several."
        ):
            show(hbar(
                genres,
                "game_count",
                "genre",
                x_title="Games",
                hover_columns=("avg_review_score", "top100_games"),
            ))

        with card("Genre details"):
            st.dataframe(
                genres[[
                    "genre", "game_count", "avg_review_score",
                    "avg_metacritic", "top100_games",
                    "top100_current_players", "free_games", "paid_games",
                    "windows_games", "mac_games", "linux_games",
                ]],
                hide_index=True,
                use_container_width=True,
                column_config={
                    "genre": "Genre",
                    "game_count": "Games",
                    "avg_review_score": score_column,
                    "avg_metacritic": st.column_config.NumberColumn(
                        "Metacritic", format="%.0f"),
                    "top100_games": "In top 100",
                    "top100_current_players": st.column_config.NumberColumn(
                        "Top-100 players", format="localized"),
                    "free_games": "Free",
                    "paid_games": "Paid",
                    "windows_games": "Windows",
                    "mac_games": "macOS",
                    "linux_games": "Linux",
                },
            )

with tag_tab:
    tags = cached_read_sql("""
        SELECT * FROM mart_tag_summary
        ORDER BY game_count DESC, total_votes DESC
        LIMIT 25
    """)

    if tags.empty:
        st.info("No tag data yet.")
    else:
        with card(
            "Most common user tags",
            "The 25 SteamSpy user tags on the most games."
        ):
            show(hbar(
                tags,
                "game_count",
                "tag",
                x_title="Games",
                hover_columns=("total_votes",),
            ))

        with card("Tag details"):
            st.dataframe(
                tags[[
                    "tag", "game_count", "total_votes", "avg_review_score",
                    "top100_games", "top100_current_players",
                ]],
                hide_index=True,
                use_container_width=True,
                column_config={
                    "tag": "Tag",
                    "game_count": "Games",
                    "total_votes": st.column_config.NumberColumn(
                        "Votes", format="localized"),
                    "avg_review_score": score_column,
                    "top100_games": "In top 100",
                    "top100_current_players": st.column_config.NumberColumn(
                        "Top-100 players", format="localized"),
                },
            )

with year_tab:
    years = cached_read_sql(
        "SELECT * FROM mart_release_year_summary ORDER BY release_year"
    )

    if years.empty:
        st.info("No release dates yet.")
    else:
        with card(
            "Games per release year",
            "Store release years (complete dates only), free and paid "
            "stacked."
        ):
            t = tokens()
            figure = go.Figure()

            for index, (column, name) in enumerate((
                ("paid_games", "Paid"),
                ("free_games", "Free to play"),
            )):
                figure.add_trace(go.Bar(
                    x=years["release_year"],
                    y=years[column],
                    name=name,
                    marker={
                        "color": t.series[index],
                        # 2px surface gap between stacked segments.
                        "line": {"color": t.surface, "width": 2},
                    },
                    hovertemplate=f"%{{x}}: %{{y}} {name.lower()}"
                                  "<extra></extra>",
                ))

            style(figure, height=320, y_title="Games")
            # Legend in trace order (Paid first), not Plotly's reversed
            # order for stacks.
            figure.update_layout(barmode="stack",
                                 legend_traceorder="normal")
            figure.update_yaxes(**count_ticks(
                (years["paid_games"] + years["free_games"]).max()
            ))
            figure.update_traces(
                marker_line={"color": t.surface, "width": 2},
                selector={"type": "bar"},
            )
            figure.update_xaxes(dtick=1 if len(years) < 15 else None)
            show(figure)

with price_tab:
    models = cached_read_sql("""
        SELECT
            CASE
                WHEN details.is_free = 1 THEN 'Free to play'
                WHEN details.is_free = 0 THEN 'Paid'
                ELSE 'Not refreshed yet'
            END AS model,
            COUNT(*) AS games,
            SUM(COALESCE(prices.on_sale, 0)) AS on_sale_now
        FROM current_game_metrics AS games
        LEFT JOIN game_details AS details ON details.appid = games.appid
        LEFT JOIN mart_price_status AS prices ON prices.appid = games.appid
        GROUP BY model
        ORDER BY games DESC
    """)

    with card(
        "Free versus paid",
        "Catalog games by the Store's free-to-play flag. 'Not refreshed "
        "yet' means the game's Store details have not been fetched."
    ):
        st.dataframe(
            models,
            hide_index=True,
            use_container_width=True,
            column_config={
                "model": "Pricing",
                "games": st.column_config.ProgressColumn(
                    "Games",
                    format="%d",
                    min_value=0,
                    max_value=int(models["games"].max())
                    if not models.empty else 1,
                ),
                "on_sale_now": "On sale now",
            },
        )

cache_note()
