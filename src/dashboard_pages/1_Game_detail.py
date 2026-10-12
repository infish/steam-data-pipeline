"""Game detail: players, rank, patch events, reviews, price, attributes."""

import pandas as pd
import plotly.express as px
import streamlit as st

from dashboard_common import (
    cache_note,
    cached_read_sql,
    format_number,
    format_prague_time,
    to_prague,
)
from dashboard_data import (
    build_measurement_series,
    price_steps,
    with_missing_days,
)


st.title("Game detail")

games = cached_read_sql("""
    SELECT games.appid, games.name
    FROM games
    WHERE EXISTS (SELECT 1 FROM player_daily
                  WHERE player_daily.appid = games.appid)
       OR EXISTS (SELECT 1 FROM game_details
                  WHERE game_details.appid = games.appid)
    ORDER BY games.name, games.appid
""")

if games.empty:
    st.info("No game has player data or Store details yet.")
    st.stop()

names = dict(zip(games["appid"].astype(int), games["name"]))
appid = st.selectbox(
    "Game",
    list(names),
    format_func=lambda value: f"{names[value]} ({value})"
)
cache_note()

# ---- Attributes -------------------------------------------------------------

details = cached_read_sql(
    "SELECT * FROM game_details WHERE appid = %s", params=(appid,)
)
genres = cached_read_sql(
    """
        SELECT genres.description FROM game_genres
        JOIN genres USING (genre_id) WHERE appid = %s
        ORDER BY genres.description
    """,
    params=(appid,)
)
tags = cached_read_sql(
    """
        SELECT tags.name, game_tags.votes FROM game_tags
        JOIN tags USING (tag_id) WHERE appid = %s
        ORDER BY game_tags.votes DESC LIMIT 10
    """,
    params=(appid,)
)

if details.empty:
    st.caption("Store details have not been fetched for this game yet.")
else:
    row = details.iloc[0]
    platforms = [
        name for name, column in (
            ("Windows", "platform_windows"),
            ("macOS", "platform_mac"),
            ("Linux", "platform_linux"),
        )
        if row[column] == 1
    ]
    released, metacritic, price = st.columns(3)
    released.metric(
        "Released",
        str(row["release_date"]) if pd.notna(row["release_date"])
        else (row["release_date_text"] or "Unknown")
    )
    metacritic.metric(
        "Metacritic",
        format_number(row["metacritic_score"])
    )
    price.metric(
        "Price model",
        "Free to play" if row["is_free"] == 1 else "Paid"
        if row["is_free"] == 0 else "Unknown"
    )
    st.markdown(
        "**Platforms:** " + (", ".join(platforms) or "unknown")
        + "  \n**Genres:** " + (", ".join(genres["description"]) or "none")
        + "  \n**Top tags:** "
        + (", ".join(
            f"{tag} ({votes:,})"
            for tag, votes in zip(tags["name"], tags["votes"].astype(int))
        ) or "none")
    )
    st.caption(
        "Steam Store details and SteamSpy user tags (with vote counts), "
        f"last refreshed {format_prague_time(row['details_fetched_at'])}."
        + ("" if row["store_available"] == 1 else
           " The Store no longer returns this game; these are the last "
           "known details.")
    )

# ---- Players ----------------------------------------------------------------

st.subheader("Players")

daily = cached_read_sql(
    """
        SELECT observation_date, peak_players, avg_players, avg_players_7d,
               best_rank
        FROM mart_player_trends
        WHERE appid = %s
        ORDER BY observation_date
    """,
    params=(appid,)
)
events = cached_read_sql(
    """
        SELECT event_time, title, url
        FROM game_events
        WHERE appid = %s AND is_patch_notes AND is_developer_post
        ORDER BY event_time DESC
    """,
    params=(appid,)
)

if daily.empty:
    st.info("No daily player data for this game yet.")
else:
    filled = with_missing_days(daily)
    plotted = filled.melt(
        id_vars="observation_date",
        value_vars=["peak_players", "avg_players", "avg_players_7d"],
        var_name="measure",
        value_name="players"
    )
    figure = px.line(
        plotted,
        x="observation_date",
        y="players",
        color="measure",
        markers=True,
        labels={"observation_date": "UTC day", "players": "Players"},
    )
    first_day = filled["observation_date"].min()
    last_day = filled["observation_date"].max()

    for event_time in pd.to_datetime(events["event_time"]):
        if first_day <= event_time <= last_day + pd.Timedelta(days=1):
            figure.add_vline(
                x=event_time,
                line_dash="dot",
                line_color="gray"
            )

    figure.update_layout(height=420)
    figure.update_xaxes(tickformat="%Y-%m-%d")
    st.plotly_chart(figure, use_container_width=True)
    st.caption(
        "Daily peak and average of Valve's hourly player counts per UTC day "
        "(source time where Valve gives one), with the 7-day rolling "
        "average. Days without data leave a gap. Dotted lines mark the "
        "developer's patch-notes posts (Steam news publication time)."
    )

    ranked = filled[["observation_date", "best_rank"]]

    if ranked["best_rank"].notna().any():
        figure = px.line(
            ranked,
            x="observation_date",
            y="best_rank",
            markers=True,
            labels={"observation_date": "UTC day",
                    "best_rank": "Best top-100 rank"},
        )
        figure.update_yaxes(autorange="reversed")
        figure.update_layout(height=300)
        figure.update_xaxes(tickformat="%Y-%m-%d")
        st.plotly_chart(figure, use_container_width=True)
        st.caption(
            "Best rank in Valve's top-100 concurrent-player chart per UTC "
            "day; gaps are days outside the top 100 or without data."
        )

hourly = build_measurement_series(
    cached_read_sql("""
        SELECT run_id, finished_at FROM pipeline_runs
        WHERE status = 'SUCCESS' AND job_name = 'players_hourly'
          AND finished_at >= UTC_TIMESTAMP() - INTERVAL 14 DAY
        ORDER BY run_id
    """),
    cached_read_sql(
        """
            SELECT run_id, collected_at, source_measured_at, current_players
            FROM player_observations WHERE appid = %s
        """,
        params=(appid,)
    ),
    cached_read_sql(
        "SELECT MIN(run_id) AS run_id FROM player_observations"
    ).iloc[0]["run_id"]
)

if hourly["current_players"].notna().any():
    hourly["measurement_time"] = to_prague(hourly["measurement_time"])
    figure = px.line(
        hourly,
        x="measurement_time",
        y="current_players",
        markers=True,
        labels={"measurement_time": "Prague time",
                "current_players": "Current players"},
    )
    figure.update_layout(height=320)
    figure.update_xaxes(tickformat="%Y-%m-%d %H:%M")
    st.plotly_chart(figure, use_container_width=True)
    st.caption(
        "Hourly current players over the last 14 days: Valve's source "
        "measurement time when given, otherwise collection time. A run "
        "that did not measure this game leaves a gap."
    )

if not events.empty:
    with st.expander(f"Patch notes ({len(events)})"):
        shown = events.copy()
        shown["event_time"] = shown["event_time"].map(format_prague_time)
        st.dataframe(shown, hide_index=True, use_container_width=True)

# ---- Reviews ----------------------------------------------------------------

st.subheader("Reviews")

reviews = cached_read_sql(
    """
        SELECT observation_date, positive_share_lifetime, positive_share_30d,
               sentiment_drift_points, new_reviews, review_score_desc
        FROM mart_review_trends
        WHERE appid = %s
        ORDER BY observation_date
    """,
    params=(appid,)
)
history = cached_read_sql(
    """
        SELECT period_start,
               recommendations_up / NULLIF(recommendations_up
                   + recommendations_down, 0) * 100 AS positive_share,
               recommendations_up + recommendations_down AS reviews,
               is_partial
        FROM review_rollups
        WHERE appid = %s AND rollup_type = 'month'
        ORDER BY period_start
    """,
    params=(appid,)
)

if reviews.empty and history.empty:
    st.info("Reviews are collected only for the tracked games.")

if not reviews.empty:
    latest = reviews.iloc[-1]
    label, drift = st.columns(2)
    label.metric("Steam rating", latest["review_score_desc"] or "N/A")
    drift.metric(
        "Sentiment drift (30 days vs lifetime)",
        "N/A" if pd.isna(latest["sentiment_drift_points"])
        else f"{latest['sentiment_drift_points']:+.2f} points"
    )
    figure = px.line(
        with_missing_days(reviews).melt(
            id_vars="observation_date",
            value_vars=["positive_share_lifetime", "positive_share_30d"],
            var_name="measure",
            value_name="positive_share"
        ),
        x="observation_date",
        y="positive_share",
        color="measure",
        markers=True,
        labels={"observation_date": "UTC day",
                "positive_share": "Positive reviews (%)"},
    )
    figure.update_layout(height=320)
    figure.update_xaxes(tickformat="%Y-%m-%d")
    st.plotly_chart(figure, use_container_width=True)
    st.caption(
        "Lifetime positive share from Steam's review summary, collected "
        "daily, against the share over the 30 days ending that day from the "
        "review histogram. The two count different review sets, so compare "
        "the trend, not the absolute gap."
    )

if not history.empty:
    figure = px.bar(
        history,
        x="period_start",
        y="reviews",
        color="positive_share",
        color_continuous_scale="RdYlGn",
        range_color=(50, 100),
        labels={"period_start": "Month", "reviews": "Reviews",
                "positive_share": "Positive (%)"},
    )
    figure.update_layout(height=320)
    st.plotly_chart(figure, use_container_width=True)
    st.caption(
        "Monthly reviews and their positive share from Steam's review "
        "histogram, dated by the source; the current month is partial."
    )

# ---- Price ------------------------------------------------------------------

st.subheader("Price")

versions = cached_read_sql(
    """
        SELECT country_code, valid_from, price_state, currency, final_price,
               discount_percent
        FROM game_price_history
        WHERE appid = %s
        ORDER BY country_code, valid_from, price_id
    """,
    params=(appid,)
)

if versions.empty:
    st.info("No price history yet.")
else:
    country = versions["country_code"].iloc[0]
    country_versions = versions[versions["country_code"] == country]
    now = pd.Timestamp.now(tz="UTC").tz_localize(None)
    steps = price_steps(country_versions, now)

    if steps["price"].notna().any():
        currency = country_versions.loc[
            country_versions["price_state"] == "priced", "currency"
        ].iloc[-1]
        figure = px.line(
            steps,
            x="time",
            y="price",
            line_shape="hv",
            markers=True,
            labels={"time": "UTC", "price": f"Price ({currency})"},
        )
        figure.update_layout(height=300)
        st.plotly_chart(figure, use_container_width=True)

    st.caption(
        f"Steam Store price for country '{country}' as a step chart: each "
        "step starts when a daily check first saw the price. Gaps are "
        "periods without a price (free or unavailable)."
    )
    st.dataframe(country_versions, hide_index=True, use_container_width=True)
