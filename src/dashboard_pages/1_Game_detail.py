"""Game detail: one game's players, reviews, price, updates and attributes."""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from dashboard_common import (
    cache_note,
    cached_read_sql,
    format_number,
    format_prague_time,
    steamspy_frozen_note,
    to_prague,
)
from dashboard_data import (
    build_measurement_series,
    latest_values_identical,
    price_steps,
    with_missing_days,
)
from dashboard_style import (
    card,
    compact,
    count_ticks,
    frozen_note,
    line,
    page_header,
    show,
    style,
    tokens,
)


page_header(
    "Game detail",
    "Everything collected for one game: players, reviews, price, updates "
    "and Store attributes."
)

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
    format_func=lambda value: f"{names[value]} ({value})",
    label_visibility="collapsed",
)

# ---- Data ---------------------------------------------------------------

details = cached_read_sql(
    "SELECT * FROM game_details WHERE appid = %s", params=(appid,)
)
daily = cached_read_sql(
    """
        SELECT observation_date, peak_players, avg_players, avg_players_7d,
               best_rank, peak_change_pct_wow
        FROM mart_player_trends
        WHERE appid = %s
        ORDER BY observation_date
    """,
    params=(appid,)
)
reviews = cached_read_sql(
    """
        SELECT observation_date, total_reviews, positive_share_lifetime,
               positive_share_30d, sentiment_drift_points, new_reviews,
               review_score_desc
        FROM mart_review_trends
        WHERE appid = %s
        ORDER BY observation_date
    """,
    params=(appid,)
)
price = cached_read_sql(
    "SELECT * FROM mart_price_status WHERE appid = %s", params=(appid,)
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
latest_players = cached_read_sql(
    """
        SELECT current_players, player_rank,
               COALESCE(source_measured_at, collected_at) AS measured_at
        FROM player_observations
        WHERE appid = %s
        ORDER BY run_id DESC
        LIMIT 1
    """,
    params=(appid,)
)

# ---- Header ---------------------------------------------------------------

st.header(names[appid], divider="orange")
badges = []

if not details.empty:
    row = details.iloc[0]

    if row["is_free"] == 1:
        badges.append(":blue-badge[Free to play]")
    elif row["is_free"] == 0:
        badges.append(":gray-badge[Paid]")

    for label, column in (("Windows", "platform_windows"),
                          ("macOS", "platform_mac"),
                          ("Linux", "platform_linux")):
        if row[column] == 1:
            badges.append(f":gray-badge[{label}]")

    if row["store_available"] == 0:
        badges.append(":orange-badge[:material/warning: not on the Store]")

if not reviews.empty and reviews.iloc[-1]["review_score_desc"]:
    badges.append(f":green-badge[{reviews.iloc[-1]['review_score_desc']}]")

if badges:
    st.markdown(" ".join(badges))

now, average, rank, share, current_price = st.columns(5)
now.metric(
    "Players now",
    compact(latest_players.iloc[0]["current_players"])
    if not latest_players.empty else "N/A",
    border=True,
    help=(
        f"{format_number(latest_players.iloc[0]['current_players'])} "
        "players, Valve's latest hourly measurement "
        f"({format_prague_time(latest_players.iloc[0]['measured_at'])})."
        if not latest_players.empty else "No measurement yet."
    ),
)
average.metric(
    "7-day avg",
    compact(daily.iloc[-1]["avg_players_7d"])
    if not daily.empty else "N/A",
    delta=(
        f"{daily.iloc[-1]['peak_change_pct_wow']:+.1f}% WoW peak"
        if not daily.empty and pd.notna(daily.iloc[-1]["peak_change_pct_wow"])
        else None
    ),
    border=True,
    help=(
        "Average daily players over the last 7 UTC days; the delta compares "
        "today's peak with the same day a week earlier (WoW)."
    ),
)
rank.metric(
    "Rank",
    f"#{int(daily.iloc[-1]['best_rank'])}"
    if not daily.empty and pd.notna(daily.iloc[-1]["best_rank"]) else "—",
    border=True,
    help="Best rank today in Valve's top-100 concurrent-player chart.",
)
share.metric(
    "Positive",
    f"{reviews.iloc[-1]['positive_share_lifetime']:.1f}%"
    if not reviews.empty else "N/A",
    delta=(
        f"{reviews.iloc[-1]['sentiment_drift_points']:+.1f} pts, 30 d"
        if not reviews.empty
        and pd.notna(reviews.iloc[-1]["sentiment_drift_points"]) else None
    ),
    border=True,
    help=(
        "Share of positive reviews, lifetime, from Steam's review summary; "
        "the delta is the last 30 days' share minus the lifetime share (sentiment "
        "drift). Reviews are collected for tracked games only."
    ),
)

if price.empty:
    price_text, price_delta = "N/A", None
else:
    status = price.iloc[0]

    if status["price_state"] == "priced":
        price_text = (
            f"{status['current_final_price'] / 100:,.2f} {status['currency']}"
        )
        price_delta = (
            f"-{int(status['discount_percent'])}% sale"
            if status["on_sale"] else None
        )
    else:
        price_text = {"no_price": "Free", "unavailable": "Unavailable"}.get(
            status["price_state"], status["price_state"]
        )
        price_delta = None

current_price.metric(
    "Price",
    price_text,
    delta=price_delta,
    delta_color="off",
    border=True,
    help="Steam Store price from the latest daily check.",
)

players_tab, reviews_tab, price_tab, updates_tab, about_tab = st.tabs([
    ":material/group: Players",
    ":material/reviews: Reviews",
    ":material/sell: Price",
    ":material/update: Updates",
    ":material/info: About",
])

# ---- Players --------------------------------------------------------------

with players_tab:
    if daily.empty:
        st.info("No daily player data for this game yet.")
    else:
        with card(
            "Daily players",
            "Valve player counts per UTC day: the peak, the average, and the "
            "7-day rolling average. Gaps are days without data; dotted lines "
            "are the developer's patch notes."
        ):
            filled = with_missing_days(daily)
            figure = line(
                filled,
                "observation_date",
                {
                    "peak_players": "Daily peak",
                    "avg_players": "Daily average",
                    "avg_players_7d": "7-day average",
                },
                y_title="Players",
                x_format="%Y-%m-%d",
            )
            first, last = filled["observation_date"].min(), \
                filled["observation_date"].max()

            for event_time in pd.to_datetime(events["event_time"]):
                if first <= event_time <= last + pd.Timedelta(days=1):
                    figure.add_vline(
                        x=event_time,
                        line_dash="dot",
                        line_width=1,
                        line_color=tokens().muted,
                    )

            show(figure)

        if filled["best_rank"].notna().any():
            with card(
                "Top-100 rank",
                "Best rank per UTC day; gaps are days outside the top 100."
            ):
                figure = line(
                    filled,
                    "observation_date",
                    {"best_rank": "Best rank"},
                    height=240,
                    y_title="Rank",
                    y_format="d",
                    x_format="%Y-%m-%d",
                )
                figure.update_yaxes(autorange="reversed")
                show(figure)

    hourly = build_measurement_series(
        cached_read_sql("""
            SELECT run_id, finished_at FROM pipeline_runs
            WHERE status = 'SUCCESS' AND job_name = 'players_hourly'
              AND finished_at >= UTC_TIMESTAMP() - INTERVAL 14 DAY
            ORDER BY run_id
        """),
        cached_read_sql(
            """
                SELECT run_id, collected_at, source_measured_at,
                       current_players
                FROM player_observations WHERE appid = %s
            """,
            params=(appid,)
        ),
        cached_read_sql(
            "SELECT MIN(run_id) AS run_id FROM player_observations"
        ).iloc[0]["run_id"]
    )

    if hourly["current_players"].notna().any():
        with card(
            "Hourly players, last 14 days",
            "Prague time; Valve's measurement time when given, otherwise "
            "collection time. A run that missed this game leaves a gap."
        ):
            hourly["measurement_time"] = to_prague(hourly["measurement_time"])
            show(line(
                hourly,
                "measurement_time",
                {"current_players": "Current players"},
                height=280,
                y_title="Players",
                x_format="%Y-%m-%d %H:%M",
            ))

            if latest_values_identical(hourly["current_players"]):
                st.warning(
                    "The latest three hourly counts are identical. The "
                    "source may be unchanged or cached; check the "
                    "Pipeline page before treating them as new.",
                    icon=":material/warning:",
                )

# ---- Reviews --------------------------------------------------------------

with reviews_tab:
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
        st.info("Reviews are collected for the tracked games only.")

    if not reviews.empty:
        with card(
            "Positive share: last 30 days vs lifetime",
            "Lifetime share from Steam's daily review summary; 30-day share "
            "from the review histogram. They count different review sets, "
            "so read the trend rather than the gap."
        ):
            show(line(
                with_missing_days(reviews),
                "observation_date",
                {
                    "positive_share_lifetime": "Lifetime",
                    "positive_share_30d": "Last 30 days",
                },
                height=280,
                y_title="Positive reviews (%)",
                y_format=".1f",
                x_format="%Y-%m-%d",
            ))

            if latest_values_identical(reviews["total_reviews"]):
                st.warning(
                    "The latest three daily review totals are identical; the "
                    "source may be unchanged or cached.",
                    icon=":material/warning:",
                )

    if not history.empty:
        volume, sentiment = st.columns(2, gap="medium")

        with volume:
            with card(
                "Reviews per month",
                "From Steam's review histogram, dated by the source; the "
                "current month is still filling."
            ):
                figure = go.Figure(go.Bar(
                    x=history["period_start"],
                    y=history["reviews"],
                    marker={"color": tokens().series[0]},
                    hovertemplate="%{x|%b %Y}: %{y:,.0f} reviews"
                                  "<extra></extra>",
                ))
                style(figure, height=260, legend=False, y_title="Reviews")
                figure.update_layout(bargap=0.15)
                figure.update_yaxes(**count_ticks(history["reviews"].max()))
                show(figure)

        with sentiment:
            with card(
                "Positive share per month",
                "Share of that month's reviews that recommend the game."
            ):
                show(line(
                    history,
                    "period_start",
                    {"positive_share": "Positive share"},
                    height=260,
                    y_title="Positive (%)",
                    y_format=".1f",
                    x_format="%b %Y",
                ))

# ---- Price ----------------------------------------------------------------

with price_tab:
    versions = cached_read_sql(
        """
            SELECT country_code, valid_from, valid_to, price_state, currency,
                   initial_price, final_price, discount_percent
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
        now_utc = pd.Timestamp.now(tz="UTC").tz_localize(None)
        steps = price_steps(country_versions, now_utc)

        with card(
            f"Price history (Store country '{country}')",
            "Each step starts when the daily check first saw the price. "
            "Gaps are periods without a price (free or unavailable)."
        ):
            if steps["price"].notna().any():
                currency = country_versions.loc[
                    country_versions["price_state"] == "priced", "currency"
                ].iloc[-1]
                figure = line(
                    steps,
                    "time",
                    {"price": f"Price ({currency})"},
                    height=260,
                    y_title=f"Price ({currency})",
                    y_format=",.2f",
                    x_format="%Y-%m-%d",
                )
                figure.update_traces(line_shape="hv")
                show(figure)
            else:
                st.caption("No priced version yet.")

            st.dataframe(
                country_versions.drop(columns=["country_code"]),
                hide_index=True,
                width="stretch",
                column_config={
                    "initial_price": st.column_config.NumberColumn(
                        "Initial (cents)", format="%d"),
                    "final_price": st.column_config.NumberColumn(
                        "Final (cents)", format="%d"),
                    "discount_percent": st.column_config.NumberColumn(
                        "Discount", format="%d%%"),
                },
            )

# ---- Updates --------------------------------------------------------------

with updates_tab:
    if events.empty:
        st.info("No developer patch notes stored for this game.")
    else:
        with card(
            f"Patch notes ({len(events)})",
            "The developer's own Steam announcements tagged as patch notes, "
            "by publication time (Prague time)."
        ):
            shown = events.copy()
            shown["event_time"] = to_prague(shown["event_time"])
            st.dataframe(
                shown,
                hide_index=True,
                width="stretch",
                column_config={
                    "event_time": st.column_config.DatetimeColumn(
                        "Published", format="YYYY-MM-DD HH:mm"),
                    "title": st.column_config.TextColumn("Title"),
                    "url": st.column_config.LinkColumn(
                        "Link", display_text="Open"),
                },
            )

# ---- About ----------------------------------------------------------------

with about_tab:
    if details.empty:
        st.info("Store details have not been fetched for this game yet.")
    else:
        row = details.iloc[0]
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
                SELECT tags.name AS tag, game_tags.votes FROM game_tags
                JOIN tags USING (tag_id) WHERE appid = %s
                ORDER BY game_tags.votes DESC LIMIT 15
            """,
            params=(appid,)
        )
        facts, tag_list = st.columns([1, 2], gap="medium")

        released = (
            row["release_date"] if pd.notna(row["release_date"])
            else row["release_date_text"] or "unknown"
        )

        with facts:
            with card("Store facts"):
                st.markdown(
                    f"**Released:** {released}  \n"
                    f"**Metacritic:** {format_number(row['metacritic_score'])}"
                    f"  \n**Required age:** "
                    f"{format_number(row['required_age'])}  \n"
                    f"**Genres:** {', '.join(genres['description']) or 'none'}"
                )
                st.caption(
                    "Steam Store details, refreshed "
                    f"{format_prague_time(row['details_fetched_at'])}."
                )

        with tag_list:
            with card("Top user tags", "SteamSpy user tags by votes."):
                frozen_note(steamspy_frozen_note())

                if tags.empty:
                    st.caption("No tags yet.")
                else:
                    st.dataframe(
                        tags,
                        hide_index=True,
                        width="stretch",
                        column_config={
                            "votes": st.column_config.ProgressColumn(
                                "Votes",
                                format="%d",
                                min_value=0,
                                max_value=int(tags["votes"].max()),
                            ),
                        },
                    )

cache_note()
