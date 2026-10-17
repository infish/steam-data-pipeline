"""reviews_daily: review summaries, review history and news.

Tracked apps get their summary, histogram and news every day. Other catalog
games get their summary on a rotation of REVIEW_SUMMARIES_PER_RUN, oldest
first, which replaces SteamSpy's frozen review counts (ADR 5).
"""

import logging

from api.steam_api import get_review_summaries
from config import get_review_summaries_per_run, get_tracked_appids
from jobs.base import Job
from jobs.loading import known_appids
from jobs.review_history import collect_histograms, collect_news


def load_review_observations(cursor, summaries, run_id, collected_at):
    cursor.executemany(
        """
        INSERT INTO review_observations (
            run_id, appid, collected_at, positive_reviews, negative_reviews,
            total_reviews, review_score_percent, review_score,
            review_score_desc
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        [
            (
                run_id,
                row["appid"],
                collected_at,
                row["positive_reviews"],
                row["negative_reviews"],
                row["total_reviews"],
                row["review_score_percent"],
                row["review_score"],
                row["review_score_desc"],
            )
            for row in summaries
        ]
    )
    return len(summaries)


def select_review_rotation(cursor, exclude, limit):
    """Catalog games without a summary first, then the oldest summary."""
    if limit == 0:
        return []

    excluded = sorted(set(exclude)) or [0]
    cursor.execute(
        f"""
        SELECT games.appid
        FROM games
        LEFT JOIN (
            SELECT appid, MAX(collected_at) AS last_collected
            FROM review_observations
            GROUP BY appid
        ) AS latest ON latest.appid = games.appid
        WHERE games.appid NOT IN ({", ".join(["%s"] * len(excluded))})
        ORDER BY latest.last_collected IS NOT NULL,
                 latest.last_collected,
                 games.appid
        LIMIT %s
        """,
        (*excluded, limit)
    )
    return [row[0] for row in cursor.fetchall()]


def rotation_in_run(cursor, run_id, exclude):
    """The rotated games a past run fetched summaries for (reprocessing)."""
    # The app ID is the last part of the endpoint (.../appreviews/<appid>).
    cursor.execute(
        """
        SELECT SUBSTRING_INDEX(endpoint, '/', -1)
        FROM raw_api_responses
        WHERE run_id = %s AND source = 'steam_review_summary'
        ORDER BY response_id
        """,
        (run_id,)
    )
    rotated = []

    for (value,) in cursor.fetchall():
        if str(value).isdigit() and int(value) not in exclude:
            rotated.append(int(value))

    return rotated


def run_reviews(ctx):
    tracked = get_tracked_appids()
    known = known_appids(ctx.cursor, tracked)
    appids = [appid for appid in tracked if appid in known]

    for appid in tracked:
        if appid not in known:
            logging.info(
                "Tracked app %s has no games row yet; reviews skipped",
                appid
            )

    if ctx.replay:
        rotation = rotation_in_run(ctx.cursor, ctx.run_id, set(appids))
    else:
        rotation = select_review_rotation(
            ctx.cursor, tracked, get_review_summaries_per_run()
        )

    with ctx.step("fetch_summaries"):
        summaries, warnings = get_review_summaries(
            ctx.session,
            appids + rotation,
            ctx.store,
            pace=lambda: ctx.pace("store")
        )

    ctx.warn(*warnings)

    with ctx.step("load_summaries") as step:
        loaded = load_review_observations(
            ctx.cursor,
            summaries,
            ctx.run_id,
            ctx.load_time()
        )
        step["rows"] = loaded

    # History and news stay with the tracked apps.
    with ctx.step("histograms") as step:
        rollups = collect_histograms(ctx, appids, summaries)
        step["rows"] = rollups

    with ctx.step("news"):
        collect_news(ctx, appids)

    # New news events depend on what earlier runs stored, so they are only
    # logged; the counters stay the same when a run is reprocessed.
    ctx.counters.update(
        rows_extracted=len(summaries),
        rows_loaded=loaded + rollups,
        measurements_loaded=loaded
    )


def clear_reviews_run(cursor, run_id):
    # Rollups and events are upserts keyed by period and gid; replaying the
    # run re-applies them without duplicates.
    for table in ("review_observations", "review_reconciliation"):
        cursor.execute(f"DELETE FROM {table} WHERE run_id = %s", (run_id,))


REVIEWS_JOB = Job(
    name="reviews_daily",
    body=run_reviews,
    clear_run=clear_reviews_run,
    load_time_sql=(
        "SELECT MIN(collected_at) FROM review_observations WHERE run_id = %s"
    )
)
