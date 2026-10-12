"""reviews_daily: review summaries, review history and news for tracked apps.
"""

import logging

from api.steam_api import get_review_summaries
from config import get_tracked_appids
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

    summaries, warnings = get_review_summaries(
        ctx.session,
        appids,
        ctx.store
    )
    ctx.warn(*warnings)
    loaded = load_review_observations(
        ctx.cursor,
        summaries,
        ctx.run_id,
        ctx.load_time()
    )
    rollups = collect_histograms(ctx, appids, summaries)
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
