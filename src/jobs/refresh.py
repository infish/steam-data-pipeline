"""refresh_hourly: Store details, SteamSpy tags and Steam review summaries
for a slice of the catalog every hour (ADR 6).

Spreading the work over the day keeps every run short, so the hourly player
collection is never held up, and keeps the Store requests evenly paced. The
slice sizes follow from the catalog size: every game's Store details are
refreshed every DETAILS_REFRESH_DAYS and its review summary every
REVIEWS_REFRESH_DAYS, within REFRESH_MAX_REQUESTS Store requests per run.
Games are always taken oldest first, so a missed or failed run is caught up
by the next ones.
"""

import logging
import math
from datetime import datetime, timedelta

from api.steam_api import get_review_summaries
from config import (
    get_details_refresh_days,
    get_quiet_reviews_refresh_days,
    get_refresh_max_requests,
    get_review_summaries_per_run,
    get_reviews_refresh_days,
    get_store_details_per_run,
    get_tracked_appids,
)
from jobs.base import Job
from jobs.dimensions import refresh_details
from jobs.reviews import load_review_observations


RUNS_PER_DAY = 24


def per_run(catalog_size, refresh_days):
    """Games per hourly run so that each is refreshed every refresh_days."""
    if refresh_days == 0 or catalog_size == 0:
        return 0

    return math.ceil(catalog_size / (refresh_days * RUNS_PER_DAY))


def plan_slices(catalog_size, details_days, reviews_days, max_requests,
                details_override=None, reviews_override=None):
    """(details, reviews) to fetch this run, within max_requests.

    An override (STORE_DETAILS_PER_RUN, REVIEW_SUMMARIES_PER_RUN) replaces
    the derived count. When both together exceed max_requests they shrink
    in proportion, keeping at least one of each that was asked for.
    """
    details = (
        per_run(catalog_size, details_days)
        if details_override is None else details_override
    )
    reviews = (
        per_run(catalog_size, reviews_days)
        if reviews_override is None else reviews_override
    )

    if details + reviews <= max_requests:
        return details, reviews

    scale = max_requests / (details + reviews)
    details = max(min(details, 1), math.floor(details * scale))
    return details, max(0, max_requests - details)


def select_review_rotation(cursor, exclude, limit, now=None,
                           active_days=1, quiet_days=0):
    """Catalog games whose review summary is due, most overdue first.

    A game is quiet when its last two summaries had the same total; it is
    due every quiet_days, other games every active_days (when quiet_days is
    0 or not longer, every game counts as active). Games without a recent
    summary come first. "Due" means due before the next hourly run, so a
    game fetched a few seconds later yesterday is not pushed back an hour.
    Without `now` every game is due and only the order applies.
    """
    if limit == 0:
        return []

    if not quiet_days or quiet_days <= active_days:
        quiet_days = active_days

    active_minutes = round(active_days * 24 * 60)
    quiet_minutes = round(quiet_days * 24 * 60)
    excluded = sorted(set(exclude)) or [0]
    due_by = None if now is None else now + timedelta(minutes=59)
    # Only recent summaries decide the state; older ones mean "overdue".
    since = (
        datetime(1970, 1, 1) if now is None
        else now - timedelta(minutes=quiet_minutes + 24 * 60)
    )
    cursor.execute(
        f"""
        WITH recent AS (
            SELECT appid, collected_at, total_reviews,
                   ROW_NUMBER() OVER (
                       PARTITION BY appid
                       ORDER BY collected_at DESC, observation_id DESC
                   ) AS position
            FROM review_observations
            WHERE collected_at >= %s
        ),
        state AS (
            SELECT last.appid,
                   last.collected_at + INTERVAL IF(
                       last.total_reviews <=> previous.total_reviews,
                       %s, %s
                   ) MINUTE AS due_at
            FROM recent AS last
            LEFT JOIN recent AS previous
                ON previous.appid = last.appid AND previous.position = 2
            WHERE last.position = 1
        )
        SELECT games.appid
        FROM games
        LEFT JOIN state ON state.appid = games.appid
        WHERE games.appid NOT IN ({", ".join(["%s"] * len(excluded))})
          AND (state.due_at IS NULL OR %s IS NULL OR state.due_at <= %s)
        ORDER BY state.due_at IS NOT NULL, state.due_at, games.appid
        LIMIT %s
        """,
        (since, quiet_minutes, active_minutes, *excluded, due_by, due_by,
         limit)
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

def run_refresh(ctx):
    ctx.cursor.execute("SELECT COUNT(*) FROM games")
    catalog_size = ctx.cursor.fetchall()[0][0]
    tracked = set(get_tracked_appids())
    details_count, reviews_count = plan_slices(
        catalog_size,
        get_details_refresh_days(),
        get_reviews_refresh_days(),
        get_refresh_max_requests(),
        get_store_details_per_run(),
        get_review_summaries_per_run(),
    )

    with ctx.step("store_details_and_tags") as step:
        refreshed = refresh_details(ctx, details_count)
        step["rows"] = refreshed

    # Tracked apps get their summary from reviews_daily every day.
    if ctx.replay:
        rotation = rotation_in_run(ctx.cursor, ctx.run_id, tracked)
    else:
        rotation = select_review_rotation(
            ctx.cursor,
            tracked,
            reviews_count,
            ctx.now_fn(),
            get_reviews_refresh_days(),
            get_quiet_reviews_refresh_days(),
        )

    with ctx.step("review_summaries"):
        summaries, warnings = get_review_summaries(
            ctx.session,
            rotation,
            ctx.store,
            pace=lambda: ctx.pace("store_reviews")
        )

    ctx.warn(*warnings)

    with ctx.step("load_summaries") as step:
        loaded = load_review_observations(
            ctx.cursor, summaries, ctx.run_id, ctx.load_time()
        )
        step["rows"] = loaded

    logging.info(
        "Refreshed %s of %s catalog games' Store details and %s review"
        " summaries",
        refreshed,
        catalog_size,
        loaded
    )
    ctx.counters.update(
        rows_extracted=len(summaries) + refreshed,
        rows_loaded=loaded + refreshed,
        measurements_loaded=loaded
    )


def clear_refresh_run(cursor, run_id):
    # Store details and tags are upserts; replaying re-applies them.
    cursor.execute(
        "DELETE FROM review_observations WHERE run_id = %s", (run_id,)
    )


REFRESH_JOB = Job(
    name="refresh_hourly",
    body=run_refresh,
    clear_run=clear_refresh_run,
    # The run's load time, from its review summaries or from the details
    # rows it still owns (later refreshes overwrite details rows).
    load_time_sql=(
        "SELECT MIN(loaded_at) FROM ("
        " SELECT run_id, collected_at AS loaded_at FROM review_observations"
        " UNION ALL"
        " SELECT run_id, details_fetched_at FROM game_details"
        ") AS loads WHERE run_id = %s"
    )
)
