"""refresh_hourly: Store details, SteamSpy tags and Steam review summaries
for a slice of the catalog every hour (ADR 6).

Spreading the work over the day keeps every run short, so the hourly player
collection is never held up, and keeps the Store requests evenly paced. The
slice sizes follow from the catalog size: every game's Store details are
refreshed every DETAILS_REFRESH_DAYS and its review summary every
REVIEWS_REFRESH_DAYS, within REFRESH_MAX_REQUESTS Store requests per run.
Games are always taken oldest first, so a missed or failed run is caught up
by the next ones.

The review summaries come from the Web API (GetAppReviews), whose limit is
separate from the Store's, so they are fetched on a second thread while the
main thread does the Store details and tags. Each API keeps its own pacer:
neither sees a faster pace than when the two ran one after the other
(docs/optimizations.md, entry 7).
"""

import logging
import math
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

from api.pacing import Pacer
from api.steam_api import API_INTERVALS, get_review_summaries
from config import (
    get_details_refresh_days,
    get_quiet_reviews_refresh_days,
    get_refresh_max_requests,
    get_review_summaries_per_run,
    get_reviews_refresh_days,
    get_store_details_per_run,
    get_tracked_appids,
)
from database.mysql_database import get_connection
from ingest.raw_store import RawStore
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

    A failed attempt counts as the game's turn: a game asked within the
    last active_days without a summary stored since (an error, or an empty
    answer such as Steam gives some removed games) waits for its next turn
    instead of heading the list, with a warning, every hour.
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
    # Failed attempts within the last active_days (matched to their run's
    # stored summaries; the app ID is a request parameter of GetAppReviews
    # or the end of the appreviews endpoint used before v1.2.2). Only runs
    # that succeeded count: a run that failed rolled its summaries back, and
    # its games are caught up by the next runs (ADR 6).
    attempted_since = (
        datetime(9999, 1, 1) if now is None
        else due_by - timedelta(minutes=active_minutes)
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
        ),
        attempts AS (
            SELECT raw.run_id, CAST(COALESCE(
                       JSON_UNQUOTE(
                           JSON_EXTRACT(raw.request_params, '$.appid')
                       ),
                       SUBSTRING_INDEX(raw.endpoint, '/', -1)
                   ) AS UNSIGNED) AS appid
            FROM raw_api_responses AS raw
            JOIN pipeline_runs AS runs ON runs.run_id = raw.run_id
            WHERE raw.source = 'steam_review_summary'
              AND raw.fetched_at >= %s
              AND runs.status = 'SUCCESS'
        ),
        failed AS (
            SELECT DISTINCT attempts.appid
            FROM attempts
            LEFT JOIN review_observations AS loaded
                ON loaded.appid = attempts.appid
               AND loaded.run_id = attempts.run_id
            WHERE loaded.observation_id IS NULL
        )
        SELECT games.appid
        FROM games
        LEFT JOIN state ON state.appid = games.appid
        LEFT JOIN failed ON failed.appid = games.appid
        WHERE games.appid NOT IN ({", ".join(["%s"] * len(excluded))})
          AND (state.due_at IS NULL OR %s IS NULL OR state.due_at <= %s)
          AND failed.appid IS NULL
        ORDER BY state.due_at IS NOT NULL, state.due_at, games.appid
        LIMIT %s
        """,
        (since, quiet_minutes, active_minutes, attempted_since, *excluded,
         due_by, due_by, limit)
    )
    return [row[0] for row in cursor.fetchall()]


def rotation_in_run(cursor, run_id, exclude):
    """The rotated games a past run fetched summaries for (reprocessing)."""
    # The app ID is a request parameter (GetAppReviews, since v1.2.2) or
    # the last part of the endpoint (.../appreviews/<appid>, before).
    cursor.execute(
        """
        SELECT COALESCE(
                   JSON_UNQUOTE(JSON_EXTRACT(request_params, '$.appid')),
                   SUBSTRING_INDEX(endpoint, '/', -1)
               )
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

class ReviewsStopped(Exception):
    """The main thread failed; the review worker stops before its next
    request instead of finishing the slice."""


def fetch_summaries_on_worker(ctx, rotation, stop):
    """Fetch review summaries on a worker thread; return them with timing.

    The worker shares nothing that is not thread-safe with the main thread:
    it has its own HTTP session, its own raw-store connection (raw rows are
    autocommitted, as on the main thread) and its own pacer and wait count.
    It loads nothing; the main thread loads the summaries after joining.
    """
    started_at = ctx.now_fn()
    started = time.perf_counter()
    waited = [0.0]

    def sleep(seconds):
        waited[0] += seconds
        ctx.base_sleep_fn(seconds)

    pacer = Pacer(API_INTERVALS["reviews"], sleep)

    def pace():
        if stop.is_set():
            raise ReviewsStopped()

        pacer.wait()

    session = connection = None

    try:
        session = ctx.session_factory()
        connection = get_connection()
        store = RawStore(connection, ctx.run_id, ctx.now_fn)
        summaries, warnings = get_review_summaries(
            session, rotation, store, pace=pace,
            keep_going=lambda: not ctx.out_of_time()
        )
    finally:
        for closeable in (session, connection):
            if closeable is None:
                continue

            try:
                closeable.close()
            except Exception:
                logging.exception("Could not close %r", closeable)

    timing = {
        "started_at": started_at,
        "duration_ms": round((time.perf_counter() - started) * 1000),
        "wait_ms": round(waited[0] * 1000),
        "requests": sum(store.stats.values()),
    }
    return summaries, warnings, store.stats, timing


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

    # Reprocessing replays stored answers on one connection: one thread.
    parallel = ctx.session_factory is not None and not ctx.replay and rotation
    stop = threading.Event()

    if parallel:
        executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="review-summaries"
        )
        worker = executor.submit(fetch_summaries_on_worker, ctx, rotation, stop)
        executor.shutdown(wait=False)

    try:
        with ctx.step("store_details_and_tags") as step:
            refreshed = refresh_details(ctx, details_count)
            step["rows"] = refreshed
    except BaseException:
        if parallel:
            stop.set()
            worker.exception()  # wait for it; the main error is reported

        raise

    if parallel:
        summaries, warnings, stats, timing = worker.result()
        # The worker's requests count towards the run and its "job" step.
        ctx.store.stats.update(stats)

        if ctx.timer:
            ctx.timer.add_finished("review_summaries", **timing)
    else:
        with ctx.step("review_summaries"):
            summaries, warnings = get_review_summaries(
                ctx.session,
                rotation,
                ctx.store,
                pace=lambda: ctx.pace("reviews"),
                keep_going=lambda: not ctx.out_of_time()
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
    ),
    # Starts at :37 and normally takes 1 to 5 minutes; the next players run
    # is at :07.
    time_limit_minutes=20
)
