"""Review histogram rollups, reconciliation and news events.

Used by reviews_daily for the tracked apps and by backfill.py on demand.
"""

import logging

from api.steam_api import (
    ENRICHMENT_ERRORS,
    FailureStreak,
    fetch_news,
    fetch_review_histogram,
    stopped_early,
)
from parsing.reviews import parse_histogram, parse_news, reconcile


DAILY_NEWS_COUNT = 20


def load_review_rollups(cursor, appid, rows, run_id, fetched_at):
    """Upsert rollups; never let an older fetch overwrite a newer one."""
    cursor.executemany(
        """
        INSERT INTO review_rollups (
            appid, rollup_type, period_start, recommendations_up,
            recommendations_down, is_partial, fetched_at, run_id
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        AS new
        ON DUPLICATE KEY UPDATE
            recommendations_up = IF(
                new.fetched_at >= review_rollups.fetched_at,
                new.recommendations_up, review_rollups.recommendations_up),
            recommendations_down = IF(
                new.fetched_at >= review_rollups.fetched_at,
                new.recommendations_down, review_rollups.recommendations_down),
            is_partial = IF(new.fetched_at >= review_rollups.fetched_at,
                new.is_partial, review_rollups.is_partial),
            run_id = IF(new.fetched_at >= review_rollups.fetched_at,
                new.run_id, review_rollups.run_id),
            fetched_at = GREATEST(new.fetched_at, review_rollups.fetched_at)
        """,
        [
            (
                appid,
                row["rollup_type"],
                row["period_start"],
                row["recommendations_up"],
                row["recommendations_down"],
                row["is_partial"],
                fetched_at,
                run_id,
            )
            for row in rows
        ]
    )
    return len(rows)


def load_reconciliation(cursor, appid, result, run_id, checked_at):
    cursor.execute(
        """
        INSERT INTO review_reconciliation (
            run_id, appid, checked_at, histogram_total, summary_total,
            count_difference_percent, histogram_positive_share,
            summary_positive_share, share_difference_points,
            tolerance_points, within_tolerance
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            run_id,
            appid,
            checked_at,
            result["histogram_total"],
            result["summary_total"],
            result["count_difference_percent"],
            result["histogram_positive_share"],
            result["summary_positive_share"],
            result["share_difference_points"],
            result["tolerance_points"],
            result["within_tolerance"],
        )
    )


def load_events(cursor, events, run_id, first_seen_at):
    """Insert events whose gid is new; return how many were new."""
    new = 0

    for event in events:
        cursor.execute(
            """
            INSERT INTO game_events (
                gid, appid, title, url, event_time, feed_label, feed_name,
                feed_type, is_patch_notes, is_developer_post, first_seen_at,
                run_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE gid = gid
            """,
            (
                event["gid"],
                event["appid"],
                event["title"],
                event["url"],
                event["event_time"],
                event["feed_label"],
                event["feed_name"],
                event["feed_type"],
                event["is_patch_notes"],
                event["is_developer_post"],
                first_seen_at,
                run_id,
            )
        )
        new += cursor.rowcount

    return new


def should_stop(ctx, streak, what, done, total):
    """Stop at the run's time limit or after repeated failures (warned)."""
    reason = (
        "run time limit" if ctx.out_of_time()
        else f"{streak.limit} failed requests in a row"
        if streak.tripped else None
    )

    if reason:
        ctx.warn(stopped_early(what, done, total, reason))

    return reason is not None


def collect_histograms(ctx, appids, summaries=None):
    """Fetch and upsert histograms; reconcile with summaries when given."""
    summaries = {row["appid"]: row for row in summaries or []}
    loaded = 0
    streak = FailureStreak()

    for done, appid in enumerate(appids):
        if should_stop(ctx, streak, "review histograms", done, len(appids)):
            break

        ctx.pace("store")

        try:
            rows = parse_histogram(
                fetch_review_histogram(ctx.session, ctx.store, appid),
                ctx.load_time()
            )
            streak.record()
        except ENRICHMENT_ERRORS as error:
            streak.record(error)
            ctx.warn(
                f"Steam review histogram failed for app {appid}, skipped:"
                f" {error}"
            )
            continue

        loaded += load_review_rollups(
            ctx.cursor,
            appid,
            rows,
            ctx.run_id,
            ctx.load_time()
        )

        if appid in summaries:
            result = reconcile(rows, summaries[appid])
            load_reconciliation(
                ctx.cursor,
                appid,
                result,
                ctx.run_id,
                ctx.load_time()
            )

            if not result["within_tolerance"]:
                ctx.warn(
                    f"Review histogram for app {appid} disagrees with the"
                    f" review summary: positive share"
                    f" {result['histogram_positive_share']}% vs"
                    f" {result['summary_positive_share']}%"
                    f" (tolerance {result['tolerance_points']} points)"
                )

    return loaded


def collect_news(ctx, appids, count=DAILY_NEWS_COUNT):
    new_events = 0
    streak = FailureStreak()

    for done, appid in enumerate(appids):
        if should_stop(ctx, streak, "news", done, len(appids)):
            break

        try:
            events = parse_news(
                fetch_news(ctx.session, ctx.store, appid, count)
            )
            streak.record()
        except ENRICHMENT_ERRORS as error:
            streak.record(error)
            ctx.warn(f"Steam news failed for app {appid}, skipped: {error}")
            continue

        new_events += load_events(
            ctx.cursor,
            [event for event in events if event["appid"] == appid],
            ctx.run_id,
            ctx.load_time()
        )

    logging.info("Stored %s new game event(s)", new_events)
    return new_events
