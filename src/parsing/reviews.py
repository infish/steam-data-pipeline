"""Parse the Steam review histogram and news (see docs/sources.md)."""

from datetime import datetime, timezone


# Phase 10 found share differences of at most 0.3 points between the
# histogram and the review summary; 2 points leaves room for drift while
# still catching a broken endpoint.
SHARE_TOLERANCE_POINTS = 2.0

MAX_TITLE_LENGTH = 500
MAX_URL_LENGTH = 1000


def utc_date(timestamp):
    return datetime.fromtimestamp(int(timestamp), timezone.utc).date()


def parse_histogram(payload, fetched_at):
    """Return review_rollups rows (without appid and run_id).

    Monthly buckets come from results.rollups, daily ones from
    results.recent. A bucket that contains the fetch date is partial.
    """
    if not payload or payload.get("success") != 1:
        raise ValueError("Steam review histogram request was not successful")

    results = payload.get("results") or {}
    rollup_type = results.get("rollup_type")

    if rollup_type != "month":
        raise ValueError(
            f"Steam review histogram has rollup_type {rollup_type!r},"
            " expected 'month'"
        )

    fetched_date = fetched_at.date()
    rows = []

    for bucket_type, key in (("month", "rollups"), ("day", "recent")):
        for bucket in results.get(key) or []:
            start = utc_date(bucket["date"])
            partial = (
                (start.year, start.month)
                == (fetched_date.year, fetched_date.month)
                if bucket_type == "month"
                else start == fetched_date
            )
            rows.append({
                "rollup_type": bucket_type,
                "period_start": start,
                "recommendations_up": int(bucket["recommendations_up"]),
                "recommendations_down": int(bucket["recommendations_down"]),
                "is_partial": partial,
            })

    if not any(row["rollup_type"] == "month" for row in rows):
        raise ValueError("Steam review histogram has no monthly rollups")

    return rows


def share(positive, total):
    return round(positive / total * 100, 2) if total else None


def reconcile(histogram_rows, summary):
    """Compare monthly histogram totals with a review summary."""
    months = [row for row in histogram_rows if row["rollup_type"] == "month"]
    up = sum(row["recommendations_up"] for row in months)
    histogram_total = up + sum(row["recommendations_down"] for row in months)
    summary_total = summary["total_reviews"]
    histogram_share = share(up, histogram_total)
    summary_share = share(summary["positive_reviews"], summary_total)
    share_difference = (
        round(abs(histogram_share - summary_share), 2)
        if None not in (histogram_share, summary_share) else None
    )

    return {
        "histogram_total": histogram_total,
        "summary_total": summary_total,
        "count_difference_percent": (
            round((histogram_total - summary_total) / summary_total * 100, 2)
            if summary_total else None
        ),
        "histogram_positive_share": histogram_share,
        "summary_positive_share": summary_share,
        "share_difference_points": share_difference,
        "tolerance_points": SHARE_TOLERANCE_POINTS,
        "within_tolerance": (
            share_difference is not None
            and share_difference <= SHARE_TOLERANCE_POINTS
        ),
    }


def parse_news(payload):
    """Return game_events rows (without first_seen_at and run_id)."""
    news = (payload or {}).get("appnews")

    if not isinstance(news, dict):
        raise ValueError("Steam news response has no appnews object")

    events = []

    for item in news.get("newsitems") or []:
        tags = item.get("tags") or []
        feed_type = item.get("feed_type")
        events.append({
            "gid": str(item["gid"]),
            "appid": int(item["appid"]),
            "title": (item.get("title") or "(untitled)")[:MAX_TITLE_LENGTH],
            "url": (item.get("url") or None) and item["url"][:MAX_URL_LENGTH],
            "event_time": datetime.fromtimestamp(
                int(item["date"]), timezone.utc
            ).replace(tzinfo=None),
            "feed_label": (item.get("feedlabel") or "")[:100] or None,
            "feed_name": (item.get("feedname") or "")[:100] or None,
            "feed_type": feed_type,
            "is_patch_notes": "patchnotes" in tags,
            "is_developer_post": feed_type == 1,
        })

    return events
