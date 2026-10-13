"""SQL tests run after every mart build.

Each mart is checked for primary-key uniqueness, non-null key columns, at
least one row, and value ranges. An empty mart is a warning (for example
before any news events exist); the other checks fail.
"""

MART_TESTS = {
    "mart_player_trends": {
        "key": ("appid", "observation_date"),
        "ranges": {
            "peak_players": (0, None),
            "avg_players_7d": (0, None),
            "volatility_7d_pct": (0, None),
            "days_in_top100": (0, None),
        },
    },
    "mart_player_peak_hours": {
        "key": ("appid",),
        "ranges": {"typical_peak_hour_prague": (0, 23)},
    },
    "mart_review_trends": {
        "key": ("appid", "observation_date"),
        "ranges": {
            "positive_share_lifetime": (0, 100),
            "positive_share_30d": (0, 100),
            "sentiment_drift_points": (-100, 100),
        },
    },
    "mart_price_status": {
        "key": ("appid", "country_code"),
        "ranges": {
            "discount_percent": (0, 100),
            "above_lowest_pct": (0, None),
        },
    },
    "mart_genre_summary": {
        "key": ("genre_id",),
        "ranges": {
            "game_count": (1, None),
            "avg_review_score": (0, 100),
            "avg_metacritic": (0, 100),
        },
    },
    "mart_tag_summary": {
        "key": ("tag_id",),
        "ranges": {"game_count": (1, None), "avg_review_score": (0, 100)},
    },
    "mart_release_year_summary": {
        "key": ("release_year",),
        "ranges": {
            "release_year": (1970, 2100),
            "avg_review_score": (0, 100),
            "avg_metacritic": (0, 100),
        },
    },
    "mart_engagement": {
        "key": ("appid",),
        "ranges": {
            "rough_peak_to_owners_pct": (0, None),
            "rough_playtime_skew": (0, None),
        },
    },
    "mart_event_impact": {
        "key": ("gid",),
        "ranges": {"days_before": (0, 7), "days_after": (0, 7)},
    },
}


def result(check_name, target, status, observed, threshold):
    return {
        "check_name": check_name,
        "target": target,
        "status": status,
        "observed_value": observed,
        "threshold": threshold,
    }


def scalar(cursor, sql):
    cursor.execute(sql)
    value = cursor.fetchall()[0][0]
    return 0 if value is None else int(value)


def range_condition(column, low, high):
    parts = []

    if low is not None:
        parts.append(f"{column} < {low}")

    if high is not None:
        parts.append(f"{column} > {high}")

    return " OR ".join(parts)


def describe_range(low, high):
    if high is None:
        return f">= {low}"

    if low is None:
        return f"<= {high}"

    return f"{low}..{high}"


def test_mart(cursor, mart, spec):
    key = ", ".join(spec["key"])
    results = []

    rows = scalar(cursor, f"SELECT COUNT(*) FROM {mart}")
    results.append(result(
        "mart_not_empty", mart, "pass" if rows else "warn", rows, "> 0 rows"
    ))

    distinct = scalar(
        cursor,
        f"SELECT COUNT(*) FROM (SELECT DISTINCT {key} FROM {mart}) AS keys_"
    )
    duplicates = rows - distinct
    results.append(result(
        "mart_unique_key", f"{mart}({key})",
        "pass" if duplicates == 0 else "fail", duplicates, "0 duplicates"
    ))

    nulls = scalar(
        cursor,
        f"SELECT COUNT(*) FROM {mart} WHERE "
        + " OR ".join(f"{column} IS NULL" for column in spec["key"])
    )
    results.append(result(
        "mart_not_null_key", f"{mart}({key})",
        "pass" if nulls == 0 else "fail", nulls, "0 nulls"
    ))

    for column, (low, high) in spec["ranges"].items():
        outside = scalar(
            cursor,
            f"SELECT COUNT(*) FROM {mart}"
            f" WHERE {range_condition(column, low, high)}"
        )
        results.append(result(
            "mart_value_range", f"{mart}.{column}",
            "pass" if outside == 0 else "fail", outside,
            f"0 rows outside {describe_range(low, high)}"
        ))

    return results


def run_mart_tests(connection, tests=MART_TESTS):
    cursor = connection.cursor()

    try:
        return [
            check
            for mart, spec in tests.items()
            for check in test_mart(cursor, mart, spec)
        ]
    finally:
        cursor.close()
