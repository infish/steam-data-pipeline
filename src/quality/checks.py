"""Data observability checks, run after each job.

Each check returns result dicts (check_name, target, status, observed_value,
threshold, details, job). Status is pass, warn or fail. A failure never
deletes data: every result is stored in data_quality_results, and run_job
turns failures of the run's own checks into run warnings. Thresholds are
documented in docs/observability.md.

Every check belongs to the job that produces its data (job), or to no job
(None) when it guards the pipeline as a whole. After a job, only its own
checks and the global ones run, so one stale source is not re-reported by
every other job's runs. Freshness is the exception: it runs after every job
as a watchdog, so a job that stops running is still noticed.
"""

import json
import statistics
from datetime import timedelta

from config import get_steamspy_page_interval_days
from ingest.raw_store import read_payload
from jobs.timing import no_step


# Expected interval between successful fetches, in hours. Freshness fails at
# 1.5 times the interval. Sources fetched only on demand are not listed:
# valve_current_players (tracked games outside the top 100) and
# steamspy_appdetails (backfill and tag refreshes).
EXPECTED_INTERVAL_HOURS = {
    "valve_top_concurrent": 1,
    "valve_most_played": 1,
    "steamspy_all": 24,
    # refresh_hourly fetches a slice of Store details and review summaries
    # every hour (ADR 6).
    "store_appdetails": 1,
    "store_prices": 24,
    "steam_review_summary": 1,
    "steam_review_histogram": 24,
    "steam_news": 24,
}
FRESHNESS_FACTOR = 1.5


def expected_interval_hours(source):
    """A source's expected interval; the SteamSpy page follows
    STEAMSPY_PAGE_INTERVAL_DAYS (weekly by default, ADR 5)."""
    if source == "steamspy_all":
        return 24 * get_steamspy_page_interval_days()

    return EXPECTED_INTERVAL_HOURS[source]

# The job that fetches each source.
SOURCE_JOBS = {
    "steamspy_all": "catalog_daily",
    "steamspy_appdetails": "refresh_hourly",
    "store_appdetails": "refresh_hourly",
    "store_prices": "catalog_daily",
    "valve_top_concurrent": "players_hourly",
    "valve_most_played": "players_hourly",
    "valve_current_players": "players_hourly",
    "steam_review_summary": "refresh_hourly",
    "steam_review_histogram": "reviews_daily",
    "steam_news": "reviews_daily",
}

# Tables whose daily row counts are compared with the previous 7 days:
# table -> (time column, owning job; None checks after every job).
# game_metric_snapshots is not here: SteamSpy is fetched weekly (ADR 5).
VOLUME_TABLES = {
    "player_observations": ("collected_at", "players_hourly"),
    "review_observations": ("collected_at", "refresh_hourly"),
    "raw_api_responses": ("fetched_at", None),
    "data_quality_results": ("checked_at", None),
    "run_steps": ("started_at", None),
}
VOLUME_TOLERANCE_PERCENT = 30
VOLUME_MIN_HISTORY_DAYS = 3

# SteamSpy fields compared between the last two catalog snapshots.
SNAPSHOT_FIELDS = (
    "owners_low",
    "owners_high",
    "positive_reviews",
    "negative_reviews",
    "average_playtime_forever_minutes",
    "average_playtime_2weeks_minutes",
    "median_playtime_forever_minutes",
    "median_playtime_2weeks_minutes",
    "ccu",
    "price_cents",
    "initial_price_cents",
    "discount_percent",
)
# 95% or more unchanged is a warn: SteamSpy is frozen. It has served the
# same snapshot since at least September 2026 (docs/sources.md), which the
# pipeline cannot fix, so the check no longer fails runs; it reports since
# when SteamSpy has been frozen and notices when its values change again.
STALENESS_WARN_PERCENT = 95

# Share of the last 24 hours' responses that repeated the previous body.
# Sources not listed are recorded for information: repeats are normal for
# them (prices and the daily leaderboard rarely change between fetches).
DUPLICATE_WARN_PERCENT = {
    "valve_top_concurrent": 50,
    "steamspy_all": 50,
    "steam_review_histogram": 50,
}

# Fields each source's newest payload must contain (docs/sources.md).
# "a.b" walks objects, "x[]" checks every list element, "*" every value
# of an object keyed by app ID.
REQUIRED_FIELDS = {
    "steamspy_all": [
        f"*.{field}" for field in (
            "appid", "name", "developer", "publisher", "owners", "positive",
            "negative", "average_forever", "average_2weeks",
            "median_forever", "median_2weeks", "ccu", "price",
            "initialprice", "discount",
        )
    ],
    "steamspy_appdetails": ["appid", "name", "tags"],
    "valve_top_concurrent": [
        "response.last_update",
        "response.ranks[].rank",
        "response.ranks[].appid",
        "response.ranks[].concurrent_in_game",
    ],
    "valve_most_played": [
        "response.rollup_date",
        "response.ranks[].rank",
        "response.ranks[].appid",
        "response.ranks[].peak_in_game",
    ],
    "valve_current_players": ["response.player_count", "response.result"],
    "steam_review_summary": [
        "success",
        "query_summary.total_positive",
        "query_summary.total_negative",
        "query_summary.total_reviews",
        "query_summary.review_score_desc",
    ],
    "steam_review_histogram": [
        "success",
        "results.rollup_type",
        "results.rollups[].date",
        "results.rollups[].recommendations_up",
        "results.rollups[].recommendations_down",
        "results.recent[].date",
    ],
    "store_appdetails": ["*.success"],
    "store_prices": ["*.success"],
    "steam_news": [
        "appnews.newsitems[].gid",
        "appnews.newsitems[].appid",
        "appnews.newsitems[].title",
        "appnews.newsitems[].date",
        "appnews.newsitems[].feed_type",
    ],
}

# Invariants: (check, target, failing status, owning job, SQL counting
# violating rows; %(now)s is the check time).
INVARIANTS = [
    (
        "consistent_daily_rollup", "player_daily", "fail", "players_hourly",
        """SELECT COUNT(*) FROM player_daily
           WHERE NOT (min_players <= avg_players
                      AND avg_players <= peak_players
                      AND observation_count >= 1)""",
    ),
    (
        "recent_leaderboard_day", "most_played_daily", "warn",
        "players_hourly",
        """SELECT IF(MAX(rollup_date) IS NULL, 0,
                     DATEDIFF(%(now)s, MAX(rollup_date)) > 2)
           FROM most_played_daily""",
    ),
    (
        "catalog_games_have_developers", "game_developers", "fail",
        "catalog_daily",
        """SELECT COUNT(*) FROM current_game_metrics AS games
           WHERE TRIM(COALESCE(games.developer, '')) <> ''
             AND NOT EXISTS (SELECT 1 FROM game_details AS details
                             WHERE details.appid = games.appid
                               AND details.companies_from_store)
             AND NOT EXISTS (SELECT 1 FROM game_developers AS links
                             WHERE links.appid = games.appid)""",
    ),
    (
        "catalog_games_have_publishers", "game_publishers", "fail",
        "catalog_daily",
        """SELECT COUNT(*) FROM current_game_metrics AS games
           WHERE TRIM(COALESCE(games.publisher, '')) <> ''
             AND NOT EXISTS (SELECT 1 FROM game_details AS details
                             WHERE details.appid = games.appid
                               AND details.companies_from_store)
             AND NOT EXISTS (SELECT 1 FROM game_publishers AS links
                             WHERE links.appid = games.appid)""",
    ),
    (
        # Games added from Valve's charts or tracked get their Store details
        # (and real name) from catalog_daily within a few days.
        "new_games_have_store_details", "games", "warn", "refresh_hourly",
        """SELECT COUNT(*) FROM games
           WHERE catalog_source <> 'steamspy'
             AND first_seen_at < %(now)s - INTERVAL 3 DAY
             AND NOT EXISTS (SELECT 1 FROM game_details AS details
                             WHERE details.appid = games.appid)""",
    ),
    (
        "named_companies", "companies", "fail", "refresh_hourly",
        "SELECT COUNT(*) FROM companies WHERE TRIM(name) = ''",
    ),
    (
        "available_details_have_type", "game_details", "fail",
        "refresh_hourly",
        """SELECT COUNT(*) FROM game_details
           WHERE store_available AND app_type IS NULL""",
    ),
    (
        "described_genres", "genres", "fail", "refresh_hourly",
        "SELECT COUNT(*) FROM genres WHERE TRIM(description) = ''",
    ),
    (
        "described_categories", "categories", "fail", "refresh_hourly",
        "SELECT COUNT(*) FROM categories WHERE TRIM(description) = ''",
    ),
    (
        "named_languages", "languages", "fail", "refresh_hourly",
        "SELECT COUNT(*) FROM languages WHERE TRIM(name) = ''",
    ),
    (
        "named_tags", "tags", "fail", "refresh_hourly",
        "SELECT COUNT(*) FROM tags WHERE TRIM(name) = ''",
    ),
    (
        "genre_links_from_details", "game_genres", "fail", "refresh_hourly",
        """SELECT COUNT(*) FROM game_genres AS links
           WHERE NOT EXISTS (SELECT 1 FROM game_details AS details
                             WHERE details.appid = links.appid)""",
    ),
    (
        "category_links_from_details", "game_categories", "fail",
        "refresh_hourly",
        """SELECT COUNT(*) FROM game_categories AS links
           WHERE NOT EXISTS (SELECT 1 FROM game_details AS details
                             WHERE details.appid = links.appid)""",
    ),
    (
        "language_links_from_details", "game_languages", "fail",
        "refresh_hourly",
        """SELECT COUNT(*) FROM game_languages AS links
           WHERE NOT EXISTS (SELECT 1 FROM game_details AS details
                             WHERE details.appid = links.appid)""",
    ),
    (
        "tag_links_have_fetch_time", "game_tags", "fail", "refresh_hourly",
        """SELECT COUNT(DISTINCT links.appid) FROM game_tags AS links
           JOIN game_details AS details ON details.appid = links.appid
           WHERE details.tags_fetched_at IS NULL""",
    ),
    (
        "one_current_price_version", "game_price_history", "fail",
        "catalog_daily",
        """SELECT COUNT(*) FROM (
               SELECT appid, country_code
               FROM game_price_history
               GROUP BY appid, country_code
               HAVING SUM(is_current) <> 1
           ) AS broken""",
    ),
    (
        "ordered_price_versions", "game_price_history", "fail",
        "catalog_daily",
        """SELECT COUNT(*) FROM game_price_history
           WHERE valid_to < valid_from""",
    ),
    (
        "single_partial_month", "review_rollups", "fail", "reviews_daily",
        """SELECT COUNT(*) FROM (
               SELECT appid FROM review_rollups
               WHERE rollup_type = 'month' AND is_partial
               GROUP BY appid HAVING COUNT(*) > 1
           ) AS broken""",
    ),
    (
        "latest_reconciliation_within_tolerance", "review_reconciliation",
        "warn", "reviews_daily",
        """SELECT COUNT(*) FROM review_reconciliation AS rec
           WHERE NOT rec.within_tolerance
             AND rec.run_id = (SELECT MAX(run_id) FROM review_reconciliation
                               WHERE appid = rec.appid)""",
    ),
    (
        "valid_events", "game_events", "fail", "reviews_daily",
        """SELECT COUNT(*) FROM game_events
           WHERE TRIM(title) = ''
              OR event_time > %(now)s + INTERVAL 1 DAY""",
    ),
    (
        "no_stuck_runs", "pipeline_runs", "fail", None,
        """SELECT COUNT(*) FROM pipeline_runs
           WHERE status = 'RUNNING'
             AND started_at < %(now)s - INTERVAL 6 HOUR""",
    ),
]


def result(check_name, target, status, observed, threshold, details=None,
           job=None):
    return {
        "check_name": check_name,
        "target": target,
        "status": status,
        "observed_value": None if observed is None else str(observed),
        "threshold": threshold,
        "details": details,
        "job": job,
    }


def runs_after(owner, job_name):
    """Whether a check owned by owner runs after job_name.

    Global checks (owner None) always run; job_name None runs everything.
    """
    return owner is None or job_name is None or owner == job_name


def scalar(cursor, sql, params=None):
    cursor.execute(sql, params or {})
    value = cursor.fetchall()[0][0]
    return value


# ---- Freshness -------------------------------------------------------------

def evaluate_freshness(source, newest, now):
    interval = expected_interval_hours(source)
    limit_hours = interval * FRESHNESS_FACTOR
    threshold = f"newest success younger than {limit_hours:g} h"

    job = SOURCE_JOBS[source]

    if newest is None:
        return result(
            "freshness", source, "warn", "never fetched", threshold,
            "No successful response yet; expected after the first run of"
            " the job that collects this source.",
            job
        )

    age_hours = round((now - newest).total_seconds() / 3600, 2)
    status = "pass" if age_hours <= limit_hours else "fail"
    return result(
        "freshness", source, status, f"{age_hours} h", threshold, job=job
    )


def freshness_checks(cursor, now):
    cursor.execute(
        """
        SELECT source, MAX(fetched_at)
        FROM raw_api_responses
        WHERE http_status = 200
        GROUP BY source
        """
    )
    newest = dict(cursor.fetchall())
    return [
        evaluate_freshness(source, newest.get(source), now)
        for source in EXPECTED_INTERVAL_HOURS
    ]


# ---- Volume ----------------------------------------------------------------

def evaluate_volume(table, day, counts, job=None):
    """Compare a complete day's rows with the median of the 7 days before.

    Returns None when fewer than VOLUME_MIN_HISTORY_DAYS earlier days have
    rows, because a median of one or two days is not a baseline.
    """
    history = [
        counts[day - timedelta(days=offset)]
        for offset in range(1, 8)
        if counts.get(day - timedelta(days=offset))
    ]

    if len(history) < VOLUME_MIN_HISTORY_DAYS:
        return None

    baseline = statistics.median(history)
    observed = counts.get(day, 0)
    deviation = round((observed - baseline) / baseline * 100, 1)
    threshold = (
        f"within ±{VOLUME_TOLERANCE_PERCENT}% of the 7-day median"
        f" ({baseline:g})"
    )

    if observed == 0:
        status = "fail"
    elif abs(deviation) > VOLUME_TOLERANCE_PERCENT:
        status = "warn"
    else:
        status = "pass"

    return result(
        "volume", table, status, f"{observed} rows ({deviation:+}%)",
        threshold, f"UTC day {day.isoformat()}", job
    )


def volume_checks(cursor, now, job_name=None):
    # The last complete UTC day: today's partial count would always look low.
    day = now.date() - timedelta(days=1)
    results = []

    for table, (column, owner) in VOLUME_TABLES.items():
        if not runs_after(owner, job_name):
            continue

        cursor.execute(
            f"""
            SELECT DATE({column}), COUNT(*)
            FROM {table}
            WHERE {column} >= %s AND {column} < %s
            GROUP BY DATE({column})
            """,
            (day - timedelta(days=7), day + timedelta(days=1))
        )
        counts = dict(cursor.fetchall())
        checked = evaluate_volume(table, day, counts, owner)

        if checked is not None:
            results.append(checked)

    return results


# ---- Staleness -------------------------------------------------------------

def evaluate_staleness(compared_values, unchanged_values, frozen_since=None):
    """frozen_since: when the current run of frozen results began, if the
    previous result was frozen too (None otherwise)."""
    percent = round(unchanged_values / compared_values * 100, 2)
    threshold = f"warn at {STALENESS_WARN_PERCENT}% unchanged (frozen)"
    status = "warn" if percent >= STALENESS_WARN_PERCENT else "pass"
    details = (
        f"{unchanged_values} of {compared_values} values unchanged since the"
        " previous catalog run"
    )

    if status == "warn" and frozen_since is not None:
        details += f"; frozen since at least {frozen_since:%Y-%m-%d}"
    elif status == "warn":
        details += "; frozen"
    elif frozen_since is not None:
        details += (
            f"; changing again after being frozen since"
            f" {frozen_since:%Y-%m-%d}"
        )

    return result(
        "steamspy_unchanged_fields", "game_metric_snapshots", status,
        f"{percent}%", threshold, details, "catalog_daily"
    )


def frozen_since(cursor):
    """When the latest unbroken run of frozen staleness results began.

    None when the latest stored result passed or there is none.
    """
    cursor.execute(
        """
        SELECT status, checked_at FROM data_quality_results
        WHERE check_name = 'steamspy_unchanged_fields'
        ORDER BY result_id DESC
        LIMIT 1000
        """
    )
    since = None

    for status, checked_at in cursor.fetchall():
        if status == "pass":
            break

        since = checked_at

    return since


def staleness_checks(cursor, now, job_name=None):
    results = []

    if not runs_after("catalog_daily", job_name):
        return results + duplicate_rate_checks(cursor, now, job_name)

    cursor.execute(
        """
        SELECT run_id FROM pipeline_runs
        WHERE status = 'SUCCESS'
          AND job_name IN ('catalog_daily', 'daily_legacy')
        ORDER BY run_id DESC
        LIMIT 2
        """
    )
    runs = [row[0] for row in cursor.fetchall()]

    if len(runs) == 2:
        comparisons = " + ".join(
            f"(latest.{field} <=> previous.{field})"
            for field in SNAPSHOT_FIELDS
        )
        cursor.execute(
            f"""
            SELECT COUNT(*), COALESCE(SUM({comparisons}), 0)
            FROM game_metric_snapshots AS latest
            JOIN game_metric_snapshots AS previous
                ON previous.appid = latest.appid
            WHERE latest.run_id = %s AND previous.run_id = %s
            """,
            (runs[0], runs[1])
        )
        games, unchanged = cursor.fetchall()[0]

        if games:
            results.append(evaluate_staleness(
                int(games) * len(SNAPSHOT_FIELDS),
                int(unchanged),
                frozen_since(cursor)
            ))

    return results + duplicate_rate_checks(cursor, now, job_name)


def duplicate_rate_checks(cursor, now, job_name=None):
    results = []
    cursor.execute(
        """
        SELECT source, COUNT(*), SUM(is_duplicate_of IS NOT NULL)
        FROM raw_api_responses
        WHERE http_status = 200 AND fetched_at >= %s
        GROUP BY source
        """,
        (now - timedelta(hours=24),)
    )

    for source, total, duplicates in cursor.fetchall():
        owner = SOURCE_JOBS.get(source)

        if not runs_after(owner, job_name):
            continue

        rate = round(int(duplicates) / int(total) * 100, 1)
        limit = DUPLICATE_WARN_PERCENT.get(source)
        status = "warn" if limit is not None and rate > limit else "pass"
        results.append(result(
            "raw_duplicate_rate", source, status,
            f"{rate}% of {total}",
            f"warn above {limit}%" if limit is not None
            else "informational; repeats are normal for this source",
            job=owner
        ))

    return results


# ---- Schema drift ----------------------------------------------------------

def missing_paths(payload, paths):
    """Return the required paths absent from a payload."""
    return [
        path for path in paths
        if not path_present(payload, path.split("."))
    ]


def path_present(value, parts):
    if not parts:
        return True

    part, rest = parts[0], parts[1:]

    if part == "*":
        return isinstance(value, dict) and all(
            path_present(item, rest) for item in value.values()
        )

    if part.endswith("[]"):
        items = value.get(part[:-2]) if isinstance(value, dict) else None
        return isinstance(items, list) and all(
            path_present(item, rest) for item in items
        )

    return isinstance(value, dict) and part in value and path_present(
        value[part], rest
    )


def schema_drift_checks(connection, cursor, job_name=None):
    results = []

    for source, paths in REQUIRED_FIELDS.items():
        if not runs_after(SOURCE_JOBS[source], job_name):
            continue

        cursor.execute(
            """
            SELECT response_id FROM raw_api_responses
            WHERE source = %s
              AND http_status = 200
              AND payload_purged_at IS NULL
            ORDER BY fetched_at DESC, response_id DESC
            LIMIT 1
            """,
            (source,)
        )
        rows = cursor.fetchall()

        if not rows:
            continue

        try:
            payload = json.loads(read_payload(connection, rows[0][0]))
        except (LookupError, ValueError) as error:
            results.append(result(
                "schema_drift", source, "fail", "unreadable",
                "newest payload parses", str(error), SOURCE_JOBS[source]
            ))
            continue

        missing = missing_paths(payload, paths)
        results.append(result(
            "schema_drift", source, "fail" if missing else "pass",
            f"{len(missing)} missing", "0 required fields missing",
            ", ".join(missing) or None, SOURCE_JOBS[source]
        ))

    return results


# ---- Invariants ------------------------------------------------------------

def invariant_checks(cursor, now, job_name=None):
    results = []

    for check_name, target, failing_status, owner, sql in INVARIANTS:
        if not runs_after(owner, job_name):
            continue

        violations = int(scalar(cursor, sql, {"now": now}) or 0)
        results.append(result(
            check_name, target,
            "pass" if violations == 0 else failing_status,
            violations, "0 violating rows", job=owner
        ))

    return results


# ---- Running and storing ---------------------------------------------------

def run_quality_checks(connection, now, job_name=None, step=no_step):
    """Run the checks for job_name: its own, the global ones and freshness.

    job_name None runs every check. step times each group (jobs.timing).
    """
    cursor = connection.cursor()
    results = []

    try:
        with step("freshness"):
            results += freshness_checks(cursor, now)

        with step("volume"):
            results += volume_checks(cursor, now, job_name)

        with step("staleness"):
            results += staleness_checks(cursor, now, job_name)

        with step("schema_drift"):
            results += schema_drift_checks(connection, cursor, job_name)

        with step("invariants"):
            results += invariant_checks(cursor, now, job_name)

        return results
    finally:
        cursor.close()


def store_results(cursor, results, run_id, checked_at):
    cursor.executemany(
        """
        INSERT INTO data_quality_results (
            check_name, target, run_id, status, observed_value, threshold,
            details, checked_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        [
            (
                row["check_name"],
                row["target"][:150],
                run_id,
                row["status"],
                row["observed_value"],
                (row["threshold"] or "")[:150] or None,
                row.get("details"),
                checked_at,
            )
            for row in results
        ]
    )


def reported_by(row, job_name):
    """Whether a failed result becomes a warning on job_name's run.

    Freshness of another job's source is stored but not reported: the run
    did not cause it, and the dashboard shows it as a failing check.
    """
    return runs_after(row.get("job"), job_name) and not (
        row["check_name"] == "freshness"
        and job_name is not None
        and row.get("job") != job_name
    )


def describe_failure(row):
    return (
        f"Data quality check {row['check_name']} failed on {row['target']}:"
        f" observed {row['observed_value']}, expected {row['threshold']}"
    )
