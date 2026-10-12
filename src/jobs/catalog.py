"""catalog_daily: SteamSpy catalog page plus tracked-app backfill."""

import logging

from api.steam_api import get_top_games, get_tracked_app_details
from config import (
    get_max_rejected_percent,
    get_raw_retention_days,
    get_tracked_appids,
)
from ingest.raw_store import purge_raw_payloads
from jobs.base import Job
from jobs.loading import clean_float, clean_int, clean_value
from quality.data_quality import format_rejections, validate_games_data
from transform.data_transformer import transform_games_data


LOGGED_REJECTIONS_LIMIT = 20


def load_games(cursor, df, observed_at):
    query = """
        INSERT INTO games (
            appid,
            name,
            developer,
            publisher,
            languages,
            genre,
            first_seen_at,
            last_seen_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)

        ON DUPLICATE KEY UPDATE
            name = IF(VALUES(last_seen_at) >= last_seen_at,
                      VALUES(name), name),
            developer = IF(VALUES(last_seen_at) >= last_seen_at,
                           VALUES(developer), developer),
            publisher = IF(VALUES(last_seen_at) >= last_seen_at,
                           VALUES(publisher), publisher),
            languages = IF(VALUES(last_seen_at) >= last_seen_at,
                           VALUES(languages), languages),
            genre = IF(VALUES(last_seen_at) >= last_seen_at,
                       VALUES(genre), genre),
            first_seen_at = LEAST(first_seen_at, VALUES(first_seen_at)),
            last_seen_at = GREATEST(last_seen_at, VALUES(last_seen_at))
    """

    values = [
        (
            int(row.appid),
            row.name,
            clean_value(row.developer),
            clean_value(row.publisher),
            clean_value(row.languages),
            clean_value(row.genre),
            observed_at,
            observed_at
        )
        for row in df.itertuples(index=False)
    ]

    cursor.executemany(query, values)
    return len(values)


def load_game_metric_snapshots(cursor, df, run_id, snapshot_time):
    query = """
        INSERT INTO game_metric_snapshots (
            run_id,
            appid,
            snapshot_time,
            owners,
            owners_low,
            owners_high,
            estimated_owners,
            positive_reviews,
            negative_reviews,
            total_reviews,
            review_score_percent,
            average_playtime_forever_minutes,
            average_playtime_2weeks_minutes,
            median_playtime_forever_minutes,
            median_playtime_2weeks_minutes,
            ccu,
            price_cents,
            initial_price_cents,
            discount_percent
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """

    values = [
        (
            run_id,
            int(row.appid),
            snapshot_time,
            row.owners,
            clean_int(row.owners_low),
            clean_int(row.owners_high),
            clean_int(row.estimated_owners),
            clean_int(row.positive_reviews),
            clean_int(row.negative_reviews),
            clean_int(row.total_reviews),
            clean_float(row.review_score_percent),
            clean_int(row.average_playtime_forever_minutes),
            clean_int(row.average_playtime_2weeks_minutes),
            clean_int(row.median_playtime_forever_minutes),
            clean_int(row.median_playtime_2weeks_minutes),
            clean_int(row.ccu),
            clean_int(row.price_cents),
            clean_int(row.initial_price_cents),
            clean_int(row.discount_percent)
        )
        for row in df.itertuples(index=False)
    ]

    cursor.executemany(query, values)
    return len(values)


def build_games_list(data):
    games_list = []

    for game_id, game_data in data.items():
        games_list.append({
            "appid": int(game_id),
            "name": game_data.get("name"),
            "developer": game_data.get("developer"),
            "publisher": game_data.get("publisher"),
            "owners": game_data.get("owners"),
            "positive_reviews": game_data.get("positive"),
            "negative_reviews": game_data.get("negative"),
            "average_playtime_forever_minutes": game_data.get("average_forever"),
            "average_playtime_2weeks_minutes": game_data.get("average_2weeks"),
            "median_playtime_forever_minutes": game_data.get("median_forever"),
            "median_playtime_2weeks_minutes": game_data.get("median_2weeks"),
            "ccu": game_data.get("ccu"),
            "price_cents": game_data.get("price"),
            "initial_price_cents": game_data.get("initialprice"),
            "discount_percent": game_data.get("discount"),
            "languages": game_data.get("languages"),
            "genre": game_data.get("genre")
        })

    return games_list


def run_catalog(ctx):
    data = get_top_games(ctx.session, ctx.store)

    # Tracked apps need a games row for their observations' foreign keys.
    off_page_appids = [
        appid
        for appid in get_tracked_appids()
        if str(appid) not in data
    ]
    backfilled_rows, backfill_warnings = get_tracked_app_details(
        ctx.session,
        off_page_appids,
        store=ctx.store,
        sleep_fn=ctx.sleep_fn
    )
    data = {**data, **backfilled_rows}
    ctx.warn(*backfill_warnings)

    if backfilled_rows:
        logging.info(
            "Backfilled %s tracked app(s) missing from the SteamSpy page: %s",
            len(backfilled_rows),
            ", ".join(backfilled_rows)
        )

    df = transform_games_data(build_games_list(data))
    rows_extracted = len(df)
    df, rejected = validate_games_data(df, get_max_rejected_percent())

    if rejected:
        logging.warning(
            "Rejected %s of %s SteamSpy rows; first %s: %s",
            len(rejected),
            rows_extracted,
            min(len(rejected), LOGGED_REJECTIONS_LIMIT),
            format_rejections(rejected, LOGGED_REJECTIONS_LIMIT)
        )
        ctx.warnings.append(
            f"Rejected {len(rejected)} of {rows_extracted} SteamSpy "
            "rows that failed validation (app IDs: "
            + ", ".join(str(appid) for appid, _ in rejected[:10])
            + (", ..." if len(rejected) > 10 else "")
            + ")"
        )

    ingestion_time = ctx.load_time()
    rows_loaded = load_games(ctx.cursor, df, ingestion_time)
    load_game_metric_snapshots(ctx.cursor, df, ctx.run_id, ingestion_time)

    ctx.counters.update(
        rows_extracted=rows_extracted,
        rows_loaded=rows_loaded,
        rows_rejected=len(rejected)
    )

    if not ctx.replay:
        purge_expired_payloads(ctx)


def purge_expired_payloads(ctx):
    """Raw retention runs once a day, at the end of the catalog job."""
    retention_days = get_raw_retention_days()

    try:
        purged = purge_raw_payloads(ctx.cursor, retention_days, ctx.now_fn())
    except Exception as error:
        ctx.warn(f"Raw payload retention cleanup failed: {error}")
        return

    if purged:
        logging.info(
            "Purged %s raw payload bodies older than %s days",
            purged,
            retention_days
        )


def clear_catalog_run(cursor, run_id):
    cursor.execute(
        "DELETE FROM game_metric_snapshots WHERE run_id = %s",
        (run_id,)
    )


CATALOG_JOB = Job(
    name="catalog_daily",
    body=run_catalog,
    clear_run=clear_catalog_run,
    load_time_sql=(
        "SELECT MIN(snapshot_time) FROM game_metric_snapshots"
        " WHERE run_id = %s"
    ),
    reaps=("daily_legacy", "daily")
)
