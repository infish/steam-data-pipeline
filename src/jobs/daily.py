"""Daily collection: SteamSpy catalog, tracked-app backfill, Valve data."""

import logging
import math

from api.steam_api import (
    get_steam_measurements,
    get_top_games,
    get_tracked_app_details,
)
from config import (
    get_max_rejected_percent,
    get_raw_retention_days,
    get_tracked_appids,
)
from ingest.raw_store import purge_raw_payloads
from jobs.base import Job
from quality.data_quality import format_rejections, validate_games_data
from transform.data_transformer import transform_games_data


RUN_LOCK_NAME = "steam_pipeline_run"
LOGGED_REJECTIONS_LIMIT = 20


def clean_value(value):
    if value is None:
        return None

    if isinstance(value, float) and math.isnan(value):
        return None

    return value


def clean_int(value):
    value = clean_value(value)

    if value is None:
        return None

    return int(value)


def clean_float(value):
    value = clean_value(value)

    if value is None:
        return None

    return float(value)

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


def load_steam_measurements(cursor, measurements, run_id, collected_at):
    query = """
        INSERT INTO steam_game_measurements (
            run_id,
            appid,
            collected_at,
            source_measured_at,
            current_players,
            player_rank,
            positive_reviews,
            negative_reviews,
            total_reviews,
            review_score_percent
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    values = [
        (
            run_id,
            measurement["appid"],
            collected_at,
            measurement["source_measured_at"],
            measurement["current_players"],
            clean_int(measurement["player_rank"]),
            clean_int(measurement["positive_reviews"]),
            clean_int(measurement["negative_reviews"]),
            clean_int(measurement["total_reviews"]),
            clean_float(measurement["review_score_percent"])
        )
        for measurement in measurements
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


def run_daily(ctx):
    data = get_top_games(ctx.session, ctx.store)
    steam_measurements, valve_warnings = get_steam_measurements(
        ctx.session,
        ctx.store
    )
    ctx.warn(*valve_warnings)

    if data is None:
        raise RuntimeError("No API data received")

    # Tracked apps need a games row for their measurements' foreign key.
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
    known_appids = set(df["appid"].astype(int))
    loadable_measurements = [
        measurement
        for measurement in steam_measurements
        if measurement["appid"] in known_appids
    ]
    skipped_top_100 = sum(
        1
        for measurement in steam_measurements
        if measurement["appid"] not in known_appids
        and measurement["player_rank"] is not None
    )

    if skipped_top_100:
        logging.info(
            "%s top-100 games not in catalog page; skipped",
            skipped_top_100
        )

    measurements_loaded = load_steam_measurements(
        ctx.cursor,
        loadable_measurements,
        ctx.run_id,
        ingestion_time
    )

    ctx.counters.update(
        rows_extracted=rows_extracted,
        rows_loaded=rows_loaded,
        measurements_loaded=measurements_loaded,
        rows_rejected=len(rejected)
    )

    if not ctx.replay:
        purge_expired_payloads(ctx)


def purge_expired_payloads(ctx):
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


def clear_daily_run(cursor, run_id):
    cursor.execute(
        "DELETE FROM steam_game_measurements WHERE run_id = %s",
        (run_id,)
    )
    cursor.execute(
        "DELETE FROM game_metric_snapshots WHERE run_id = %s",
        (run_id,)
    )


DAILY_JOB = Job(
    name="daily",
    lock_name=RUN_LOCK_NAME,
    body=run_daily,
    clear_run=clear_daily_run
)
