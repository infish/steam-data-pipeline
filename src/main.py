import logging
import math
from api.steam_api import (
    get_steam_measurements,
    get_top_games,
    get_tracked_app_details,
)
from config import get_max_rejected_percent, get_tracked_appids
from database.mysql_database import get_connection
from quality.data_quality import format_rejections, validate_games_data
from transform.data_transformer import transform_games_data
from datetime import datetime, timezone


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

RUN_LOCK_NAME = "steam_pipeline_run"
ABANDONED_RUN_MESSAGE = "Abandoned: process ended before completion"
WARNING_MESSAGE_LIMIT = 2000
LOGGED_REJECTIONS_LIMIT = 20

def utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None)

def acquire_run_lock(cursor):
    cursor.execute("SELECT GET_LOCK(%s, 0)", (RUN_LOCK_NAME,))
    return cursor.fetchall()[0][0] == 1


def release_run_lock(cursor):
    cursor.execute("SELECT RELEASE_LOCK(%s)", (RUN_LOCK_NAME,))
    cursor.fetchall()


def reap_abandoned_runs(cursor):
    # Only safe while holding the run lock: no other run can be live.
    cursor.execute(
        """
        UPDATE pipeline_runs
        SET status = 'FAILED', error_message = %s
        WHERE status = 'RUNNING'
        """,
        (ABANDONED_RUN_MESSAGE,)
    )
    return cursor.rowcount


def start_pipeline_run(cursor, started_at):
    query = """
        INSERT INTO pipeline_runs (
            started_at,
            status
        )
        VALUES (%s, %s)
    """
    cursor.execute(query, (started_at, "RUNNING"))
    return cursor.lastrowid


def finish_pipeline_run(
    cursor,
    run_id,
    status,
    rows_extracted=0,
    rows_loaded=0,
    measurements_loaded=0,
    rows_rejected=0,
    error_message=None,
    warning_message=None
):
    query = """
        UPDATE pipeline_runs
        SET
            finished_at = %s,
            status = %s,
            rows_extracted = %s,
            rows_loaded = %s,
            measurements_loaded = %s,
            rows_rejected = %s,
            error_message = %s,
            warning_message = %s
        WHERE run_id = %s
    """
    cursor.execute(
        query,
        (
            utc_now(),
            status,
            rows_extracted,
            rows_loaded,
            measurements_loaded,
            rows_rejected,
            error_message,
            warning_message,
            run_id
        )
    )


def summarize_warnings(warnings, limit=WARNING_MESSAGE_LIMIT):
    if not warnings:
        return None

    message = "\n".join(warnings)

    if len(message) <= limit:
        return message

    return message[:limit - 1] + "\u2026"


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
            name = VALUES(name),
            developer = VALUES(developer),
            publisher = VALUES(publisher),
            languages = VALUES(languages),
            genre = VALUES(genre),
            last_seen_at = VALUES(last_seen_at)
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


def run_pipeline():
    connection = None
    cursor = None
    run_id = None
    lock_acquired = False

    try:
        connection = get_connection()
        cursor = connection.cursor()

        if not acquire_run_lock(cursor):
            logging.warning(
                "Another pipeline run is active; skipping this run"
            )
            return

        lock_acquired = True

        abandoned_runs = reap_abandoned_runs(cursor)
        connection.commit()

        if abandoned_runs:
            logging.warning(
                "Marked %s abandoned RUNNING pipeline run(s) as FAILED",
                abandoned_runs
            )

        run_id = start_pipeline_run(cursor, utc_now())
        connection.commit()

        data = get_top_games()
        steam_measurements, run_warnings = get_steam_measurements()

        for warning in run_warnings:
            logging.warning(warning)

        if data is None:
            raise RuntimeError("No API data received")

        # Tracked apps need a games row for their measurements' foreign key.
        off_page_appids = [
            appid
            for appid in get_tracked_appids()
            if str(appid) not in data
        ]
        backfilled_rows, backfill_warnings = get_tracked_app_details(
            off_page_appids
        )
        data = {**data, **backfilled_rows}

        for warning in backfill_warnings:
            logging.warning(warning)

        run_warnings.extend(backfill_warnings)

        if backfilled_rows:
            logging.info(
                "Backfilled %s tracked app(s) missing from the SteamSpy page:"
                " %s",
                len(backfilled_rows),
                ", ".join(backfilled_rows)
            )

        games_list = []

        for game_id, game_data in data.items():

            game_info = {
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
            }

            games_list.append(game_info)

        df = transform_games_data(games_list)
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
            run_warnings.append(
                f"Rejected {len(rejected)} of {rows_extracted} SteamSpy "
                "rows that failed validation (app IDs: "
                + ", ".join(str(appid) for appid, _ in rejected[:10])
                + (", ..." if len(rejected) > 10 else "")
                + ")"
            )

        ingestion_time = utc_now()
        rows_loaded = load_games(cursor, df, ingestion_time)
        load_game_metric_snapshots(cursor, df, run_id, ingestion_time)
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
            cursor,
            loadable_measurements,
            run_id,
            ingestion_time
        )

        finish_pipeline_run(
            cursor,
            run_id,
            "SUCCESS",
            rows_extracted=rows_extracted,
            rows_loaded=rows_loaded,
            measurements_loaded=measurements_loaded,
            rows_rejected=len(rejected),
            warning_message=summarize_warnings(run_warnings)
        )
        connection.commit()

        logging.info(
            "Pipeline completed: %s games and %s Steam measurements loaded"
            " (%s warnings)",
            rows_loaded,
            measurements_loaded,
            len(run_warnings)
        )

    except Exception as error:
        logging.exception("Pipeline failed")

        if connection is not None and cursor is not None and run_id is not None:
            try:
                connection.rollback()
                finish_pipeline_run(
                    cursor,
                    run_id,
                    "FAILED",
                    error_message=str(error)[:1000]
                )
                connection.commit()
            except Exception:
                logging.exception(
                    "Could not record pipeline run %s as FAILED",
                    run_id
                )

        raise error

    finally:
        if lock_acquired:
            try:
                release_run_lock(cursor)
            except Exception:
                logging.exception("Could not release the pipeline run lock")

        if cursor is not None:
            cursor.close()

        if connection is not None:
            connection.close()

if __name__ == "__main__":
    run_pipeline()
