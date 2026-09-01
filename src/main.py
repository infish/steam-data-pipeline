import logging
import math
from api.steam_api import get_top_games
from database.mysql_database import get_connection
from quality.data_quality import validate_games_data
from transform.data_transformer import transform_games_data
from datetime import datetime, timezone


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None)

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
    error_message=None
):
    query = """
        UPDATE pipeline_runs
        SET
            finished_at = %s,
            status = %s,
            rows_extracted = %s,
            rows_loaded = %s,
            error_message = %s
        WHERE run_id = %s
    """
    cursor.execute(
        query,
        (
            utc_now(),
            status,
            rows_extracted,
            rows_loaded,
            error_message,
            run_id
        )
    )


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

    cursor.executemany(query, values)
    return len(values)


def run_pipeline():
    connection = None
    cursor = None
    run_id = None

    try:
        connection = get_connection()
        cursor = connection.cursor()

        run_id = start_pipeline_run(cursor, utc_now())
        connection.commit()

        data = get_top_games()

        if data is None:
            raise RuntimeError("No API data received")

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
        validate_games_data(df)

        ingestion_time = utc_now()
        rows_loaded = load_games(cursor, df, ingestion_time)
        load_game_metric_snapshots(cursor, df, run_id, ingestion_time)

        finish_pipeline_run(
            cursor,
            run_id,
            "SUCCESS",
            rows_extracted=len(df),
            rows_loaded=rows_loaded
        )
        connection.commit()

        logging.info("Pipeline completed: %s games loaded", rows_loaded)

    except Exception as error:
        logging.exception("Pipeline failed")

        if connection is not None and cursor is not None and run_id is not None:
            connection.rollback()
            finish_pipeline_run(
                cursor,
                run_id,
                "FAILED",
                error_message=str(error)[:1000]
            )
            connection.commit()

        raise

    finally:
        if cursor is not None:
            cursor.close()

        if connection is not None:
            connection.close()

if __name__ == "__main__":
    run_pipeline()
