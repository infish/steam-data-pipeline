import logging
import math
from api.steam_api import get_top_games
from database.mysql_database import get_connection
from quality.data_quality import validate_games_data
from transform.data_transformer import transform_games_data
from datetime import datetime


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="logs/pipeline.log"
)

def ensure_pipeline_runs_table(cursor):
    query = """
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id INT AUTO_INCREMENT PRIMARY KEY,
            started_at DATETIME NOT NULL,
            finished_at DATETIME,
            status VARCHAR(20) NOT NULL,
            rows_loaded INT DEFAULT 0,
            error_message TEXT
        )
    """
    cursor.execute(query)


def ensure_analysis_views(cursor):
    queries = [
        """
        CREATE OR REPLACE VIEW top_games_by_owners AS
        SELECT
            appid,
            name,
            developer,
            publisher,
            owners,
            estimated_owners,
            review_score_percent,
            ccu,
            price_cents
        FROM games
        ORDER BY estimated_owners DESC
        """,
        """
        CREATE OR REPLACE VIEW top_games_by_review_score AS
        SELECT
            appid,
            name,
            developer,
            publisher,
            total_reviews,
            review_score_percent,
            estimated_owners,
            ccu
        FROM games
        WHERE total_reviews >= 10000
        ORDER BY review_score_percent DESC
        """,
        """
        CREATE OR REPLACE VIEW most_active_games_by_ccu AS
        SELECT
            appid,
            name,
            developer,
            publisher,
            ccu,
            estimated_owners,
            review_score_percent
        FROM games
        WHERE ccu IS NOT NULL
        ORDER BY ccu DESC
        """,
        """
        CREATE OR REPLACE VIEW free_vs_paid_summary AS
        SELECT
            CASE
                WHEN price_cents = 0 THEN 'Free'
                ELSE 'Paid'
            END AS price_type,
            COUNT(*) AS game_count,
            ROUND(AVG(estimated_owners), 0) AS avg_estimated_owners,
            ROUND(AVG(review_score_percent), 2) AS avg_review_score_percent,
            ROUND(AVG(ccu), 0) AS avg_ccu
        FROM games
        GROUP BY price_type
        """,
        """
        CREATE OR REPLACE VIEW publisher_summary AS
        SELECT
            publisher,
            COUNT(*) AS game_count,
            ROUND(AVG(estimated_owners), 0) AS avg_estimated_owners,
            SUM(estimated_owners) AS total_estimated_owners,
            ROUND(AVG(review_score_percent), 2) AS avg_review_score_percent,
            SUM(ccu) AS total_ccu
        FROM games
        WHERE publisher IS NOT NULL
        GROUP BY publisher
        ORDER BY total_estimated_owners DESC
        """
    ]

    for query in queries:
        cursor.execute(query)


def ensure_games_table(cursor):
    query = """
        CREATE TABLE IF NOT EXISTS games (
            appid INT PRIMARY KEY,
            name VARCHAR(255),
            developer VARCHAR(500),
            publisher VARCHAR(500),
            owners VARCHAR(100),
            owners_low BIGINT,
            owners_high BIGINT,
            estimated_owners BIGINT,
            positive_reviews INT,
            negative_reviews INT,
            total_reviews INT,
            review_score_percent DECIMAL(5,2),
            average_playtime_forever_minutes INT,
            average_playtime_2weeks_minutes INT,
            median_playtime_forever_minutes INT,
            median_playtime_2weeks_minutes INT,
            ccu INT,
            price_cents INT,
            initial_price_cents INT,
            discount_percent INT,
            ingestion_time DATETIME
        )
    """
    cursor.execute(query)

    cursor.execute("SHOW COLUMNS FROM games")
    existing_columns = {row[0] for row in cursor.fetchall()}

    columns_to_add = {
        "developer": "VARCHAR(500)",
        "publisher": "VARCHAR(500)",
        "positive_reviews": "INT",
        "negative_reviews": "INT",
        "total_reviews": "INT",
        "review_score_percent": "DECIMAL(5,2)",
        "average_playtime_forever_minutes": "INT",
        "average_playtime_2weeks_minutes": "INT",
        "median_playtime_forever_minutes": "INT",
        "median_playtime_2weeks_minutes": "INT",
        "ccu": "INT",
        "price_cents": "INT",
        "initial_price_cents": "INT",
        "discount_percent": "INT"
    }

    for column_name, column_type in columns_to_add.items():
        if column_name not in existing_columns:
            cursor.execute(f"ALTER TABLE games ADD COLUMN {column_name} {column_type}")


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


def finish_pipeline_run(cursor, run_id, status, rows_loaded=0, error_message=None):
    query = """
        UPDATE pipeline_runs
        SET
            finished_at = %s,
            status = %s,
            rows_loaded = %s,
            error_message = %s
        WHERE run_id = %s
    """
    cursor.execute(
        query,
        (
            datetime.now(),
            status,
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


def load_games(cursor, df, ingestion_time):
    query = """
        INSERT INTO games (
            appid,
            name,
            developer,
            publisher,
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
            discount_percent,
            ingestion_time
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)

        ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            developer = VALUES(developer),
            publisher = VALUES(publisher),
            owners = VALUES(owners),
            owners_low = VALUES(owners_low),
            owners_high = VALUES(owners_high),
            estimated_owners = VALUES(estimated_owners),
            positive_reviews = VALUES(positive_reviews),
            negative_reviews = VALUES(negative_reviews),
            total_reviews = VALUES(total_reviews),
            review_score_percent = VALUES(review_score_percent),
            average_playtime_forever_minutes = VALUES(average_playtime_forever_minutes),
            average_playtime_2weeks_minutes = VALUES(average_playtime_2weeks_minutes),
            median_playtime_forever_minutes = VALUES(median_playtime_forever_minutes),
            median_playtime_2weeks_minutes = VALUES(median_playtime_2weeks_minutes),
            ccu = VALUES(ccu),
            price_cents = VALUES(price_cents),
            initial_price_cents = VALUES(initial_price_cents),
            discount_percent = VALUES(discount_percent),
            ingestion_time = VALUES(ingestion_time)
    """

    values = [
        (
            int(row.appid),
            row.name,
            clean_value(row.developer),
            clean_value(row.publisher),
            row.owners,
            int(row.owners_low),
            int(row.owners_high),
            int(row.estimated_owners),
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
            clean_int(row.discount_percent),
            ingestion_time
        )
        for row in df.itertuples(index=False)
    ]

    cursor.executemany(query, values)
    return len(values)


def run_pipeline():
    connection = None
    cursor = None
    run_id = None

    try:
        connection = get_connection()
        cursor = connection.cursor()

        ensure_games_table(cursor)
        ensure_pipeline_runs_table(cursor)
        ensure_analysis_views(cursor)
        run_id = start_pipeline_run(cursor, datetime.now())
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
                "discount_percent": game_data.get("discount")
            }

            games_list.append(game_info)

        df = transform_games_data(games_list)
        validate_games_data(df)

        rows_loaded = load_games(cursor, df, datetime.now())
        ensure_analysis_views(cursor)

        finish_pipeline_run(cursor, run_id, "SUCCESS", rows_loaded)
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
