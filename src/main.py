import logging
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


def load_games(cursor, df, ingestion_time):
    query = """
        INSERT INTO games (
            appid,
            name,
            owners,
            owners_low,
            owners_high,
            estimated_owners,
            ingestion_time
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)

        ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            owners = VALUES(owners),
            owners_low = VALUES(owners_low),
            owners_high = VALUES(owners_high),
            estimated_owners = VALUES(estimated_owners),
            ingestion_time = VALUES(ingestion_time)
    """

    values = [
        (
            int(row.appid),
            row.name,
            row.owners,
            int(row.owners_low),
            int(row.owners_high),
            int(row.estimated_owners),
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

        ensure_pipeline_runs_table(cursor)
        run_id = start_pipeline_run(cursor, datetime.now())
        connection.commit()

        data = get_top_games()

        if data is None:
            raise RuntimeError("No API data received")

        games_list = []

        for game_id, game_data in data.items():

            game_info = {
                "appid": int(game_id),
                "name": game_data["name"],
                "owners": game_data["owners"]
            }

            games_list.append(game_info)

        df = transform_games_data(games_list)
        validate_games_data(df)

        rows_loaded = load_games(cursor, df, datetime.now())

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
