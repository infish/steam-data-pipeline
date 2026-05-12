import pandas as pd
import logging
from api.steam_api import get_top_games
from database.mysql_database import get_connection
from transform.data_transformer import transform_games_data
from datetime import datetime


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="logs/pipeline.log"
)

def run_pipeline():
    connection = get_connection()

    cursor = connection.cursor()
    current_time = datetime.now()

    data = get_top_games()
    if data is None:
        logging.error("Pipeline stopped: no API data received")
        exit()
    games_list = []

    for game_id, game_data in data.items():

        game_info = {
            "appid": int(game_id),
            "name": game_data["name"],
            "owners": game_data["owners"]
        }

        games_list.append(game_info)

    df = transform_games_data(games_list)

    df["owners_low"] = df["owners"].str.split(" .. ").str[0]
    df["owners_high"] = df["owners"].str.split(" .. ").str[1]
    df["owners_low"] = df["owners_low"].str.replace(",", "").astype(int)
    df["owners_high"] = df["owners_high"].str.replace(",", "").astype(int)
    df["estimated_owners"] = (
        df["owners_low"] + df["owners_high"]
    ) / 2

    for game_id, game_data in data.items():

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

        values = (
        int(game_id),
        game_data["name"],
        game_data["owners"],
        int(df.loc[df["appid"] == int(game_id), "owners_low"].iloc[0]),
    int(df.loc[df["appid"] == int(game_id), "owners_high"].iloc[0]),
    int(df.loc[df["appid"] == int(game_id), "estimated_owners"].iloc[0]),
current_time
)

    cursor.execute(query, values)

    connection.commit()

    logging.info("Pipeline completed")

if __name__ == "__main__":
    run_pipeline()