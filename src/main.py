from api.steam_api import get_top_games
from database.mysql_database import get_connection

connection = get_connection()

cursor = connection.cursor()

data = get_top_games()

for game_id, game_data in data.items():

    query = """
    INSERT INTO games (appid, name, owners)
    VALUES (%s, %s, %s)
    ON DUPLICATE KEY UPDATE
        name = VALUES(name),
        owners = VALUES(owners)
    """

    values = (
        int(game_id),
        game_data["name"],
        game_data["owners"]
    )

    cursor.execute(query, values)

connection.commit()

print("Pipeline completed")