import time
import mysql.connector

from config import get_mysql_config

def get_connection():
    config = get_mysql_config()

    max_attempts = 20

    for attempt in range(1, max_attempts + 1):

        try:
            connection = mysql.connector.connect(
                host=config["host"],
                user=config["user"],
                password=config["password"],
                database=config["database"]
            )

            return connection

        except mysql.connector.Error as error:
            print(f"MySQL connection failed on attempt {attempt}/{max_attempts}: {error}")
            print("MySQL not ready yet, waiting 5 seconds...")

            time.sleep(5)

    raise Exception("Could not connect to MySQL after retries")
