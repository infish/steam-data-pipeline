import os
import time
import mysql.connector

from dotenv import load_dotenv

load_dotenv()

def get_connection():

    for attempt in range(20):

        try:
            connection = mysql.connector.connect(
                host=os.getenv("MYSQL_HOST"),
                user=os.getenv("MYSQL_USER"),
                password=os.getenv("MYSQL_PASSWORD"),
                database=os.getenv("MYSQL_DATABASE")
            )

            return connection

        except mysql.connector.Error as error:
            print(f"MySQL connection failed: {error}")
            print("MySQL not ready yet, waiting...")

            time.sleep(5)

    raise Exception("Could not connect to MySQL after retries")