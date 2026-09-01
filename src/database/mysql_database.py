import mysql.connector

from config import get_mysql_config


def get_connection():
    config = get_mysql_config()

    return mysql.connector.connect(
        host=config["host"],
        user=config["user"],
        password=config["password"],
        database=config["database"],
        connection_timeout=10
    )