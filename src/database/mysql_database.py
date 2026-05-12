import mysql.connector

def get_connection():
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="E2oQ97w42026!",
        database="steam_pipeline"
    )

    return connection