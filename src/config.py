import os
from pathlib import Path

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env.local")


def get_required_env(name):
    value = os.getenv(name)

    if value is None or value.strip() == "":
        raise ValueError(f"Missing required environment variable: {name}")

    return value


def get_mysql_config():
    return {
        "host": get_required_env("MYSQL_HOST"),
        "user": get_required_env("MYSQL_USER"),
        "password": get_required_env("MYSQL_PASSWORD"),
        "database": get_required_env("MYSQL_DATABASE")
    }
