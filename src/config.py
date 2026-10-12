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


def get_env(name, default):
    value = os.getenv(name)

    if value is None or value.strip() == "":
        return default

    return value


def get_mysql_config():
    return {
        "host": get_required_env("MYSQL_HOST"),
        "user": get_required_env("MYSQL_USER"),
        "password": get_required_env("MYSQL_PASSWORD"),
        "database": get_required_env("MYSQL_DATABASE")
    }


def get_steamspy_config():
    return {
        "mode": get_env("STEAMSPY_MODE", "all"),
        "page": int(get_env("STEAMSPY_PAGE", "0"))
    }


def get_tracked_appids():
    raw_appids = get_env(
        "STEAM_TRACKED_APPIDS",
        "730,427520,620"
    )

    try:
        appids = [
            int(value.strip())
            for value in raw_appids.split(",")
            if value.strip()
        ]
    except ValueError as error:
        raise ValueError(
            "STEAM_TRACKED_APPIDS must be a comma-separated list of integers"
        ) from error

    if not appids:
        raise ValueError("STEAM_TRACKED_APPIDS must contain at least one app ID")

    return list(dict.fromkeys(appids))


def get_max_rejected_percent():
    raw_value = get_env("PIPELINE_MAX_REJECTED_PERCENT", "5")

    try:
        value = float(raw_value)
    except ValueError as error:
        raise ValueError(
            "PIPELINE_MAX_REJECTED_PERCENT must be a number"
        ) from error

    if not 0 <= value <= 100:
        raise ValueError(
            "PIPELINE_MAX_REJECTED_PERCENT must be between 0 and 100"
        )

    return value


def get_raw_retention_days():
    raw_value = get_env("RAW_RETENTION_DAYS", "30")

    try:
        value = int(raw_value)
    except ValueError as error:
        raise ValueError(
            "RAW_RETENTION_DAYS must be a whole number"
        ) from error

    if value < 1:
        raise ValueError("RAW_RETENTION_DAYS must be at least 1")

    return value


def get_player_hourly_retention_days():
    raw_value = get_env("PLAYER_HOURLY_RETENTION_DAYS", "90")

    try:
        value = int(raw_value)
    except ValueError as error:
        raise ValueError(
            "PLAYER_HOURLY_RETENTION_DAYS must be a whole number"
        ) from error

    if value < 1:
        raise ValueError("PLAYER_HOURLY_RETENTION_DAYS must be at least 1")

    return value
