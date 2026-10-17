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


def get_store_details_per_run():
    raw_value = get_env("STORE_DETAILS_PER_RUN", "120")

    try:
        value = int(raw_value)
    except ValueError as error:
        raise ValueError(
            "STORE_DETAILS_PER_RUN must be a whole number"
        ) from error

    if not 0 <= value <= 140:
        # 140 keeps one catalog run inside the 150-per-5-minute Store budget.
        raise ValueError("STORE_DETAILS_PER_RUN must be between 0 and 140")

    return value


def get_tag_refresh_days():
    """Days before a game's SteamSpy tags are fetched again (0 = every time).

    SteamSpy has served a frozen snapshot since at least September 2026
    (docs/sources.md), so refetching tags daily only repeats old data.
    """
    raw_value = get_env("TAG_REFRESH_DAYS", "30")

    try:
        value = int(raw_value)
    except ValueError as error:
        raise ValueError("TAG_REFRESH_DAYS must be a whole number") from error

    if value < 0:
        raise ValueError("TAG_REFRESH_DAYS must be 0 or more")

    return value


def get_store_country():
    value = get_env("STEAM_STORE_COUNTRY", "us").strip().lower()

    if len(value) != 2 or not value.isalpha():
        raise ValueError(
            "STEAM_STORE_COUNTRY must be a two-letter country code"
        )

    return value
