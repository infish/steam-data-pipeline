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


def optional_count(name, maximum):
    """A whole-number setting that may be unset or empty (None)."""
    raw_value = get_env(name, "").strip()

    if raw_value == "":
        return None

    try:
        value = int(raw_value)
    except ValueError as error:
        raise ValueError(f"{name} must be a whole number") from error

    if not 0 <= value <= maximum:
        raise ValueError(f"{name} must be between 0 and {maximum}")

    return value


def get_store_details_per_run():
    """Override for the Store details refreshed per refresh run, or None.

    Normally unset: refresh_hourly derives the count from the catalog size
    and DETAILS_REFRESH_DAYS (ADR 6).
    """
    return optional_count("STORE_DETAILS_PER_RUN", 300)


def get_refresh_days(name, default):
    """Days in which every catalog game is refreshed once; 0 switches off."""
    raw_value = get_env(name, default)

    try:
        value = float(raw_value)
    except ValueError as error:
        raise ValueError(f"{name} must be a number of days") from error

    if value != 0 and value < 1 / 24:
        raise ValueError(f"{name} must be 0 (off) or at least 1/24 (an hour)")

    return value


def get_details_refresh_days():
    return get_refresh_days("DETAILS_REFRESH_DAYS", "7")


def get_reviews_refresh_days():
    return get_refresh_days("REVIEWS_REFRESH_DAYS", "1")


def get_refresh_max_requests():
    """Ceiling on Store requests per refresh run (300 is about 10.5 min)."""
    raw_value = get_env("REFRESH_MAX_REQUESTS", "300")

    try:
        value = int(raw_value)
    except ValueError as error:
        raise ValueError(
            "REFRESH_MAX_REQUESTS must be a whole number"
        ) from error

    if not 1 <= value <= 1000:
        raise ValueError("REFRESH_MAX_REQUESTS must be between 1 and 1000")

    return value


def get_steamspy_pages():
    """SteamSpy catalog pages fetched, from STEAMSPY_PAGE on (1,000 each)."""
    raw_value = get_env("STEAMSPY_PAGES", "5")

    try:
        value = int(raw_value)
    except ValueError as error:
        raise ValueError("STEAMSPY_PAGES must be a whole number") from error

    if not 1 <= value <= 20:
        raise ValueError("STEAMSPY_PAGES must be between 1 and 20")

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


def get_catalog_from_valve_charts():
    """Whether games seen in Valve's charts join the catalog (ADR 5)."""
    value = get_env("CATALOG_FROM_VALVE_CHARTS", "true").strip().lower()

    if value not in {"true", "false", "1", "0", "yes", "no"}:
        raise ValueError("CATALOG_FROM_VALVE_CHARTS must be true or false")

    return value in {"true", "1", "yes"}


def get_steamspy_page_interval_days():
    """Days between SteamSpy catalog page fetches (1 = every catalog run).

    SteamSpy is frozen (docs/sources.md), so the page is a weekly probe for
    it coming back rather than a daily source.
    """
    raw_value = get_env("STEAMSPY_PAGE_INTERVAL_DAYS", "7")

    try:
        value = int(raw_value)
    except ValueError as error:
        raise ValueError(
            "STEAMSPY_PAGE_INTERVAL_DAYS must be a whole number"
        ) from error

    if value < 1:
        raise ValueError("STEAMSPY_PAGE_INTERVAL_DAYS must be 1 or more")

    return value


def get_review_summaries_per_run():
    """Override for the review summaries fetched per refresh run, or None.

    Normally unset: refresh_hourly derives the count from the catalog size
    and REVIEWS_REFRESH_DAYS (ADR 6).
    """
    return optional_count("REVIEW_SUMMARIES_PER_RUN", 300)


def get_store_country():
    value = get_env("STEAM_STORE_COUNTRY", "us").strip().lower()

    if len(value) != 2 or not value.isalpha():
        raise ValueError(
            "STEAM_STORE_COUNTRY must be a two-letter country code"
        )

    return value
