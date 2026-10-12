"""Parse Steam Store appdetails responses (see docs/sources.md)."""

import html
import re
from datetime import datetime


# Store release dates are display text in the requested language (l=english).
# Only complete dates are parsed; month-only or free text ("Coming soon",
# "Q1 2027") becomes NULL.
RELEASE_DATE_FORMATS = ("%b %d, %Y", "%d %b, %Y", "%B %d, %Y", "%d %B, %Y")


def parse_release_date(text):
    if not text:
        return None

    cleaned = re.sub(r"\s+", " ", str(text)).strip()

    for date_format in RELEASE_DATE_FORMATS:
        try:
            return datetime.strptime(cleaned, date_format).date()
        except ValueError:
            continue

    return None


def parse_languages(raw):
    """Return [(language, full_audio)] from Store supported_languages HTML.

    The Store marks full audio with <strong>*</strong> after a name and ends
    the list with "<br><strong>*</strong>languages with full audio support".
    """
    if not raw:
        return []

    listing = re.split(r"<br\s*/?>", str(raw), maxsplit=1)[0]
    languages = []
    seen = set()

    for item in listing.split(","):
        full_audio = "*" in re.sub(r"<[^>]+>", "", item)
        name = html.unescape(re.sub(r"<[^>]+>", "", item))
        name = re.sub(r"\s+", " ", name.replace("*", "")).strip()

        if name and name.casefold() not in seen:
            seen.add(name.casefold())
            languages.append((name[:100], full_audio))

    return languages


def to_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def parse_store_details(appid, payload):
    """Return a dict for game_details and its bridge tables.

    available is False when the Store answers success: false (for example a
    delisted or region-locked app); the other fields are then absent.
    """
    entry = (payload or {}).get(str(appid)) or {}

    if not entry.get("success"):
        return {"appid": appid, "available": False}

    data = entry.get("data") or {}
    release = data.get("release_date") or {}
    platforms = data.get("platforms") or {}
    metacritic = data.get("metacritic") or {}
    recommendations = data.get("recommendations") or {}
    release_text = release.get("date") or None

    return {
        "appid": appid,
        "available": True,
        "app_type": data.get("type"),
        "release_date": parse_release_date(release_text),
        "release_date_text": release_text[:100] if release_text else None,
        "coming_soon": release.get("coming_soon"),
        "is_free": data.get("is_free"),
        "required_age": to_int(data.get("required_age")),
        "metacritic_score": to_int(metacritic.get("score")),
        "platform_windows": platforms.get("windows"),
        "platform_mac": platforms.get("mac"),
        "platform_linux": platforms.get("linux"),
        "recommendations_total": to_int(recommendations.get("total")),
        "genres": [
            (to_int(genre.get("id")), genre.get("description"))
            for genre in data.get("genres") or []
            if to_int(genre.get("id")) is not None
        ],
        "categories": [
            (to_int(category.get("id")), category.get("description"))
            for category in data.get("categories") or []
            if to_int(category.get("id")) is not None
        ],
        "languages": parse_languages(data.get("supported_languages")),
        "price_overview": data.get("price_overview"),
    }


def parse_steamspy_tags(payload):
    """Return [(tag, votes)] from a SteamSpy appdetails response."""
    tags = (payload or {}).get("tags") or {}

    if not isinstance(tags, dict):
        return []

    return [
        (str(name)[:100], int(votes))
        for name, votes in tags.items()
        if str(name).strip() and to_int(votes) is not None
    ]
