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
        "name": clean_name(data.get("name")),
        "developers": company_list(data.get("developers")),
        "publishers": company_list(data.get("publishers")),
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


def clean_name(value):
    """A Store name, trimmed to the games.name column; None when blank."""
    if not isinstance(value, str) or not value.strip():
        return None

    return value.strip()[:255]


def company_list(values):
    """Store developers or publishers: distinct non-blank names, in order."""
    if not isinstance(values, list):
        return []

    names = []

    for value in values:
        if isinstance(value, str) and value.strip():
            name = value.strip()[:255]

            if name not in names:
                names.append(name)

    return names


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


PRICE_FIELDS = ("currency", "initial_price", "final_price", "discount_percent")


def parse_price_batch(appids, payload):
    """Return {appid: price state} from a filters=price_overview batch.

    Each state has price_state ('priced', 'no_price' or 'unavailable') and,
    when priced, currency and amounts in minor units. A game the Store sells
    without a price comes back as "data": [] (a list), not an object.

    Only an explicit "success": false is 'unavailable'. A game missing from
    the answer gets no state, so its current price stays as it is: the Store
    sometimes answers a whole batch with null, and treating that as 200
    games going unavailable (and back the next day) would write false price
    history. An answer that is not an object raises ValueError, which fails
    the batch with a warning.
    """
    if not isinstance(payload, dict):
        raise ValueError(
            f"Store price batch answer is not an object: {payload!r:.40}"
        )

    states = {}

    for appid in appids:
        entry = payload.get(str(appid))

        if not isinstance(entry, dict) or "success" not in entry:
            continue

        data = entry.get("data")
        price = data.get("price_overview") if isinstance(data, dict) else None

        if not entry.get("success"):
            state = {"price_state": "unavailable"}
        elif not price:
            state = {"price_state": "no_price"}
        else:
            state = {
                "price_state": "priced",
                "currency": price.get("currency"),
                "initial_price": to_int(price.get("initial")),
                "final_price": to_int(price.get("final")),
                "discount_percent": to_int(price.get("discount_percent")),
            }

            if not state["currency"] or state["final_price"] is None:
                raise ValueError(
                    f"Store price for app {appid} has no currency or amount"
                )

        for field in PRICE_FIELDS:
            state.setdefault(field, None)

        states[appid] = state

    return states
