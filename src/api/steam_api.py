import requests
from datetime import datetime, timezone
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import get_steamspy_config, get_tracked_appids


STEAMSPY_URL = "https://steamspy.com/api.php"
STEAM_CURRENT_PLAYERS_URL = (
    "https://api.steampowered.com/"
    "ISteamUserStats/GetNumberOfCurrentPlayers/v1/"
)
STEAM_TOP_PLAYERS_URL = (
    "https://api.steampowered.com/"
    "ISteamChartsService/GetGamesByConcurrentPlayers/v1/"
)
STEAM_REVIEWS_URL = "https://store.steampowered.com/appreviews/{appid}"


def create_session():
    retry_policy = Retry(
        total=4,
        connect=4,
        read=4,
        status=4,
        backoff_factor=2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True
    )

    session = requests.Session()
    session.mount(
        "https://",
        HTTPAdapter(max_retries=retry_policy)
    )

    return session


def get_top_games():
    config = get_steamspy_config()

    params = {
        "request": config["mode"]
    }

    if config["mode"] == "all":
        params["page"] = config["page"]

    with create_session() as session:
        response = session.get(
            STEAMSPY_URL,
            params=params,
            timeout=(5, 45)
        )
        response.raise_for_status()
        data = response.json()

    if not isinstance(data, dict) or not data:
        raise ValueError("SteamSpy returned an empty or unexpected response")

    return data


# Valve data enriches the SteamSpy catalog. These failures degrade a run
# into warnings instead of failing it.
ENRICHMENT_ERRORS = (requests.RequestException, ValueError, KeyError)

REVIEW_PARAMS = {
    "json": 1,
    "filter": "recent",
    "language": "all",
    "review_type": "all",
    "purchase_type": "all",
    "num_per_page": 1,
    "filter_offtopic_activity": 0
}


def empty_reviews():
    return {
        "positive_reviews": None,
        "negative_reviews": None,
        "total_reviews": None,
        "review_score_percent": None
    }


def fetch_top_player_chart(session):
    charts_response = session.get(
        STEAM_TOP_PLAYERS_URL,
        timeout=(5, 45)
    )
    charts_response.raise_for_status()
    charts_data = charts_response.json().get("response", {})
    chart_rows = charts_data.get("ranks", [])
    last_update = charts_data.get("last_update")

    if not chart_rows or not isinstance(last_update, int):
        raise ValueError("Steam concurrent-player chart is incomplete")

    source_measured_at = datetime.fromtimestamp(
        last_update,
        timezone.utc
    ).replace(tzinfo=None)

    return {
        int(row["appid"]): {
            "appid": int(row["appid"]),
            "current_players": int(row["concurrent_in_game"]),
            "player_rank": int(row["rank"]),
            "source_measured_at": source_measured_at,
            **empty_reviews()
        }
        for row in chart_rows
    }


def fetch_current_players(session, appid):
    players_response = session.get(
        STEAM_CURRENT_PLAYERS_URL,
        params={"appid": appid},
        timeout=(5, 45)
    )
    players_response.raise_for_status()
    players_data = players_response.json().get("response", {})

    if players_data.get("result") != 1:
        raise ValueError(
            f"Steam current-player request failed for app {appid}"
        )

    return {
        "appid": appid,
        "current_players": int(players_data["player_count"]),
        "player_rank": None,
        "source_measured_at": None,
        **empty_reviews()
    }


def fetch_review_summary(session, appid):
    reviews_response = session.get(
        STEAM_REVIEWS_URL.format(appid=appid),
        params=REVIEW_PARAMS,
        timeout=(5, 45)
    )
    reviews_response.raise_for_status()
    reviews_data = reviews_response.json()

    if reviews_data.get("success") != 1:
        raise ValueError(
            f"Steam review request failed for app {appid}"
        )

    summary = reviews_data.get("query_summary", {})
    positive = summary.get("total_positive")
    negative = summary.get("total_negative")
    total = summary.get("total_reviews")

    if None in (positive, negative, total):
        raise ValueError(
            f"Steam review summary is incomplete for app {appid}"
        )

    if positive + negative != total:
        raise ValueError(
            f"Steam review summary is inconsistent for app {appid}"
        )

    return {
        "positive_reviews": positive,
        "negative_reviews": negative,
        "total_reviews": total,
        "review_score_percent": (
            round(positive / total * 100, 2)
            if total else None
        )
    }


def get_steam_measurements():
    """Return (measurements, warnings) from Valve's player and review APIs.

    A failed top-100 chart falls back to direct player lookups for the
    tracked apps. A failed player lookup skips that app; a failed review
    lookup keeps the app's row with null review fields.
    """
    warnings = []

    with create_session() as session:
        try:
            measurements = fetch_top_player_chart(session)
        except ENRICHMENT_ERRORS as error:
            warnings.append(
                "Valve top-100 chart unavailable, used direct player "
                f"lookups for tracked apps: {error}"
            )
            measurements = {}

        for appid in get_tracked_appids():
            if appid not in measurements:
                try:
                    measurements[appid] = fetch_current_players(
                        session,
                        appid
                    )
                except ENRICHMENT_ERRORS as error:
                    warnings.append(
                        f"Valve player lookup failed for app {appid}, "
                        f"row skipped: {error}"
                    )
                    continue

            try:
                measurements[appid].update(
                    fetch_review_summary(session, appid)
                )
            except ENRICHMENT_ERRORS as error:
                warnings.append(
                    f"Steam review lookup failed for app {appid}, "
                    f"reviews left empty: {error}"
                )

    return list(measurements.values()), warnings
