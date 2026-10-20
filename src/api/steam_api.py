import json

import requests
from datetime import datetime, timezone
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from api.pacing import Pacer
from config import get_steamspy_config, get_tracked_appids
from ingest.raw_store import MemoryRawStore


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
STORE_APPDETAILS_URL = "https://store.steampowered.com/api/appdetails"
STEAM_REVIEW_HISTOGRAM_URL = (
    "https://store.steampowered.com/appreviewhistogram/{appid}"
)
STEAM_NEWS_URL = (
    "https://api.steampowered.com/ISteamNews/GetNewsForApp/v2/"
)
STEAM_MOST_PLAYED_URL = (
    "https://api.steampowered.com/"
    "ISteamChartsService/GetMostPlayedGames/v1/"
)

# Source names recorded in raw_api_responses.source (see docs/sources.md).
SOURCE_STEAMSPY_ALL = "steamspy_all"
SOURCE_STEAMSPY_APPDETAILS = "steamspy_appdetails"
SOURCE_TOP_CONCURRENT = "valve_top_concurrent"
SOURCE_CURRENT_PLAYERS = "valve_current_players"
SOURCE_REVIEW_SUMMARY = "steam_review_summary"
SOURCE_MOST_PLAYED = "valve_most_played"
SOURCE_STORE_APPDETAILS = "store_appdetails"
SOURCE_STORE_PRICES = "store_prices"
SOURCE_REVIEW_HISTOGRAM = "steam_review_histogram"
SOURCE_NEWS = "steam_news"

# Batching works with filters=price_overview (docs/sources.md). Tested live
# on 2026-10-20: 500 app IDs per request worked; 1,000 failed with HTTP 414
# (the URL got too long). 200 leaves room for longer, 7-digit app IDs
# (docs/optimizations.md, entry 3).
PRICE_BATCH_SIZE = 200

# SteamSpy allows one non-"all" request per second; this is the minimum
# time between the starts of two SteamSpy requests, the page request
# included (api/pacing.py).
STEAMSPY_REQUEST_INTERVAL_SECONDS = 1.1

# The Store budget is 150 requests per 5 minutes (docs/request_budget.md):
# one request every 2 s, plus a margin, between the starts of two paced Store
# requests (details, price batches, review histograms).
STORE_REQUEST_INTERVAL_SECONDS = 2.1

# SteamSpy allows one request=all page per 60 seconds.
STEAMSPY_PAGE_INTERVAL_SECONDS = 61

# Minimum interval per paced API, for RunContext.pace.
API_INTERVALS = {
    "steamspy": STEAMSPY_REQUEST_INTERVAL_SECONDS,
    "steamspy_all": STEAMSPY_PAGE_INTERVAL_SECONDS,
    "store": STORE_REQUEST_INTERVAL_SECONDS,
}

# The SteamSpy catalog fields run_pipeline reads from a page row.
CATALOG_ROW_FIELDS = (
    "appid",
    "name",
    "developer",
    "publisher",
    "owners",
    "positive",
    "negative",
    "average_forever",
    "average_2weeks",
    "median_forever",
    "median_2weeks",
    "ccu",
    "price",
    "initialprice",
    "discount",
    "languages",
    "genre",
)


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


def fetch_json(session, store, source, url, params=None):
    """GET a JSON endpoint through the raw landing layer.

    The response is recorded before anything parses it, and the parsed
    value comes from the stored payload, not from the live response, so a
    later reprocess reads exactly what this run read.
    """
    try:
        response = session.get(url, params=params, timeout=(5, 45))
    except requests.RequestException as error:
        store.record(source, url, params, error=str(error))
        raise

    response_id = store.record(source, url, params, response=response)
    response.raise_for_status()
    return json.loads(store.read_payload(response_id))


def get_top_games(session, store=None, page=None):
    """One SteamSpy catalog page (STEAMSPY_PAGE unless page is given)."""
    store = store or MemoryRawStore()
    config = get_steamspy_config()

    params = {
        "request": config["mode"]
    }

    if config["mode"] == "all":
        params["page"] = config["page"] if page is None else page

    data = fetch_json(
        session,
        store,
        SOURCE_STEAMSPY_ALL,
        STEAMSPY_URL,
        params
    )

    if not isinstance(data, dict) or not data:
        raise ValueError("SteamSpy returned an empty or unexpected response")

    return data


# Enrichment requests (Valve data and the SteamSpy backfill of tracked
# apps) degrade a run into warnings instead of failing it.
ENRICHMENT_ERRORS = (
    requests.RequestException,
    ValueError,
    KeyError,
    TypeError
)


class SteamSpyUnknownApp(ValueError):
    """SteamSpy has no data for the app: it answers without a name.

    Usual for games released after SteamSpy froze (docs/sources.md); callers
    that only want optional data (tags) treat it as "none", not a failure.
    """


def fetch_steamspy_appdetails_payload(session, store, appid):
    """The full SteamSpy appdetails object, including tags."""
    details = fetch_json(
        session,
        store,
        SOURCE_STEAMSPY_APPDETAILS,
        STEAMSPY_URL,
        {"request": "appdetails", "appid": appid}
    )

    if not isinstance(details, dict):
        raise ValueError(
            f"SteamSpy appdetails is not an object for app {appid}"
        )

    if int(details.get("appid", -1)) != appid:
        raise ValueError(
            f"SteamSpy appdetails returned another app for {appid}"
        )

    if not details.get("name"):
        raise SteamSpyUnknownApp(
            f"SteamSpy appdetails has no name for app {appid}"
        )

    return details


def fetch_steamspy_app_details(session, store, appid):
    """SteamSpy appdetails shaped like a request=all page row."""
    details = fetch_steamspy_appdetails_payload(session, store, appid)
    return {field: details.get(field) for field in CATALOG_ROW_FIELDS}


def fetch_store_prices(session, store, appids, country):
    """One price_overview batch for several apps."""
    return fetch_json(
        session,
        store,
        SOURCE_STORE_PRICES,
        STORE_APPDETAILS_URL,
        {
            "appids": ",".join(str(appid) for appid in appids),
            "cc": country,
            "filters": "price_overview",
        }
    )


def fetch_review_histogram(session, store, appid):
    """The undocumented review histogram (monthly and last-30-day buckets)."""
    return fetch_json(
        session,
        store,
        SOURCE_REVIEW_HISTOGRAM,
        STEAM_REVIEW_HISTOGRAM_URL.format(appid=appid),
        {"l": "english", "review_score_preference": 0}
    )


def fetch_news(session, store, appid, count=20):
    """The newest `count` Steam news items for an app."""
    return fetch_json(
        session,
        store,
        SOURCE_NEWS,
        STEAM_NEWS_URL,
        {"appid": appid, "count": count}
    )


def fetch_store_app_details(session, store, appid, country):
    """The Store appdetails response for one app (prices in `country`)."""
    return fetch_json(
        session,
        store,
        SOURCE_STORE_APPDETAILS,
        STORE_APPDETAILS_URL,
        {"appids": appid, "cc": country, "l": "english"}
    )


def get_tracked_app_details(session, appids, store=None, pace=None):
    """Return ({appid_str: page_row}, warnings) for apps off the page.

    Rows are shaped like SteamSpy request=all page rows. A failed lookup
    becomes a warning and the app is skipped. pace() is called before each
    SteamSpy request (RunContext.pace); by default requests are spaced by
    STEAMSPY_REQUEST_INTERVAL_SECONDS.
    """
    store = store or MemoryRawStore()
    pace = pace or Pacer(STEAMSPY_REQUEST_INTERVAL_SECONDS).wait
    rows = {}
    warnings = []

    for appid in appids:
        pace()

        try:
            rows[str(appid)] = fetch_steamspy_app_details(
                session,
                store,
                appid
            )
        except ENRICHMENT_ERRORS as error:
            warnings.append(
                f"SteamSpy appdetails failed for tracked app {appid}, "
                f"skipped: {error}"
            )

    return rows, warnings


REVIEW_PARAMS = {
    "json": 1,
    "filter": "recent",
    "language": "all",
    "review_type": "all",
    "purchase_type": "all",
    "num_per_page": 1,
    "filter_offtopic_activity": 0
}


def fetch_top_player_chart(session, store):
    charts_data = fetch_json(
        session,
        store,
        SOURCE_TOP_CONCURRENT,
        STEAM_TOP_PLAYERS_URL
    ).get("response", {})
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
            "source": "top100"
        }
        for row in chart_rows
    }


def fetch_current_players(session, store, appid):
    players_data = fetch_json(
        session,
        store,
        SOURCE_CURRENT_PLAYERS,
        STEAM_CURRENT_PLAYERS_URL,
        {"appid": appid}
    ).get("response", {})

    if players_data.get("result") != 1:
        raise ValueError(
            f"Steam current-player request failed for app {appid}"
        )

    return {
        "appid": appid,
        "current_players": int(players_data["player_count"]),
        "player_rank": None,
        "source_measured_at": None,
        "source": "direct"
    }


def fetch_review_summary(session, store, appid):
    reviews_data = fetch_json(
        session,
        store,
        SOURCE_REVIEW_SUMMARY,
        STEAM_REVIEWS_URL.format(appid=appid),
        REVIEW_PARAMS
    )

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
        "appid": appid,
        "positive_reviews": positive,
        "negative_reviews": negative,
        "total_reviews": total,
        "review_score_percent": (
            round(positive / total * 100, 2)
            if total else None
        ),
        "review_score": summary.get("review_score"),
        "review_score_desc": summary.get("review_score_desc")
    }


def get_player_measurements(session, store=None):
    """Return (measurements, warnings) from Valve's player APIs.

    The top-100 chart covers most games in one request. Tracked apps outside
    it get a direct lookup; if the chart fails, every tracked app does. A
    failed direct lookup skips that app with a warning.
    """
    store = store or MemoryRawStore()
    warnings = []

    try:
        measurements = fetch_top_player_chart(session, store)
    except ENRICHMENT_ERRORS as error:
        warnings.append(
            "Valve top-100 chart unavailable, used direct player "
            f"lookups for tracked apps: {error}"
        )
        measurements = {}

    for appid in get_tracked_appids():
        if appid in measurements:
            continue

        try:
            measurements[appid] = fetch_current_players(session, store, appid)
        except ENRICHMENT_ERRORS as error:
            warnings.append(
                f"Valve player lookup failed for app {appid}, "
                f"row skipped: {error}"
            )

    return list(measurements.values()), warnings


def get_review_summaries(session, appids, store=None, pace=None):
    """Return (summaries, warnings); a failed lookup skips that app.

    pace() is called before each request (RunContext.pace("store")); the
    review summary is a Store endpoint.
    """
    store = store or MemoryRawStore()
    summaries = []
    warnings = []

    for appid in appids:
        if pace:
            pace()

        try:
            summaries.append(fetch_review_summary(session, store, appid))
        except ENRICHMENT_ERRORS as error:
            warnings.append(
                f"Steam review lookup failed for app {appid}, "
                f"skipped: {error}"
            )

    return summaries, warnings


def get_most_played(session, store=None):
    """Return (rollup_date, rows) for Valve's daily peak leaderboard.

    rollup_date is the UTC day the peaks refer to (the previous full day).
    """
    store = store or MemoryRawStore()
    data = fetch_json(
        session,
        store,
        SOURCE_MOST_PLAYED,
        STEAM_MOST_PLAYED_URL
    ).get("response", {})
    rollup_date = data.get("rollup_date")
    ranks = data.get("ranks") or []

    if not isinstance(rollup_date, int) or not ranks:
        raise ValueError("Steam most-played leaderboard is incomplete")

    rows = [
        {
            "appid": int(row["appid"]),
            "player_rank": int(row["rank"]),
            # -1 (or 0) marks a game that was not ranked last week.
            "last_week_rank": (
                int(row["last_week_rank"])
                if int(row.get("last_week_rank") or 0) > 0 else None
            ),
            "peak_players": int(row["peak_in_game"]),
        }
        for row in ranks
    ]
    day = datetime.fromtimestamp(rollup_date, timezone.utc).date()
    return day, rows
