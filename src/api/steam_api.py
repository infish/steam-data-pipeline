import json
from itertools import takewhile

import requests
from datetime import datetime, timezone
from requests.adapters import HTTPAdapter
from urllib3.exceptions import MaxRetryError
from urllib3.util.retry import Retry

from api.pacing import Pacer
from config import get_steamspy_config, get_tracked_appids
from ingest.raw_store import RETRIED_PREFIX, MemoryRawStore


STEAMSPY_URL = "https://steamspy.com/api.php"
STEAM_CURRENT_PLAYERS_URL = (
    "https://api.steampowered.com/"
    "ISteamUserStats/GetNumberOfCurrentPlayers/v1/"
)
STEAM_TOP_PLAYERS_URL = (
    "https://api.steampowered.com/"
    "ISteamChartsService/GetGamesByConcurrentPlayers/v1/"
)
# Review summaries come from the Web API since v1.2.2: Valve switched the
# Store's appreviews endpoint off on 2026-10-22 (it now redirects to the
# Store home page). No key is needed; keyless calls share a lower limit.
STEAM_REVIEWS_URL = (
    "https://api.steampowered.com/IUserReviewsService/GetAppReviews/v1/"
)
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

# Review summaries (IUserReviewsService/GetAppReviews) have their own limit,
# separate from the Store's. Keyless calls share an unpublished lower limit;
# a live probe on 2026-10-23 ran 144, 201, 251 and then 301 requests per
# 5 minutes with no refusal or slowdown. 1.2 s (250 per 5 minutes) keeps a
# step below the fastest rate tested (docs/sources.md, rate limits).
REVIEWS_INTERVAL_SECONDS = 1.2

# SteamSpy allows one request=all page per 60 seconds.
STEAMSPY_PAGE_INTERVAL_SECONDS = 61

# Minimum interval per paced API, for RunContext.pace.
API_INTERVALS = {
    "steamspy": STEAMSPY_REQUEST_INTERVAL_SECONDS,
    "steamspy_all": STEAMSPY_PAGE_INTERVAL_SECONDS,
    "store": STORE_REQUEST_INTERVAL_SECONDS,
    "reviews": REVIEWS_INTERVAL_SECONDS,
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


# How long one logical request may take (docs/request_budget.md, "When an
# API fails"): three tries of up to 25 s plus two waits of at most 30 s,
# 135 s. Before v1.3.0 it was 253 s: five tries of up to 45 s, the first
# retry immediate, Retry-After honoured without a cap.
REQUEST_TIMEOUT = (5, 20)  # seconds to connect, seconds to wait for data
RETRY_ATTEMPTS = 2  # retries after the first try: at most three tries
RETRY_BACKOFF_SECONDS = 5  # 5 s before the first retry, 10 s before the next
RETRY_AFTER_CAP_SECONDS = 30
# A run stops calling an API after this many failed requests in a row.
CONSECUTIVE_FAILURE_LIMIT = 5


class BoundedRetry(Retry):
    """urllib3's Retry with no immediate first retry and a capped
    Retry-After, so a struggling API gets room and a run gets a bound.

    Retries are always at least RETRY_BACKOFF_SECONDS apart, longer than any
    pacer's interval, so they never exceed an API's paced rate.
    """

    def get_backoff_time(self):
        failures = len(list(takewhile(
            lambda attempt: attempt.redirect_location is None,
            reversed(self.history)
        )))

        if failures == 0:
            return 0

        return min(self.backoff_max,
                   self.backoff_factor * 2 ** (failures - 1))

    def get_retry_after(self, response):
        seconds = super().get_retry_after(response)
        return None if seconds is None else min(seconds,
                                                 RETRY_AFTER_CAP_SECONDS)

    def increment(self, *args, **kwargs):
        # MaxRetryError does not say how many attempts came before the last
        # one; attach them, so fetch_json can record each (retried_attempts).
        try:
            return super().increment(*args, **kwargs)
        except MaxRetryError as error:
            error.retry_history = self.history
            raise


def create_session():
    retry_policy = BoundedRetry(
        total=RETRY_ATTEMPTS,
        connect=RETRY_ATTEMPTS,
        read=RETRY_ATTEMPTS,
        status=RETRY_ATTEMPTS,
        backoff_factor=RETRY_BACKOFF_SECONDS,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
        # Return the last refused answer instead of raising, so it is
        # recorded in the raw layer like any other answer.
        raise_on_status=False
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
        response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
    except requests.RequestException as error:
        # Attempts the session made before giving up (BoundedRetry attaches
        # them) are requests too, so an outage shows its real request count.
        cause = error.args[0] if error.args else None

        for attempt in describe_attempts(
            getattr(cause, "retry_history", None) or ()
        ):
            store.record(source, url, params, error=attempt)

        store.record(source, url, params, error=str(error))
        raise

    # Attempts the session retried are requests too: record each one, so
    # the raw layer and the request counts show the real load.
    for attempt in retried_attempts(response):
        store.record(source, url, params, error=attempt)

    if response.history:
        # A JSON API that redirects has moved or been switched off (the
        # Store's appreviews went to the home page on 2026-10-22). Record a
        # failed request, so freshness checks see the source go stale,
        # instead of storing the page it redirected to as a success.
        error = (
            f"redirected ({response.history[0].status_code}) to "
            f"{response.url}"
        )
        store.record(source, url, params, error=error)
        raise requests.HTTPError(error, response=response)

    response_id = store.record(source, url, params, response=response)
    response.raise_for_status()
    return json.loads(store.read_payload(response_id))


def retried_attempts(response):
    """Errors of the attempts the HTTP session retried before `response`."""
    retries = getattr(getattr(response, "raw", None), "retries", None)
    return describe_attempts(getattr(retries, "history", None) or ())


def describe_attempts(history):
    """'retried: ...' error messages for urllib3 retry history entries."""
    return [
        f"{RETRIED_PREFIX}HTTP {attempt.status}" if attempt.status
        else f"{RETRIED_PREFIX}{attempt.error}"
        for attempt in history
        if attempt.redirect_location is None
    ]


def api_struggling(error):
    """Whether an error says the API itself is struggling: no connection, a
    timeout, or 429/5xx after retries. Any other answer (a 404 for one game,
    an empty summary) shows the API is up."""
    if isinstance(error, requests.HTTPError):
        status = getattr(error.response, "status_code", None)
        return status is None or status == 429 or status >= 500

    return isinstance(error, requests.RequestException)


class FailureStreak:
    """One API's struggling answers in a row, within a run (api_struggling).
    Any other answer, even one that is unusable for its game, ends it.
    """

    def __init__(self, limit=CONSECUTIVE_FAILURE_LIMIT):
        self.limit = limit
        self.count = 0

    def record(self, error=None):
        if error is not None and api_struggling(error):
            self.count += 1
        else:
            self.count = 0

    @property
    def tripped(self):
        return self.count >= self.limit


def stopped_early(what, done, total, reason):
    return (
        f"Stopped {what} early ({reason}): {done} of {total} done, the"
        " rest keep their previous data"
    )


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


# GetAppReviews with plain parameters (input_json is optional). The totals
# count every purchase (purchase_type 1 = all, as appreviews'
# purchase_type=all did; 0 counts only Steam purchases) in every language,
# review bombs included. Tested live on 2026-10-23.
REVIEW_PARAMS = {
    "num_per_page": 1,
    "languages[0]": "all",
    "purchase_type": 1,
    "filter_offtopic_activity": 0,
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


def review_query_summary(reviews_data, appid):
    """The query_summary of a review answer.

    GetAppReviews wraps it in "response" and has no success flag (an error
    is an HTTP status). Raw payloads stored before v1.2.2 come from the
    Store's appreviews, which answered {"success": 1, "query_summary": ...}.
    """
    if not isinstance(reviews_data, dict):
        raise ValueError(
            f"Steam review answer is not an object for app {appid}"
        )

    if "response" in reviews_data:
        reviews_data = reviews_data["response"]
    elif reviews_data.get("success") != 1:
        raise ValueError(f"Steam review request failed for app {appid}")

    summary = reviews_data.get("query_summary")

    if not isinstance(summary, dict):
        raise ValueError(f"Steam review summary is missing for app {appid}")

    return summary


def fetch_review_summary(session, store, appid):
    reviews_data = fetch_json(
        session,
        store,
        SOURCE_REVIEW_SUMMARY,
        STEAM_REVIEWS_URL,
        {"appid": appid, **REVIEW_PARAMS}
    )
    summary = review_query_summary(reviews_data, appid)
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


def get_review_summaries(session, appids, store=None, pace=None,
                         keep_going=None):
    """Return (summaries, warnings); a failed lookup skips that app.

    pace() is called before each request (RunContext.pace("reviews")).
    The loop stops early when keep_going() turns false (the run's time
    limit) or after CONSECUTIVE_FAILURE_LIMIT failed requests in a row.
    """
    store = store or MemoryRawStore()
    summaries = []
    warnings = []
    streak = FailureStreak()

    for done, appid in enumerate(appids):
        reason = (
            "run time limit" if keep_going and not keep_going()
            else f"{streak.limit} failed requests in a row"
            if streak.tripped else None
        )

        if reason:
            warnings.append(stopped_early(
                "review summaries", done, len(appids), reason
            ))
            break

        if pace:
            pace()

        try:
            summaries.append(fetch_review_summary(session, store, appid))
            streak.record()
        except ENRICHMENT_ERRORS as error:
            streak.record(error)
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
