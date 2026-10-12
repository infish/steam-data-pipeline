"""HTTP sessions that replay stored responses instead of calling the APIs.

FixtureSession serves the recorded responses in tests/fixtures/sources (see
docs/sources.md). RawReplaySession serves a past run's raw_api_responses, so
reprocessing goes through exactly the same parsing and loading code as the
original run.
"""

import json
from pathlib import Path
from urllib.parse import urlparse

import requests

from ingest.raw_store import read_payload, request_hash


FIXTURE_DIR = (
    Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "sources"
)


def make_response(url, status, body, raw_response_id=None):
    response = requests.Response()
    response.status_code = status
    response._content = body
    response.url = url
    response.reason = "OK" if status < 400 else "Replayed error"
    response.raw_response_id = raw_response_id
    return response


def route_for(url, params):
    """Name the logical request, e.g. 'steam_review_summary:620'."""
    params = params or {}
    path = urlparse(url).path

    if "steamspy.com" in url:
        if params.get("request") == "appdetails":
            return f"steamspy_appdetails:{params.get('appid')}"
        return "steamspy_all"

    if path.startswith("/appreviews/"):
        return f"steam_review_summary:{path.rsplit('/', 1)[-1]}"

    if path.startswith("/appreviewhistogram/"):
        return f"steam_review_histogram:{path.rsplit('/', 1)[-1]}"

    if path == "/api/appdetails":
        if params.get("filters") == "price_overview":
            return f"store_prices:{params.get('appids')}"
        return f"store_appdetails:{params.get('appids')}"

    if "GetGamesByConcurrentPlayers" in path:
        return "valve_top_concurrent"

    if "GetMostPlayedGames" in path:
        return "valve_most_played"

    if "GetNumberOfCurrentPlayers" in path:
        return f"valve_current_players:{params.get('appid')}"

    if "GetNewsForApp" in path:
        return f"steam_news:{params.get('appid')}"

    return f"unknown:{path}"


class FixtureSession:
    """A requests-like session answering from recorded fixtures.

    overrides maps a route (see route_for) to a payload (dict or list), an
    (http_status, payload) tuple, or an exception instance to raise.
    """

    FIXTURE_FILES = {
        "steamspy_all": "steamspy_all/page0_tracked.json",
        "steamspy_appdetails": "steamspy_appdetails/{key}.json",
        "steam_review_summary": "review_summary/{key}.json",
        "steam_review_histogram": "review_histogram/{key}.json",
        "store_appdetails": "store_appdetails/{key}.json",
        "valve_top_concurrent": "top_concurrent/all.json",
        "valve_most_played": "most_played/all.json",
        "valve_current_players": "current_players/{key}.json",
        "steam_news": "news/{key}.json",
    }

    def __init__(self, overrides=None, fixture_dir=FIXTURE_DIR):
        self.overrides = overrides or {}
        self.fixture_dir = Path(fixture_dir)
        self.calls = []

    def get(self, url, params=None, timeout=None):
        route = route_for(url, params)
        self.calls.append(route)

        if route in self.overrides:
            answer = self.overrides[route]

            if isinstance(answer, BaseException):
                raise answer

            status, payload = (
                answer if isinstance(answer, tuple) else (200, answer)
            )
            return make_response(url, status, json.dumps(payload).encode())

        payload = self._fixture(route)

        if payload is None:
            return make_response(url, 404, b"null")

        return make_response(url, 200, json.dumps(payload).encode())

    def _fixture(self, route):
        name, _, key = route.partition(":")

        if name == "store_prices":
            return self._price_batch(key.split(","))

        pattern = self.FIXTURE_FILES.get(name)

        if pattern is None:
            return None

        path = self.fixture_dir / pattern.format(key=key)

        if not path.exists():
            return None

        return json.loads(path.read_text())

    def _price_batch(self, appids):
        """Answer a price batch from each app's recorded store details."""
        batch = {}

        for appid in appids:
            details = self._fixture(f"store_appdetails:{appid}")

            if details is None:
                batch[appid] = {"success": False}
                continue

            data = details[appid].get("data", {})
            price = data.get("price_overview")
            batch[appid] = {
                "success": True,
                "data": {"price_overview": price} if price else []
            }

        return batch

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()


class RawReplaySession:
    """Answers each request with the response a past run stored for it."""

    def __init__(self, connection, run_id):
        self.connection = connection
        self.responses = {}
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT response_id, request_params_hash, http_status,
                   error_message
            FROM raw_api_responses
            WHERE run_id = %s
            ORDER BY response_id
            """,
            (run_id,)
        )

        for response_id, params_hash, status, error in cursor.fetchall():
            self.responses.setdefault(params_hash, []).append(
                (response_id, status, error)
            )

        cursor.close()

    def get(self, url, params=None, timeout=None):
        recorded = self.responses.get(request_hash(url, params))

        if not recorded:
            raise requests.ConnectionError(
                f"no stored response for {url} {params}"
            )

        response_id, status, error = recorded.pop(0)

        if status is None:
            raise requests.ConnectionError(error or "request failed")

        body = read_payload(self.connection, response_id)
        return make_response(url, status, body or b"", response_id)

    def close(self):
        pass


class ReplayRawStore:
    """Raw store for reprocessing: points at existing rows, writes nothing."""

    def __init__(self, connection):
        self.connection = connection

    def record(self, source, endpoint, params, response=None, error=None):
        return getattr(response, "raw_response_id", None)

    def read_payload(self, response_id):
        return read_payload(self.connection, response_id)
