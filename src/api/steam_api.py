import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import get_steamspy_config


STEAMSPY_URL = "https://steamspy.com/api.php"


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
