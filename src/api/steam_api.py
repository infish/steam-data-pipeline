import requests

from config import get_steamspy_config

def get_top_games():
    config = get_steamspy_config()

    if config["mode"] == "all":
        url = f"https://steamspy.com/api.php?request=all&page={config['page']}"
    else:
        url = f"https://steamspy.com/api.php?request={config['mode']}"

    try:

        response = requests.get(url, timeout=30)
        response.raise_for_status()

        data = response.json()

        return data

    except requests.RequestException as error:

        print("API request failed")

        print(error)

        return None

    except ValueError as error:

        print("API response was not valid JSON")

        print(error)

        return None
