import requests

def get_top_games():

    url = "https://steamspy.com/api.php?request=top100in2weeks"

    response = requests.get(url)

    data = response.json()

    return data