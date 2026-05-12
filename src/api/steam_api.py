import requests

def get_top_games():

    url = "https://steamspy.com/api.php?request=top100in2weeks"

    try:

        response = requests.get(url)

        data = response.json()

        return data

    except Exception as error:

        print("API request failed")

        print(error)

        return None