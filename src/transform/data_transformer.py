import pandas as pd

def transform_games_data(games_list):

    df = pd.DataFrame(games_list)

    df["owners_low"] = df["owners"].str.split(" .. ").str[0]
    df["owners_high"] = df["owners"].str.split(" .. ").str[1]

    df["owners_low"] = (
        df["owners_low"]
        .str.replace(",", "")
        .astype(int)
    )

    df["owners_high"] = (
        df["owners_high"]
        .str.replace(",", "")
        .astype(int)
    )

    df["estimated_owners"] = (
        df["owners_low"] + df["owners_high"]
    ) / 2

    return df