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

    numeric_columns = [
        "positive_reviews",
        "negative_reviews",
        "average_playtime_forever_minutes",
        "average_playtime_2weeks_minutes",
        "median_playtime_forever_minutes",
        "median_playtime_2weeks_minutes",
        "ccu",
        "price_cents",
        "initial_price_cents",
        "discount_percent"
    ]

    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    df["total_reviews"] = (
        df["positive_reviews"] + df["negative_reviews"]
    )

    df["review_score_percent"] = (
        df["positive_reviews"] / df["total_reviews"] * 100
    ).round(2)

    df.loc[df["total_reviews"] == 0, "review_score_percent"] = None

    return df
