import pandas as pd

def transform_games_data(games_list):

    df = pd.DataFrame(games_list)

    # Malformed or missing owner ranges become NaN for validation to reject.
    owners = df["owners"].where(
        df["owners"].map(lambda value: isinstance(value, str))
    )
    owner_bounds = owners.str.split(" .. ", regex=False)

    df["owners_low"] = pd.to_numeric(
        owner_bounds.str[0].str.replace(",", "", regex=False),
        errors="coerce"
    )

    df["owners_high"] = pd.to_numeric(
        owner_bounds.str[1].str.replace(",", "", regex=False),
        errors="coerce"
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
