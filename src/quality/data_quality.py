def validate_games_data(df):
    errors = []

    if df.empty:
        errors.append("DataFrame is empty")

    required_columns = [
        "appid",
        "name",
        "owners",
        "owners_low",
        "owners_high",
        "estimated_owners",
        "positive_reviews",
        "negative_reviews",
        "total_reviews",
        "review_score_percent",
        "average_playtime_forever_minutes",
        "average_playtime_2weeks_minutes",
        "median_playtime_forever_minutes",
        "median_playtime_2weeks_minutes",
        "ccu",
        "price_cents",
        "initial_price_cents",
        "discount_percent"
    ]

    missing_columns = [
        column for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:
        errors.append(f"Missing columns: {', '.join(missing_columns)}")

    if errors:
        raise ValueError("; ".join(errors))

    if df["appid"].isna().any():
        errors.append("appid contains null values")

    if df["appid"].duplicated().any():
        errors.append("appid contains duplicate values")

    if df["name"].isna().any() or df["name"].str.strip().eq("").any():
        errors.append("name contains empty values")

    if df["owners_low"].isna().any():
        errors.append("owners_low contains null values")

    if df["owners_high"].isna().any():
        errors.append("owners_high contains null values")

    if df["estimated_owners"].isna().any():
        errors.append("estimated_owners contains null values")

    if (df["owners_low"] > df["owners_high"]).any():
        errors.append("owners_low is greater than owners_high")

    if (df["estimated_owners"] < 0).any():
        errors.append("estimated_owners contains negative values")

    non_negative_columns = [
        "positive_reviews",
        "negative_reviews",
        "total_reviews",
        "average_playtime_forever_minutes",
        "average_playtime_2weeks_minutes",
        "median_playtime_forever_minutes",
        "median_playtime_2weeks_minutes",
        "ccu",
        "price_cents",
        "initial_price_cents",
        "discount_percent"
    ]

    for column in non_negative_columns:
        if df[column].dropna().lt(0).any():
            errors.append(f"{column} contains negative values")

    review_scores = df["review_score_percent"].dropna()

    if review_scores.lt(0).any() or review_scores.gt(100).any():
        errors.append("review_score_percent must be between 0 and 100")

    calculated_total_reviews = df["positive_reviews"] + df["negative_reviews"]
    total_reviews_mask = calculated_total_reviews.notna() & df["total_reviews"].notna()

    if calculated_total_reviews[total_reviews_mask].ne(
        df.loc[total_reviews_mask, "total_reviews"]
    ).any():
        errors.append("total_reviews does not match positive_reviews + negative_reviews")

    if errors:
        raise ValueError("; ".join(errors))
