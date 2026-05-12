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
        "estimated_owners"
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

    if errors:
        raise ValueError("; ".join(errors))
