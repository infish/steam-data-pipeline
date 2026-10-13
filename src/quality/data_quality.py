REQUIRED_COLUMNS = [
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

NON_NEGATIVE_COLUMNS = [
    "owners_low",
    "owners_high",
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


def check_structure(df):
    """Raise for problems that make the whole frame unusable."""
    errors = []

    if df.empty:
        errors.append("DataFrame is empty")

    missing_columns = [
        column for column in REQUIRED_COLUMNS
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

    if errors:
        raise ValueError("; ".join(errors))


def find_row_problems(df):
    """Return {index: [reasons]} for rows that must not be loaded."""
    problems = {}

    def reject(mask, reason):
        for index in df.index[mask.fillna(False).astype(bool)]:
            problems.setdefault(index, []).append(reason)

    name = df["name"].map(lambda value: value if isinstance(value, str) else "")
    reject(name.str.strip().eq(""), "name is missing or empty")

    reject(
        df["owners_low"].isna() | df["owners_high"].isna(),
        "owners is missing or not a 'low .. high' range"
    )
    reject(
        df["owners_low"] > df["owners_high"],
        "owners_low is greater than owners_high"
    )

    for column in NON_NEGATIVE_COLUMNS:
        reject(df[column].lt(0), f"{column} is negative")

    reject(
        df["review_score_percent"].lt(0) | df["review_score_percent"].gt(100),
        "review_score_percent is outside 0-100"
    )

    calculated_total = df["positive_reviews"] + df["negative_reviews"]
    reject(
        calculated_total.notna()
        & df["total_reviews"].notna()
        & calculated_total.ne(df["total_reviews"]),
        "total_reviews does not match positive_reviews + negative_reviews"
    )

    return problems


def validate_games_data(df, max_rejected_percent=5.0):
    """Split a transformed frame into loadable rows and rejected rows.

    Structural problems (empty frame, missing columns, null or duplicate
    app IDs) raise ValueError. Row-level problems reject only that row,
    unless more than max_rejected_percent of rows are rejected, which
    raises ValueError.

    Returns (valid_df, rejected), where rejected is a list of
    (appid, reasons) tuples in input order.
    """
    check_structure(df)

    problems = find_row_problems(df)
    rejected = [
        (int(df.at[index, "appid"]), problems[index])
        for index in df.index
        if index in problems
    ]

    rejected_percent = len(problems) / len(df) * 100

    if rejected_percent > max_rejected_percent:
        raise ValueError(
            f"{len(problems)} of {len(df)} rows "
            f"({rejected_percent:.1f}%) failed validation, above the "
            f"{max_rejected_percent:g}% limit; first problems: "
            + format_rejections(rejected, limit=5)
        )

    valid_df = df.drop(index=list(problems))

    if valid_df.empty:
        raise ValueError("No rows passed validation")

    return valid_df, rejected


def format_rejections(rejected, limit=20):
    return "; ".join(
        f"{appid}: {', '.join(reasons)}"
        for appid, reasons in rejected[:limit]
    )
