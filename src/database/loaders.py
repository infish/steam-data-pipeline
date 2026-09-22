"""Shared bounded database loaders used by production and benchmark paths."""

import math


def clean_value(value):
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def clean_int(value):
    value = clean_value(value)
    return None if value is None else int(value)


def clean_float(value):
    value = clean_value(value)
    return None if value is None else float(value)


def load_games(cursor, rows, observed_at):
    """Upsert an iterable of DataFrame rows or mapping-like benchmark rows."""
    query = """
        INSERT INTO games (
            appid, name, developer, publisher, languages, genre,
            first_seen_at, last_seen_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            name = VALUES(name), developer = VALUES(developer),
            publisher = VALUES(publisher), languages = VALUES(languages),
            genre = VALUES(genre), last_seen_at = VALUES(last_seen_at)
    """
    source = rows.itertuples(index=False) if hasattr(rows, "itertuples") else rows
    values = []
    for row in source:
        get = row.get if isinstance(row, dict) else lambda key: getattr(row, key)
        values.append((
            int(get("appid")), get("name"), clean_value(get("developer")),
            clean_value(get("publisher")), clean_value(get("languages")),
            clean_value(get("genre")), observed_at, observed_at,
        ))
    cursor.executemany(query, values)
    return len(values)


def load_game_metric_snapshots(cursor, rows, run_id, snapshot_time):
    query = """
        INSERT INTO game_metric_snapshots (
            run_id, appid, snapshot_time, owners, owners_low, owners_high,
            estimated_owners, positive_reviews, negative_reviews, total_reviews,
            review_score_percent, average_playtime_forever_minutes,
            average_playtime_2weeks_minutes, median_playtime_forever_minutes,
            median_playtime_2weeks_minutes, ccu, price_cents,
            initial_price_cents, discount_percent
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """
    source = rows.itertuples(index=False) if hasattr(rows, "itertuples") else rows
    values = []
    for row in source:
        get = row.get if isinstance(row, dict) else lambda key: getattr(row, key)
        values.append((
            run_id, int(get("appid")), snapshot_time, get("owners"),
            clean_int(get("owners_low")), clean_int(get("owners_high")),
            clean_int(get("estimated_owners")), clean_int(get("positive_reviews")),
            clean_int(get("negative_reviews")), clean_int(get("total_reviews")),
            clean_float(get("review_score_percent")),
            clean_int(get("average_playtime_forever_minutes")),
            clean_int(get("average_playtime_2weeks_minutes")),
            clean_int(get("median_playtime_forever_minutes")),
            clean_int(get("median_playtime_2weeks_minutes")), clean_int(get("ccu")),
            clean_int(get("price_cents")), clean_int(get("initial_price_cents")),
            clean_int(get("discount_percent")),
        ))
    cursor.executemany(query, values)
    return len(values)


def load_steam_measurements(cursor, measurements, run_id, collected_at):
    query = """
        INSERT INTO steam_game_measurements (
            run_id, appid, collected_at, source_measured_at, current_players,
            player_rank, positive_reviews, negative_reviews, total_reviews,
            review_score_percent
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    values = [(
        run_id, row["appid"], collected_at, row["source_measured_at"],
        row["current_players"], clean_int(row["player_rank"]),
        clean_int(row["positive_reviews"]), clean_int(row["negative_reviews"]),
        clean_int(row["total_reviews"]), clean_float(row["review_score_percent"]),
    ) for row in measurements]
    cursor.executemany(query, values)
    return len(values)
