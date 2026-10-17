"""Value cleaning and games-row helpers shared by the loaders."""

import math


def clean_value(value):
    if value is None:
        return None

    if isinstance(value, float) and math.isnan(value):
        return None

    return value


def clean_int(value):
    value = clean_value(value)

    if value is None:
        return None

    return int(value)


def clean_float(value):
    value = clean_value(value)

    if value is None:
        return None

    return float(value)


def known_appids(cursor, appids):
    """The subset of appids that already have a games row."""
    appids = sorted(set(appids))

    if not appids:
        return set()

    placeholders = ", ".join(["%s"] * len(appids))
    cursor.execute(
        f"SELECT appid FROM games WHERE appid IN ({placeholders})",
        tuple(appids)
    )
    return {row[0] for row in cursor.fetchall()}


def ensure_games(cursor, appids, catalog_source, seen_at):
    """Add a games row for each app ID that has none; return those added.

    New rows get the placeholder name "App <appid>" until the Store details
    supply the real one (name_source 'placeholder'). Games already added
    from the same source get their last_seen_at moved forward.
    """
    appids = sorted(set(int(appid) for appid in appids))

    if not appids:
        return []

    missing = sorted(set(appids) - known_appids(cursor, appids))
    cursor.executemany(
        """
        INSERT INTO games (
            appid, name, first_seen_at, last_seen_at, catalog_source,
            name_source
        )
        VALUES (%s, %s, %s, %s, %s, 'placeholder')
        AS new
        ON DUPLICATE KEY UPDATE
            last_seen_at = IF(
                games.catalog_source = new.catalog_source,
                GREATEST(games.last_seen_at, new.last_seen_at),
                games.last_seen_at
            )
        """,
        [
            (appid, f"App {appid}", seen_at, seen_at, catalog_source)
            for appid in appids
        ]
    )
    return missing


def catalog_appids(cursor):
    """Every game in the catalog, whatever its source."""
    cursor.execute("SELECT appid FROM games ORDER BY appid")
    return [row[0] for row in cursor.fetchall()]
