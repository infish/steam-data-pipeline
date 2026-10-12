"""Value cleaning shared by the loaders."""

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
