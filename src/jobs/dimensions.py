"""Normalized dimensions loaded by catalog_daily.

13a: company links parsed from SteamSpy developer/publisher strings, for
every loaded game on every run.
13b: Store details (type, release date, platforms, genres, categories,
languages, Metacritic) and SteamSpy user tags, for the STORE_DETAILS_PER_RUN
stalest games per run, new games first.
"""

import logging

from api.steam_api import (
    ENRICHMENT_ERRORS,
    fetch_steamspy_appdetails_payload,
    fetch_store_app_details,
)
from config import get_store_country, get_store_details_per_run
from parsing.names import split_company_names
from parsing.store import parse_steamspy_tags, parse_store_details


LISTED_FAILURES = 10


def placeholders(values):
    return ", ".join(["%s"] * len(values))


def upsert_names(cursor, table, id_column, names):
    """Insert missing names into a name-keyed lookup; return {name: id}."""
    names = list(dict.fromkeys(names))

    if not names:
        return {}

    # The no-op update sets name, not the id: names equal under the
    # collation ("Valve" and "VALVE") can collide with a row inserted earlier
    # in the same batch, and MySQL rejects touching that row's freshly
    # generated auto-increment id (error 1869).
    cursor.executemany(
        f"INSERT INTO {table} (name) VALUES (%s)"
        " ON DUPLICATE KEY UPDATE name = name",
        [(name,) for name in names]
    )
    ids = {}

    # The unique key uses MySQL's case- and accent-insensitive collation, so
    # look each name up through MySQL rather than comparing in Python.
    for name in names:
        cursor.execute(
            f"SELECT {id_column} FROM {table} WHERE name = %s",
            (name,)
        )
        ids[name] = cursor.fetchall()[0][0]

    return ids


def replace_links(cursor, table, appids, columns, rows):
    """Replace a bridge table's rows for the given games."""
    appids = list(appids)

    if appids:
        cursor.execute(
            f"DELETE FROM {table} WHERE appid IN ({placeholders(appids)})",
            tuple(appids)
        )

    if rows:
        cursor.executemany(
            f"INSERT INTO {table} ({', '.join(columns)})"
            f" VALUES ({placeholders(columns)})",
            rows
        )


def load_company_links(cursor, df):
    """13a: rebuild game_developers and game_publishers for loaded games."""
    parsed = {
        int(row.appid): (
            split_company_names(row.developer),
            split_company_names(row.publisher),
        )
        for row in df.itertuples(index=False)
    }
    company_ids = upsert_names(
        cursor,
        "companies",
        "company_id",
        [
            name
            for developers, publishers in parsed.values()
            for name in developers + publishers
        ]
    )

    for table, index in (("game_developers", 0), ("game_publishers", 1)):
        rows = {}

        for appid, names in parsed.items():
            for position, name in enumerate(names[index], start=1):
                # Two names can map to one company (case or accents differ);
                # keep the first link.
                rows.setdefault(
                    (appid, company_ids[name]),
                    (appid, company_ids[name], min(position, 255))
                )

        replace_links(
            cursor,
            table,
            parsed,
            ("appid", "company_id", "position"),
            list(rows.values())
        )

    return len(company_ids)


def select_stalest(cursor, appids, limit):
    """Games without details first, then the oldest refresh, then appid."""
    appids = sorted(set(appids))

    if not appids or limit == 0:
        return []

    cursor.execute(
        f"""
        SELECT games.appid
        FROM games
        LEFT JOIN game_details AS details
            ON details.appid = games.appid
        WHERE games.appid IN ({placeholders(appids)})
        ORDER BY details.details_fetched_at IS NOT NULL,
                 details.details_fetched_at,
                 games.appid
        LIMIT %s
        """,
        (*appids, limit)
    )
    return [row[0] for row in cursor.fetchall()]


def refreshed_in_run(cursor, run_id):
    """The games a past run fetched Store details for (for reprocessing)."""
    cursor.execute(
        """
        SELECT JSON_UNQUOTE(JSON_EXTRACT(request_params, '$.appids'))
        FROM raw_api_responses
        WHERE run_id = %s AND source = 'store_appdetails'
        ORDER BY response_id
        """,
        (run_id,)
    )
    return [int(row[0]) for row in cursor.fetchall()]


def load_store_details(cursor, details, run_id, fetched_at):
    appid = details["appid"]

    if not details["available"]:
        # Delisted or region-locked: keep the last known details.
        cursor.execute(
            """
            INSERT INTO game_details (
                appid, store_available, details_fetched_at, run_id
            )
            VALUES (%s, FALSE, %s, %s)
            AS new
            ON DUPLICATE KEY UPDATE
                store_available = FALSE,
                details_fetched_at = new.details_fetched_at,
                run_id = new.run_id
            """,
            (appid, fetched_at, run_id)
        )
        return

    cursor.execute(
        """
        INSERT INTO game_details (
            appid, store_available, app_type, release_date,
            release_date_text, coming_soon, is_free, required_age,
            metacritic_score, platform_windows, platform_mac, platform_linux,
            recommendations_total, details_fetched_at, run_id
        )
        VALUES (%s, TRUE, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        AS new
        ON DUPLICATE KEY UPDATE
            store_available = TRUE,
            app_type = new.app_type,
            release_date = new.release_date,
            release_date_text = new.release_date_text,
            coming_soon = new.coming_soon,
            is_free = new.is_free,
            required_age = new.required_age,
            metacritic_score = new.metacritic_score,
            platform_windows = new.platform_windows,
            platform_mac = new.platform_mac,
            platform_linux = new.platform_linux,
            recommendations_total = new.recommendations_total,
            details_fetched_at = new.details_fetched_at,
            run_id = new.run_id
        """,
        (
            appid,
            details["app_type"],
            details["release_date"],
            details["release_date_text"],
            details["coming_soon"],
            details["is_free"],
            details["required_age"],
            details["metacritic_score"],
            details["platform_windows"],
            details["platform_mac"],
            details["platform_linux"],
            details["recommendations_total"],
            fetched_at,
            run_id,
        )
    )

    for table, rows in (("genres", details["genres"]),
                        ("categories", details["categories"])):
        if rows:
            id_column = "genre_id" if table == "genres" else "category_id"
            cursor.executemany(
                f"INSERT INTO {table} ({id_column}, description)"
                " VALUES (%s, %s) AS new"
                " ON DUPLICATE KEY UPDATE description = new.description",
                rows
            )

    replace_links(
        cursor,
        "game_genres",
        [appid],
        ("appid", "genre_id"),
        [(appid, genre_id) for genre_id, _ in details["genres"]]
    )
    replace_links(
        cursor,
        "game_categories",
        [appid],
        ("appid", "category_id"),
        [(appid, category_id) for category_id, _ in details["categories"]]
    )

    language_ids = upsert_names(
        cursor,
        "languages",
        "language_id",
        [name for name, _ in details["languages"]]
    )
    language_rows = {}

    for name, full_audio in details["languages"]:
        language_rows.setdefault(
            language_ids[name],
            (appid, language_ids[name], full_audio)
        )

    replace_links(
        cursor,
        "game_languages",
        [appid],
        ("appid", "language_id", "full_audio"),
        list(language_rows.values())
    )


def load_tags(cursor, appid, tags, fetched_at):
    tag_ids = upsert_names(
        cursor,
        "tags",
        "tag_id",
        [name for name, _ in tags]
    )
    rows = {}

    for name, votes in tags:
        rows.setdefault(tag_ids[name], (appid, tag_ids[name], votes))

    replace_links(
        cursor,
        "game_tags",
        [appid],
        ("appid", "tag_id", "votes"),
        list(rows.values())
    )
    cursor.execute(
        "UPDATE game_details SET tags_fetched_at = %s WHERE appid = %s",
        (fetched_at, appid)
    )


def describe_failures(failures):
    listed = "; ".join(
        f"{appid}: {error}" for appid, error in failures[:LISTED_FAILURES]
    )
    more = len(failures) - LISTED_FAILURES
    return listed + (f"; and {more} more" if more > 0 else "")


def refresh_details(ctx, appids):
    """13b: refresh Store details and SteamSpy tags for the stalest games."""
    if ctx.replay:
        chosen = refreshed_in_run(ctx.cursor, ctx.run_id)
    else:
        chosen = select_stalest(
            ctx.cursor,
            appids,
            get_store_details_per_run()
        )

    country = get_store_country()
    fetched_at = ctx.load_time()
    store_failures = []
    tag_failures = []
    unparsed_dates = []

    for appid in chosen:
        ctx.pace("store")

        try:
            payload = fetch_store_app_details(
                ctx.session,
                ctx.store,
                appid,
                country
            )
            details = parse_store_details(appid, payload)
            load_store_details(ctx.cursor, details, ctx.run_id, fetched_at)

            if (
                details["available"]
                and details["release_date_text"]
                and details["release_date"] is None
                and not details["coming_soon"]
            ):
                unparsed_dates.append(
                    (appid, repr(details["release_date_text"]))
                )
        except ENRICHMENT_ERRORS as error:
            store_failures.append((appid, error))

        # The Store pause above usually covers this already; pace() then
        # returns at once instead of adding 1.1 s per game.
        ctx.pace("steamspy")

        try:
            payload = fetch_steamspy_appdetails_payload(
                ctx.session,
                ctx.store,
                appid
            )
            load_tags(
                ctx.cursor,
                appid,
                parse_steamspy_tags(payload),
                fetched_at
            )
        except ENRICHMENT_ERRORS as error:
            tag_failures.append((appid, error))

    if store_failures:
        ctx.warn(
            f"Store details failed for {len(store_failures)} game(s), kept"
            f" previous details: {describe_failures(store_failures)}"
        )

    if tag_failures:
        ctx.warn(
            f"SteamSpy tags failed for {len(tag_failures)} game(s), kept"
            f" previous tags: {describe_failures(tag_failures)}"
        )

    if unparsed_dates:
        ctx.warn(
            f"Unparseable release dates stored as NULL for"
            f" {len(unparsed_dates)} game(s): "
            + describe_failures(unparsed_dates)
        )

    logging.info(
        "Refreshed Store details for %s of %s game(s)",
        len(chosen) - len(store_failures),
        len(chosen)
    )
    return len(chosen) - len(store_failures)
