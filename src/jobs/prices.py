"""Daily price check with Type 2 price history (game_price_history)."""

import logging

from api.steam_api import (
    ENRICHMENT_ERRORS,
    PRICE_BATCH_SIZE,
    fetch_store_prices,
)
from config import get_store_country
from parsing.store import PRICE_FIELDS, parse_price_batch


COMPARED_FIELDS = ("price_state", *PRICE_FIELDS)


def current_versions(cursor, appids, country):
    if not appids:
        return {}

    placeholders = ", ".join(["%s"] * len(appids))
    cursor.execute(
        f"""
        SELECT price_id, appid, price_state, currency, initial_price,
               final_price, discount_percent
        FROM game_price_history
        WHERE country_code = %s
          AND is_current
          AND appid IN ({placeholders})
        """,
        (country, *appids)
    )
    columns = [column[0] for column in cursor.description]
    return {
        row["appid"]: row
        for row in (dict(zip(columns, values)) for values in cursor.fetchall())
    }


def apply_price_states(cursor, states, country, run_id, observed_at):
    """Open a new version for every changed price; return (opened, same)."""
    current = current_versions(cursor, sorted(states), country)
    opened = 0

    for appid, state in sorted(states.items()):
        version = current.get(appid)

        if version and all(
            version[field] == state[field] for field in COMPARED_FIELDS
        ):
            continue

        if version:
            cursor.execute(
                """
                UPDATE game_price_history
                SET is_current = FALSE,
                    valid_to = %s,
                    closed_by_run_id = %s
                WHERE price_id = %s
                """,
                (observed_at, run_id, version["price_id"])
            )

        cursor.execute(
            """
            INSERT INTO game_price_history (
                appid, country_code, price_state, currency, initial_price,
                final_price, discount_percent, valid_from, is_current,
                opened_by_run_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, TRUE, %s)
            """,
            (
                appid,
                country,
                *(state[field] for field in COMPARED_FIELDS),
                observed_at,
                run_id,
            )
        )
        opened += 1

    return opened, len(states) - opened


def check_prices(ctx, appids):
    """Fetch prices for the catalog in batches and version any changes.

    Price history only moves forward, so reprocessing an older run does not
    replay its price check.
    """
    if ctx.replay:
        return

    country = get_store_country()
    appids = sorted(set(appids))
    states = {}
    failed_batches = []
    missing = []

    for start in range(0, len(appids), PRICE_BATCH_SIZE):
        batch = appids[start:start + PRICE_BATCH_SIZE]
        ctx.pace("store")

        try:
            payload = fetch_store_prices(
                ctx.session,
                ctx.store,
                batch,
                country
            )
            batch_states = parse_price_batch(batch, payload)
        except ENRICHMENT_ERRORS as error:
            failed_batches.append((batch, error))
            continue

        states.update(batch_states)
        missing.extend(appid for appid in batch if appid not in batch_states)

    if failed_batches:
        skipped = sum(len(batch) for batch, _ in failed_batches)
        ctx.warn(
            f"Store price check failed for {len(failed_batches)} batch(es)"
            f" ({skipped} games, prices unchanged): {failed_batches[0][1]}"
        )

    if missing:
        ctx.warn(
            f"Store price answers left out {len(missing)} game(s), prices"
            f" unchanged: {', '.join(map(str, missing[:10]))}"
            + (f" and {len(missing) - 10} more" if len(missing) > 10 else "")
        )

    opened, unchanged = apply_price_states(
        ctx.cursor,
        states,
        country,
        ctx.run_id,
        ctx.load_time()
    )
    logging.info(
        "Price check (%s): %s new price version(s), %s unchanged",
        country,
        opened,
        unchanged
    )
