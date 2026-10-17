"""catalog_daily: the game list and prices.

The game list is every games row: STEAMSPY_PAGES SteamSpy catalog pages
(fetched every STEAMSPY_PAGE_INTERVAL_DAYS, since SteamSpy is frozen),
tracked apps, and games players_hourly added from Valve's charts (ADR 5).
Every run checks prices for all of them; Store details, tags and review
summaries are refreshed hourly by refresh_hourly (ADR 6).
"""

import logging

from api.steam_api import (
    ENRICHMENT_ERRORS,
    get_top_games,
    get_tracked_app_details,
)
from config import (
    get_max_rejected_percent,
    get_raw_retention_days,
    get_steamspy_config,
    get_steamspy_page_interval_days,
    get_steamspy_pages,
    get_tracked_appids,
)
from ingest.raw_store import purge_raw_payloads
from jobs.base import Job
from jobs.dimensions import load_company_links
from jobs.prices import check_prices
from jobs.loading import (
    catalog_appids,
    clean_float,
    clean_int,
    clean_value,
    ensure_games,
)
from quality.data_quality import format_rejections, validate_games_data
from transform.data_transformer import transform_games_data


LOGGED_REJECTIONS_LIMIT = 20


def load_games(cursor, df, observed_at):
    # A Store name is current and SteamSpy's is frozen (ADR 5), so SteamSpy
    # never replaces a name that came from the Store.
    query = """
        INSERT INTO games (
            appid,
            name,
            developer,
            publisher,
            languages,
            genre,
            first_seen_at,
            last_seen_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)

        ON DUPLICATE KEY UPDATE
            name = IF(name_source <> 'store'
                          AND VALUES(last_seen_at) >= last_seen_at,
                      VALUES(name), name),
            name_source = IF(name_source = 'store', 'store', 'steamspy'),
            developer = IF(VALUES(last_seen_at) >= last_seen_at,
                           VALUES(developer), developer),
            publisher = IF(VALUES(last_seen_at) >= last_seen_at,
                           VALUES(publisher), publisher),
            languages = IF(VALUES(last_seen_at) >= last_seen_at,
                           VALUES(languages), languages),
            genre = IF(VALUES(last_seen_at) >= last_seen_at,
                       VALUES(genre), genre),
            first_seen_at = LEAST(first_seen_at, VALUES(first_seen_at)),
            last_seen_at = GREATEST(last_seen_at, VALUES(last_seen_at))
    """

    values = [
        (
            int(row.appid),
            row.name,
            clean_value(row.developer),
            clean_value(row.publisher),
            clean_value(row.languages),
            clean_value(row.genre),
            observed_at,
            observed_at
        )
        for row in df.itertuples(index=False)
    ]

    cursor.executemany(query, values)
    return len(values)


def load_game_metric_snapshots(cursor, df, run_id, snapshot_time):
    query = """
        INSERT INTO game_metric_snapshots (
            run_id,
            appid,
            snapshot_time,
            owners,
            owners_low,
            owners_high,
            estimated_owners,
            positive_reviews,
            negative_reviews,
            total_reviews,
            review_score_percent,
            average_playtime_forever_minutes,
            average_playtime_2weeks_minutes,
            median_playtime_forever_minutes,
            median_playtime_2weeks_minutes,
            ccu,
            price_cents,
            initial_price_cents,
            discount_percent
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """

    values = [
        (
            run_id,
            int(row.appid),
            snapshot_time,
            row.owners,
            clean_int(row.owners_low),
            clean_int(row.owners_high),
            clean_int(row.estimated_owners),
            clean_int(row.positive_reviews),
            clean_int(row.negative_reviews),
            clean_int(row.total_reviews),
            clean_float(row.review_score_percent),
            clean_int(row.average_playtime_forever_minutes),
            clean_int(row.average_playtime_2weeks_minutes),
            clean_int(row.median_playtime_forever_minutes),
            clean_int(row.median_playtime_2weeks_minutes),
            clean_int(row.ccu),
            clean_int(row.price_cents),
            clean_int(row.initial_price_cents),
            clean_int(row.discount_percent)
        )
        for row in df.itertuples(index=False)
    ]

    cursor.executemany(query, values)
    return len(values)


def build_games_list(data):
    games_list = []

    for game_id, game_data in data.items():
        games_list.append({
            "appid": int(game_id),
            "name": game_data.get("name"),
            "developer": game_data.get("developer"),
            "publisher": game_data.get("publisher"),
            "owners": game_data.get("owners"),
            "positive_reviews": game_data.get("positive"),
            "negative_reviews": game_data.get("negative"),
            "average_playtime_forever_minutes": game_data.get("average_forever"),
            "average_playtime_2weeks_minutes": game_data.get("average_2weeks"),
            "median_playtime_forever_minutes": game_data.get("median_forever"),
            "median_playtime_2weeks_minutes": game_data.get("median_2weeks"),
            "ccu": game_data.get("ccu"),
            "price_cents": game_data.get("price"),
            "initial_price_cents": game_data.get("initialprice"),
            "discount_percent": game_data.get("discount"),
            "languages": game_data.get("languages"),
            "genre": game_data.get("genre")
        })

    return games_list


def steamspy_page_due(ctx):
    """Whether this run fetches the SteamSpy page.

    With STEAMSPY_PAGE_INTERVAL_DAYS 1, every catalog run fetches it, as
    before v1.2.0. Otherwise it is due when the newest SteamSpy snapshot is
    at least that many calendar days old, when there is none, or when the
    run that loaded it did not ask for every page STEAMSPY_PAGE and
    STEAMSPY_PAGES now cover (the setting was raised, so the larger catalog
    arrives with the next run, not a week later). Reprocessing does what
    the original run did.
    """
    if ctx.replay:
        ctx.cursor.execute(
            "SELECT COUNT(*) FROM raw_api_responses"
            " WHERE run_id = %s AND source = 'steamspy_all'",
            (ctx.run_id,)
        )
        return ctx.cursor.fetchall()[0][0] > 0

    interval_days = get_steamspy_page_interval_days()

    if interval_days == 1:
        return True

    # The newest snapshot, and the last page its run asked SteamSpy for
    # (request metadata stays after payloads are purged).
    ctx.cursor.execute(
        """
        SELECT
            (SELECT MAX(snapshot_time) FROM game_metric_snapshots),
            (SELECT MAX(CAST(JSON_UNQUOTE(
                        JSON_EXTRACT(request_params, '$.page')) AS UNSIGNED))
             FROM raw_api_responses
             WHERE source = 'steamspy_all'
               AND run_id = (SELECT MAX(run_id) FROM game_metric_snapshots))
        """
    )
    newest, last_page = ctx.cursor.fetchall()[0]

    if newest is None:
        return True

    config = get_steamspy_config()
    wanted_last_page = config["page"] + get_steamspy_pages() - 1

    if last_page is None or last_page < wanted_last_page:
        return True

    return (ctx.now_fn().date() - newest.date()).days >= interval_days


def load_steamspy_page(ctx):
    """Fetch, validate and load the SteamSpy page; return the counters."""
    data = {}
    first_page = get_steamspy_config()["page"]

    with ctx.step("fetch_catalog"):
        for page in range(first_page, first_page + get_steamspy_pages()):
            # SteamSpy allows one page a minute; the page also counts
            # towards its 1-per-second interval for appdetails.
            ctx.pace("steamspy_all")
            ctx.pace("steamspy")

            try:
                data.update(get_top_games(ctx.session, ctx.store, page))
            except ENRICHMENT_ERRORS as error:
                # SteamSpy is a frozen reference (ADR 5): a missing page is
                # a warning, not a failed run; prices still run.
                ctx.warn(f"SteamSpy catalog page {page} failed: {error}")

    if not data:
        return 0, 0, 0

    # Tracked apps off the page get a SteamSpy row from appdetails.
    off_page_appids = [
        appid
        for appid in get_tracked_appids()
        if str(appid) not in data
    ]

    with ctx.step("backfill_tracked"):
        backfilled_rows, backfill_warnings = get_tracked_app_details(
            ctx.session,
            off_page_appids,
            store=ctx.store,
            pace=lambda: ctx.pace("steamspy")
        )
    data = {**data, **backfilled_rows}
    ctx.warn(*backfill_warnings)

    if backfilled_rows:
        logging.info(
            "Backfilled %s tracked app(s) missing from the SteamSpy page: %s",
            len(backfilled_rows),
            ", ".join(backfilled_rows)
        )

    with ctx.step("validate"):
        df = transform_games_data(build_games_list(data))
        rows_extracted = len(df)
        df, rejected = validate_games_data(df, get_max_rejected_percent())

    if rejected:
        logging.warning(
            "Rejected %s of %s SteamSpy rows; first %s: %s",
            len(rejected),
            rows_extracted,
            min(len(rejected), LOGGED_REJECTIONS_LIMIT),
            format_rejections(rejected, LOGGED_REJECTIONS_LIMIT)
        )
        ctx.warnings.append(
            f"Rejected {len(rejected)} of {rows_extracted} SteamSpy "
            "rows that failed validation (app IDs: "
            + ", ".join(str(appid) for appid, _ in rejected[:10])
            + (", ..." if len(rejected) > 10 else "")
            + ")"
        )

    ingestion_time = ctx.load_time()

    with ctx.step("load_games") as step:
        rows_loaded = load_games(ctx.cursor, df, ingestion_time)
        step["rows"] = rows_loaded

    with ctx.step("load_snapshots") as step:
        load_game_metric_snapshots(
            ctx.cursor, df, ctx.run_id, ingestion_time
        )
        step["rows"] = len(df)

    with ctx.step("load_company_links"):
        load_company_links(ctx.cursor, df)

    return rows_extracted, rows_loaded, len(rejected)


def run_catalog(ctx):
    rows_extracted = rows_loaded = rows_rejected = 0

    if steamspy_page_due(ctx):
        rows_extracted, rows_loaded, rows_rejected = load_steamspy_page(ctx)
    else:
        logging.info(
            "SteamSpy page not due (every %s days); skipped",
            get_steamspy_page_interval_days()
        )

    with ctx.step("add_tracked") as step:
        added = ensure_games(
            ctx.cursor, get_tracked_appids(), "tracked", ctx.load_time()
        )
        step["rows"] = len(added)

    # Store details, tags and review summaries are refreshed in hourly slices
    # by refresh_hourly (ADR 6); the daily job keeps the cheap price batches.
    with ctx.step("prices"):
        check_prices(ctx, catalog_appids(ctx.cursor))

    ctx.counters.update(
        rows_extracted=rows_extracted,
        rows_loaded=rows_loaded,
        rows_rejected=rows_rejected
    )

    if not ctx.replay:
        with ctx.step("raw_retention"):
            purge_expired_payloads(ctx)


def purge_expired_payloads(ctx):
    """Raw retention runs once a day, at the end of the catalog job."""
    retention_days = get_raw_retention_days()

    try:
        purged = purge_raw_payloads(ctx.cursor, retention_days, ctx.now_fn())
    except Exception as error:
        ctx.warn(f"Raw payload retention cleanup failed: {error}")
        return

    if purged:
        logging.info(
            "Purged %s raw payload bodies older than %s days",
            purged,
            retention_days
        )


def clear_catalog_run(cursor, run_id):
    cursor.execute(
        "DELETE FROM game_metric_snapshots WHERE run_id = %s",
        (run_id,)
    )


CATALOG_JOB = Job(
    name="catalog_daily",
    body=run_catalog,
    clear_run=clear_catalog_run,
    load_time_sql=(
        "SELECT MIN(snapshot_time) FROM game_metric_snapshots"
        " WHERE run_id = %s"
    ),
    reaps=("daily_legacy", "daily")
)
