"""Resumable, paced SteamSpy catalog importer for the isolated staging DB."""

import argparse
import hashlib
import json
import os
import re
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

from api.steam_api import STEAMSPY_URL, create_bulk_session, get_steamspy_page
from database.loaders import load_game_metric_snapshots, load_games
from database.mysql_database import get_connection
from transform.data_transformer import transform_games_data

MARKER_VALUE = "steam-data-pipeline-isolated-benchmark"


def utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def validate_catalog_target(connection):
    if connection.database != "steam_catalog_stage":
        raise RuntimeError("catalog importer requires steam_catalog_stage")
    cursor = connection.cursor()
    cursor.execute("SELECT marker_value FROM benchmark_database_marker WHERE marker_key='purpose'")
    if cursor.fetchone() != (MARKER_VALUE,):
        raise RuntimeError("staging benchmark marker is missing")
    cursor.close()


def validate_record(source_key, raw):
    if not isinstance(raw, dict):
        raise ValueError("record is not an object")
    try:
        appid = int(raw.get("appid", source_key))
    except (TypeError, ValueError) as error:
        raise ValueError("appid is not an integer") from error
    if not 0 < appid <= 4_294_967_295:
        raise ValueError("appid is outside unsigned integer range")
    name = raw.get("name")
    if not isinstance(name, str) or not name.strip() or len(name) > 255:
        raise ValueError("name is missing or exceeds 255 characters")
    for field in ("developer", "publisher", "genre"):
        value = raw.get(field)
        if value is not None and (not isinstance(value, str) or len(value) > 500):
            raise ValueError(f"{field} exceeds schema bounds")
    owners = raw.get("owners")
    match = re.fullmatch(r"\s*([0-9,]+)\s*\.\.\s*([0-9,]+)\s*", str(owners))
    if not match:
        raise ValueError("owners range is invalid")
    owners_low, owners_high = (int(value.replace(",", "")) for value in match.groups())
    if owners_low > owners_high:
        raise ValueError("owners range is reversed")
    numeric_fields = (
        "positive", "negative", "average_forever", "average_2weeks",
        "median_forever", "median_2weeks", "ccu", "price", "initialprice",
        "discount",
    )
    for field in numeric_fields:
        value = raw.get(field)
        if value is None:
            continue
        try:
            parsed = int(value)
        except (TypeError, ValueError) as error:
            raise ValueError(f"{field} is not an integer") from error
        if parsed < 0 or (field == "discount" and parsed > 100):
            raise ValueError(f"{field} is outside schema bounds")
    return {
        "appid": appid, "name": name, "developer": raw.get("developer"),
        "publisher": raw.get("publisher"), "owners": raw.get("owners"),
        "positive_reviews": raw.get("positive"), "negative_reviews": raw.get("negative"),
        "average_playtime_forever_minutes": raw.get("average_forever"),
        "average_playtime_2weeks_minutes": raw.get("average_2weeks"),
        "median_playtime_forever_minutes": raw.get("median_forever"),
        "median_playtime_2weeks_minutes": raw.get("median_2weeks"),
        "ccu": raw.get("ccu"), "price_cents": raw.get("price"),
        "initial_price_cents": raw.get("initialprice"),
        "discount_percent": raw.get("discount"), "languages": raw.get("languages"),
        "genre": raw.get("genre"),
    }


def write_cache(cache_dir, import_id, page, payload, budget_bytes):
    payload_bytes = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    digest = hashlib.sha256(payload_bytes).hexdigest()
    envelope = {"import_id": import_id, "page": page, "source_url": STEAMSPY_URL,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "payload_hash": digest, "payload": payload}
    encoded = json.dumps(envelope, sort_keys=True, separators=(",", ":")).encode()
    cache_dir.mkdir(parents=True, exist_ok=True)
    used = sum(path.stat().st_size for path in cache_dir.glob("*.json"))
    if used + len(encoded) > budget_bytes:
        raise RuntimeError("catalog cache budget exceeded")
    target = cache_dir / f"import-{import_id}-page-{page:03d}-{digest}.json"
    temporary = target.with_suffix(".tmp")
    with temporary.open("xb") as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())
    temporary.replace(target)
    return digest


def read_cache(cache_dir, import_id, page):
    matches = sorted(cache_dir.glob(f"import-{import_id}-page-{page:03d}-*.json"))
    if not matches:
        return None
    if len(matches) != 1:
        raise RuntimeError(f"ambiguous cache entries for page {page}")
    envelope = json.loads(matches[0].read_text())
    if envelope.get("import_id") != import_id or envelope.get("page") != page:
        raise RuntimeError(f"cache identity mismatch for page {page}")
    payload = envelope.get("payload")
    if not isinstance(payload, dict):
        raise RuntimeError(f"cache payload type mismatch for page {page}")
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    digest = hashlib.sha256(encoded).hexdigest()
    if digest != envelope.get("payload_hash") or digest not in matches[0].name:
        raise RuntimeError(f"cache checksum mismatch for page {page}")
    return payload, digest


def wait_for_attempt_slot(connection, minimum_seconds=65, sleep_fn=time.sleep):
    cursor = connection.cursor()
    cursor.execute("SELECT MAX(last_attempt_at) FROM catalog_imports")
    last = cursor.fetchone()[0]
    if last:
        remaining = minimum_seconds - (utc_now() - last).total_seconds()
        if remaining > 0:
            connection.rollback()
            sleep_fn(remaining)
            cursor.close()
            return wait_for_attempt_slot(connection, minimum_seconds, sleep_fn)
    cursor.close()


def retry_after_seconds(value, now=None):
    if not value:
        return 0
    try:
        return max(0, int(value))
    except ValueError:
        try:
            target = parsedate_to_datetime(value)
        except (TypeError, ValueError, OverflowError):
            return 0
        if target.tzinfo is None:
            target = target.replace(tzinfo=timezone.utc)
        return max(0, int((target - (now or datetime.now(timezone.utc))).total_seconds()))


def fetch_with_retries(connection, import_id, page, session, retries=3, sleep_fn=time.sleep):
    last_error = None
    for _ in range(retries):
        wait_for_attempt_slot(connection, sleep_fn=sleep_fn)
        cursor = connection.cursor()
        cursor.execute("UPDATE catalog_imports SET last_attempt_at=%s WHERE import_id=%s",
                       (utc_now(), import_id))
        connection.commit()
        cursor.close()
        try:
            return get_steamspy_page(page, session=session)
        except Exception as error:
            last_error = error
            response = getattr(error, "response", None)
            retry_after = response.headers.get("Retry-After") if response is not None else None
            if retry_after:
                sleep_fn(retry_after_seconds(retry_after))
    raise RuntimeError(f"SteamSpy page {page} failed after {retries} paced attempts") from last_error


def import_catalog(args):
    connection = get_connection()
    lock_cursor = connection.cursor()
    validate_catalog_target(connection)
    lock_cursor.execute("SELECT GET_LOCK('steam-catalog-import',0)")
    if lock_cursor.fetchone() != (1,):
        raise RuntimeError("another catalog importer is active")
    session = create_bulk_session()
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT * FROM catalog_imports WHERE import_key=%s", (args.import_key,))
        item = cursor.fetchone()
        if not item:
            cursor.execute("INSERT INTO pipeline_runs (started_at,status) VALUES (%s,'RUNNING')", (utc_now(),))
            run_id = cursor.lastrowid
            cursor.execute("INSERT INTO catalog_imports (import_key,state,requested_pages,started_at,run_id) VALUES (%s,'RUNNING',%s,%s,%s)",
                           (args.import_key, args.pages, utc_now(), run_id))
            import_id = cursor.lastrowid
            connection.commit()
            item = {"import_id": import_id, "run_id": run_id, "next_page": 0}
        elif item["requested_pages"] != args.pages:
            raise RuntimeError("import page-limit configuration conflict")
        empty_candidate = False
        for page in range(item["next_page"], args.pages):
            cache_dir = Path(args.cache_dir)
            cached = read_cache(cache_dir, item["import_id"], page)
            if cached:
                payload, digest = cached
            else:
                payload = fetch_with_retries(connection, item["import_id"], page, session)
                digest = None
            if not payload:
                if not empty_candidate:
                    empty_candidate = True
                    payload = fetch_with_retries(connection, item["import_id"], page, session)
                if not payload:
                    reason = f"confirmed empty page {page}; API-observed coverage only"
                    cursor.execute("UPDATE catalog_imports SET termination_reason=%s,next_page=%s WHERE import_id=%s",
                                   (reason, page, item["import_id"]))
                    connection.commit()
                    break
            empty_candidate = False
            if digest is None:
                digest = write_cache(cache_dir, item["import_id"], page, payload, args.cache_budget)
            accepted, rejected = [], []
            for source_key, raw in payload.items():
                try:
                    accepted.append(validate_record(source_key, raw))
                except ValueError as error:
                    rejected.append((str(source_key)[:100], str(error)[:255]))
            df = transform_games_data(accepted) if accepted else None
            transaction = connection.cursor()
            unique_rows, duplicates = [], 0
            for row in ([] if df is None else df.to_dict("records")):
                transaction.execute("INSERT IGNORE INTO catalog_seen_appids (import_id,appid,first_page) VALUES (%s,%s,%s)",
                                    (item["import_id"], int(row["appid"]), page))
                if transaction.rowcount == 1:
                    unique_rows.append(row)
                else:
                    duplicates += 1
            if unique_rows:
                observed = utc_now()
                load_games(transaction, unique_rows, observed)
                load_game_metric_snapshots(transaction, unique_rows, item["run_id"], observed)
            for source_key, reason in rejected:
                transaction.execute("INSERT INTO catalog_rejections (import_id,page_number,source_key,reason) VALUES (%s,%s,%s,%s)",
                                    (item["import_id"], page, source_key, reason))
            transaction.execute("""
              INSERT INTO catalog_pages
                (import_id,page_number,source_url,fetched_at,payload_hash,raw_count,
                 accepted_count,duplicate_count,rejected_count)
              VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (item["import_id"], page, f"{STEAMSPY_URL}?request=all&page={page}",
                  utc_now(), digest, len(payload), len(unique_rows), duplicates, len(rejected)))
            transaction.execute("""
              UPDATE catalog_imports SET next_page=%s,raw_rows=raw_rows+%s,
                unique_rows=unique_rows+%s,duplicate_rows=duplicate_rows+%s,
                rejected_rows=rejected_rows+%s WHERE import_id=%s
            """, (page + 1, len(payload), len(unique_rows), duplicates, len(rejected), item["import_id"]))
            connection.commit()
            transaction.close()
        cursor.execute("SELECT rejected_rows,unique_rows FROM catalog_imports WHERE import_id=%s", (item["import_id"],))
        stats = cursor.fetchone()
        rejected_count = stats["rejected_rows"]
        unique_count = stats["unique_rows"]
        reason = "requested bounded pages completed; API-observed coverage only"
        cursor.execute("UPDATE catalog_imports SET state='SUCCESS',finished_at=%s,termination_reason=COALESCE(termination_reason,%s) WHERE import_id=%s",
                       (utc_now(), reason, item["import_id"]))
        cursor.execute("UPDATE pipeline_runs SET status='SUCCESS',finished_at=%s,rows_extracted=%s,rows_loaded=%s,error_message=%s WHERE run_id=%s",
                       (utc_now(), unique_count + rejected_count, unique_count,
                        "quality warnings present" if rejected_count else None, item["run_id"]))
        connection.commit()
        cursor.close()
    finally:
        session.close()
        try:
            lock_cursor.execute("SELECT RELEASE_LOCK('steam-catalog-import')")
            lock_cursor.fetchone()
        finally:
            lock_cursor.close()
            connection.close()


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--import-key", required=True)
    parser.add_argument("--pages", type=int, default=10)
    parser.add_argument("--full", action="store_true")
    parser.add_argument("--cache-dir", default="/artifacts/catalog-cache")
    parser.add_argument("--cache-budget", type=int, default=512 * 1024 * 1024)
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    if args.full:
        args.pages = min(args.pages if args.pages != 10 else 200, 200)
    if not 1 <= args.pages <= 200:
        raise SystemExit("pages must be between 1 and 200")
    import_catalog(args)


if __name__ == "__main__":
    main()
