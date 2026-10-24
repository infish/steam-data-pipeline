"""Raw landing layer: store API responses before anything parses them."""

import hashlib
import json
import zlib
from collections import Counter
from datetime import timedelta


COMPRESSION_LEVEL = 6

# error_message of a row recording an attempt the HTTP session retried
# (since v1.3.0). Such a row counts as a request, but it is not the answer:
# replaying a run, or listing what it fetched, must skip it.
RETRIED_PREFIX = "retried: "
# For parameterised queries (% is doubled for the driver's formatting).
NOT_RETRIED_SQL = (
    "(error_message IS NULL OR error_message NOT LIKE 'retried:%%')"
)


def canonical_params(params):
    return json.dumps(
        params or {},
        sort_keys=True,
        separators=(",", ":"),
        default=str
    )


def request_hash(endpoint, params):
    """Identify a request by endpoint and parameters.

    The endpoint is part of the hash because some requests differ only in
    the URL path (for example appreviews/<appid>).
    """
    key = f"{endpoint}?{canonical_params(params)}"
    return hashlib.sha256(key.encode()).hexdigest()


def payload_sha256(body):
    return hashlib.sha256(body).hexdigest()


class MemoryRawStore:
    """In-memory stand-in for RawStore, for unit tests and dry runs."""

    def __init__(self):
        self.rows = []
        self.stats = Counter()

    def record(self, source, endpoint, params, response=None, error=None):
        body = response.content if response is not None else None
        self.rows.append({
            "response_id": len(self.rows) + 1,
            "source": source,
            "endpoint": endpoint,
            "params": params,
            "http_status": (
                response.status_code if response is not None else None
            ),
            "body": body,
            "error": error,
        })
        self.stats["stored" if body is not None else "failed"] += 1
        return len(self.rows)

    def read_payload(self, response_id):
        return self.rows[response_id - 1]["body"]


class RawStore:
    """Writes raw_api_responses rows on its own autocommit connection.

    Raw rows survive a run whose load is rolled back, because the landing
    layer records what was fetched, not what was loaded.
    """

    def __init__(self, connection, run_id, now_fn):
        self.connection = connection
        self.connection.autocommit = True
        self.run_id = run_id
        self.now_fn = now_fn
        self.stats = Counter()

    def record(self, source, endpoint, params, response=None, error=None):
        body = response.content if response is not None else None
        status = response.status_code if response is not None else None
        params_hash = request_hash(endpoint, params)
        sha = payload_sha256(body) if body is not None else None
        duplicate_of = (
            self._find_original(source, params_hash, sha)
            if sha is not None else None
        )
        payload = (
            zlib.compress(body, COMPRESSION_LEVEL)
            if body is not None and duplicate_of is None else None
        )

        cursor = self.connection.cursor()
        cursor.execute(
            """
            INSERT INTO raw_api_responses (
                run_id, source, endpoint, request_params,
                request_params_hash, http_status, fetched_at, payload,
                payload_sha256, payload_bytes, is_duplicate_of, error_message
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                self.run_id, source, endpoint, canonical_params(params),
                params_hash, status, self.now_fn(), payload, sha,
                len(body) if body is not None else None, duplicate_of,
                error[:2000] if error else None
            )
        )
        response_id = cursor.lastrowid
        cursor.close()

        if body is None:
            self.stats["failed"] += 1
        elif duplicate_of is not None:
            self.stats["duplicate"] += 1
        else:
            self.stats["stored"] += 1

        return response_id

    def _find_original(self, source, params_hash, sha):
        """Return the row holding an identical body, if the latest one is."""
        cursor = self.connection.cursor()
        cursor.execute(
            """
            SELECT response_id, payload_sha256, is_duplicate_of
            FROM raw_api_responses
            WHERE source = %s
              AND request_params_hash = %s
              AND payload_sha256 IS NOT NULL
            ORDER BY fetched_at DESC, response_id DESC
            LIMIT 1
            """,
            (source, params_hash)
        )
        rows = cursor.fetchall()

        if not rows or rows[0][1] != sha:
            cursor.close()
            return None

        previous_id, _, previous_original = rows[0]
        original_id = previous_original or previous_id
        cursor.execute(
            "SELECT payload IS NOT NULL FROM raw_api_responses"
            " WHERE response_id = %s",
            (original_id,)
        )
        has_body = cursor.fetchall()[0][0] == 1
        cursor.close()

        # A purged original cannot serve the duplicate; store the body again.
        return original_id if has_body else None

    def read_payload(self, response_id):
        return read_payload(self.connection, response_id)


def read_payload(connection, response_id):
    """Return the uncompressed body of a response, following duplicates."""
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            SELECT
                COALESCE(original.payload, raw.payload),
                raw.payload_sha256,
                raw.payload_purged_at,
                original.payload_purged_at
            FROM raw_api_responses AS raw
            LEFT JOIN raw_api_responses AS original
                ON original.response_id = raw.is_duplicate_of
            WHERE raw.response_id = %s
            """,
            (response_id,)
        )
        rows = cursor.fetchall()
    finally:
        cursor.close()

    if not rows:
        raise LookupError(f"raw response {response_id} does not exist")

    payload, sha, purged_at, original_purged_at = rows[0]

    if payload is None:
        if sha is None:
            return None

        raise LookupError(
            f"raw response {response_id} payload was purged"
            f" at {purged_at or original_purged_at}"
        )

    return zlib.decompress(bytes(payload))


def purge_raw_payloads(cursor, retention_days, now):
    """Drop payload bodies older than the retention window; keep metadata.

    A body that a newer duplicate still points to is kept until that
    duplicate is old enough too, so recent runs stay reprocessable.
    """
    cutoff = now - timedelta(days=retention_days)
    # Candidates are originals whose body is still stored (index
    # idx_raw_unpurged_originals), and only duplicates inside the window can
    # keep one: both scans stay the size of the retention window, not of the
    # whole history (before v1.3.0 both read every row ever stored).
    cursor.execute(
        """
        UPDATE raw_api_responses AS raw
        LEFT JOIN (
            SELECT DISTINCT is_duplicate_of AS original_id
            FROM raw_api_responses
            WHERE is_duplicate_of IS NOT NULL
              AND fetched_at >= %s
        ) AS recent_refs
            ON recent_refs.original_id = raw.response_id
        SET raw.payload = NULL,
            raw.payload_purged_at = %s
        WHERE raw.is_duplicate_of IS NULL
          AND raw.payload_purged_at IS NULL
          AND raw.fetched_at < %s
          AND raw.payload IS NOT NULL
          AND recent_refs.original_id IS NULL
        """,
        (cutoff, now, cutoff)
    )
    return cursor.rowcount
