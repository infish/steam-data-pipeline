# 2. A raw landing layer with hash deduplication

Status: accepted (2026-10)

## Context

In September 2026 SteamSpy values had silently stopped changing, and the
dashboard drew them as a time series. Detecting that, and fixing the parsing
afterwards, required the original responses, which were not kept. Several
sources are undocumented (`appreviewhistogram`) and may change shape.

## Decision

Every API request goes through one helper (`api.steam_api.fetch_json`) that
writes the response to `raw_api_responses` **before** anything parses it;
parsers read the stored copy, not the live response.

- Bodies are zlib-compressed with their SHA-256.
- A body byte-identical to the previous one for the same source and request
  is not stored again; the row points to the original (`is_duplicate_of`).
- Bodies older than `RAW_RETENTION_DAYS` (default 30) are removed; metadata is
  kept forever.
- `reprocess.py --run-id N` replays a run's stored responses through the same
  code, so a parsing fix can be applied to past runs.

## Consequences

- Repeated identical responses are a cheap staleness signal; the duplicate
  rate per source is a data-quality check (`docs/observability.md`).
- Storage stays small: repeats cost a metadata row, and old bodies expire.
- Reprocessing is limited to the retention window and uses the current
  configuration; requests the original run did not make fail like an
  unreachable API.
- Writes to the raw layer use their own autocommit connection, so a run whose
  load rolls back still records what it fetched.
