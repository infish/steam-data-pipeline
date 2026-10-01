# steam-data-pipeline — Fix Plan for an Agent

Repo: https://github.com/infish/steam-data-pipeline
Source: code review of 2026-10-01 (`steam-data-pipeline-review.md` in this project).

How to use: give the agent this file and say **"Execute Phase N only."** Start with Phase 0. After each phase the agent stops, reports back, and waits for the human to verify and approve.

The file has two parts:

- **Part 1 (Phases 0–9):** fix the bugs found in the review.
- **Part 2 (Phases 10–19):** grow the project into a richer data-engineering showcase, with more data sources, a layered data model, derived metrics, and data observability. Part 2 assumes Phases 2–5 of Part 1 are merged.

---

## Global rules for the agent

1. **One phase per branch and PR.** Branch name: `fix/phase-N-short-name`, created from up-to-date `main`. Never push to `main` directly. Never force-push.
2. **Stop at the end of every phase.** Report:
   - what changed, file by file;
   - the exact test commands you ran and their real output;
   - anything you could not verify.
   Then wait. Do not start the next phase on your own.
3. **Never claim a test passed that you did not run.** If Docker or MySQL isn't available in your environment, say so, and leave integration verification to CI or the human.
4. **Migrations:**
   - Never edit `V1`, `V2` or `V3`.
   - Schema changes go in the next free version number (`V4__…`, `V5__…`).
   - `R__analytics_views.sql` may be edited; it is repeatable.
5. **Never touch real data.** Never run `docker compose down -v`. Never point anything at the TrueNAS deployment. For local stack tests, always use a throwaway project name: `docker compose -p steam-test …`, and tear it down with `docker compose -p steam-test down -v`.
6. **Keep docs in sync.** Every phase updates `README.md` (if behavior or config changed) and adds a `CHANGELOG.md` entry.
7. **Config parity.** Any new environment variable goes into all three of: `.env.example`, `docker-compose.yml`, and `deploy/truenas-compose.yml`.
8. **Keep scope tight.** No drive-by refactors and no new dependencies unless the phase says so.

Test commands:

```bash
# unit tests (local Python)
PYTHONPATH=src python -m unittest discover -s tests -v

# unit tests (inside the image)
docker compose -p steam-test run --build --rm --no-deps -e PYTHONPATH=/app/src pipeline \
  python -m unittest discover -s tests -v
```

---

## Phase 0 — Baseline (no code changes)

- Clone the repo, install `requirements.txt`, and run the unit tests. Expect 12 passing.
- Check whether Docker is available. Record the answer in your report; later phases depend on it.
- If Docker is available: bring the stack up as `steam-test` and confirm `migrate` exits 0 and the dashboard is healthy. Then tear it down.

**Done when:** you have reported the baseline test result and Docker availability.

---

## Phase 1 — Quick, isolated fixes

1. **DST bug in `src/scheduler.py` → `wait_until`.**
   - The problem: subtracting two datetimes that share a tzinfo uses wall-clock time. On the spring DST change, the 20:00 run fires at 21:00.
   - The fix: compute remaining time as `target.timestamp() - now_fn().timestamp()`.
   - Add a test with a fake clock across the spring-forward night (e.g. `2027-03-27 20:00:01` → `2027-03-28 20:00` Europe/Prague). Assert the first sleep is about 23 h, not 24 h.
   - Add the matching test for the autumn change (about 25 h).
2. **Delete dead code.** Remove `db/init/01_create_dashboard_user.sh` (it is never mounted). Remove the empty `db/init/` folder.
3. **Fix the README project tree.** Add V3, `deploy/` and `.github/`; remove `db/init`.
4. **CI hygiene.**
   - Align `actions/checkout` to the same major version in both workflows.
   - Remove the leftover `refactor/production-ready` branch trigger from `tests.yml`.

**Done when:** unit tests pass, including the 2 new DST tests.

---

## Phase 2 — Integration test harness (real MySQL)

Later phases change transactions and locking, so they need a real database to test against.

- Add `tests/integration/` with an `__init__.py`.
  - Tests are skipped unless `INTEGRATION_DB=1` is set.
  - They read connection settings from the usual `MYSQL_*` env vars.
- Add a fixture that:
  - truncates tables between tests (respecting foreign key order);
  - runs `main.run_pipeline()` with `get_top_games` and `get_steam_measurements` patched to return small canned payloads.
- Write the first tests:
  - a successful run writes `games`, `game_metric_snapshots`, `steam_game_measurements`, and a `SUCCESS` row in `pipeline_runs`;
  - a forced failure after the snapshot insert leaves **no** snapshot or measurement rows, plus a `FAILED` run row (proves the transaction is atomic).
- Add a CI job `integration-tests` in `tests.yml`:
  - a `mysql:8.0` service container;
  - apply migrations with `docker run --network host -v $PWD/db/migrations:/flyway/sql flyway/flyway:13.4.0-alpine -url=… migrate`;
  - run the tests with `INTEGRATION_DB=1`.

**Done when:** the integration tests pass locally (if Docker is available) or are pushed and ready for CI. Say which.

---

## Phase 3 — Run lifecycle robustness

1. **Migration `V4__run_audit_columns.sql`.** Add to `pipeline_runs`:
   - `measurements_loaded INT UNSIGNED NOT NULL DEFAULT 0`
   - `rows_rejected INT UNSIGNED NOT NULL DEFAULT 0`
   - `warning_message TEXT NULL`

   Update `finish_pipeline_run` to write these columns.
2. **Concurrency lock.**
   - Right after connecting, run `SELECT GET_LOCK('steam_pipeline_run', 0)`.
   - If the result is not 1: log "another run is active" and exit cleanly, without creating a run row and without raising.
   - Release the lock in `finally`.
3. **Reap abandoned runs.** After acquiring the lock, mark any existing `RUNNING` rows as `FAILED` with `error_message = 'Abandoned: process ended before completion'`. This is safe because the lock guarantees no other run is live.
4. **Stop the error handler masking the real error.** In `run_pipeline`'s `except` block, wrap the rollback and `finish_pipeline_run(FAILED)` in their own try/except. Log any secondary failure, then always re-raise the **original** exception.

Integration tests to add:
- a second concurrent run exits without creating a run row;
- a stale `RUNNING` row gets reaped;
- `measurements_loaded` is recorded.

Unit test to add:
- simulate `rollback()` raising, and check the original exception propagates.

**Done when:** all unit and integration tests pass.

---

## Phase 4 — Decouple Valve enrichment from the core load

SteamSpy is the core data source; a SteamSpy failure should still fail the run. Valve data is enrichment, so its failures should degrade the run, not abort it.

- Change `get_steam_measurements()` to return `(measurements, warnings)`.
- If the top-100 chart request fails:
  - add a warning;
  - fall back to direct player lookups for all tracked app IDs.
- Per tracked app, handle the player lookup and the review lookup independently. Catch `requests.RequestException`, `ValueError` and `KeyError`, and turn them into warnings.
  - Player lookup OK, reviews fail → keep the row, with review fields set to null.
  - Player lookup fails → skip that app's row (`current_players` is NOT NULL), with a warning.
- In `run_pipeline`, write the joined warnings to `warning_message` (truncated to about 2000 chars). The run status stays `SUCCESS`.
- In the dashboard, show `st.warning` when the latest run has a `warning_message`.

Tests to add:
- chart failure → fallback path is used;
- review failure → row kept with null reviews;
- player failure → row skipped;
- warnings reach `pipeline_runs` (integration).

**Done when:** all tests pass.

---

## Phase 5 — Transform and validation robustness

1. **`transform_games_data`: parse owners safely.** Use `pd.to_numeric(..., errors="coerce")` instead of `.astype(int)`. Then a malformed or null `owners` value becomes NaN that validation can see, instead of a TypeError.
2. **Quarantine bad rows instead of failing the whole run.**
   - Change `validate_games_data(df)` to return `(valid_df, rejected)`, where `rejected` is a list of `(appid, reasons)`.
   - These still raise: an empty frame, missing columns, duplicate app IDs.
   - Row-level problems (null or empty name, null or invalid owners, negative values, review score out of range, totals mismatch) reject only that row.
   - If rejected rows exceed `PIPELINE_MAX_REJECTED_PERCENT` (new env var, default `5`), raise and fail the run.
   - Otherwise:
     - load only the valid rows;
     - write the count to `rows_rejected`;
     - log the first 20 rejected app IDs with their reasons;
     - add a short summary to `warning_message`.
3. Add `PIPELINE_MAX_REJECTED_PERCENT` everywhere required by global rule 7.

Unit tests to add:
- null owners → rejected, not a crash;
- empty name → rejected;
- over the threshold → raises;
- under the threshold → the valid subset is returned.

**Done when:** all tests pass.

---

## Phase 6 — Tracked games outside the SteamSpy page

Today, a tracked app that isn't on SteamSpy page 0 is fetched from Valve and then discarded (there is no `games` row for the foreign key).

- For each tracked app ID missing from the catalog frame, call SteamSpy `request=appdetails&appid=<id>`.
  - Sleep at least 1 s between calls (SteamSpy rate limit).
  - Shape the result exactly like a page row and append it before transform and validation, so the game gets a `games` row and a snapshot.
  - If the call fails, add a warning and skip that app.
- Do **not** backfill top-100 games that aren't on the page. Replace the per-run warning list with a single INFO log: "N top-100 games not in catalog page; skipped".
- Document in the README that `rows_extracted` can now exceed 1,000.

Tests to add (all mocked):
- a missing tracked app gets backfilled and its measurement is stored;
- a backfill failure produces a warning and the run still succeeds.

**Done when:** all tests pass.

---

## Phase 7 — Database accounts and bootstrap (affects deployments)

1. **Separate the runtime writer from the migrator.**
   - Keep `PIPELINE_DB_*` as the schema owner, used only by Flyway. Don't rename it; that would break existing `.env` files.
   - Add `WRITER_DB_USER` / `WRITER_DB_PASSWORD`, granted `SELECT, INSERT, UPDATE ON <db>.*`. Use a database-level grant, because bootstrap runs before the tables exist.
   - Switch the `scheduler` and `pipeline` services to the writer credentials.
2. **Make bootstrap actually repair.**
   - For both the dashboard and writer accounts: `CREATE USER IF NOT EXISTS …` followed by `ALTER USER … IDENTIFIED BY …`, so a rotated password takes effect.
   - Then the grants.
3. **Get passwords off command lines.** In the healthcheck and in bootstrap, pass the password via the `MYSQL_PWD` env var instead of `-p…`.
4. **Guard the SQL interpolation.** Bootstrap fails with a clear error if any password doesn't match `^[A-Za-z0-9_-]+$`. Document this; the README already recommends `openssl rand -hex`.
5. Update both compose files, `.env.example`, and the README.
6. Add a README section **"Upgrading an existing TrueNAS deployment"**: add the writer password to the YAML before updating the app, and explain what bootstrap will do.

**Verify:**
- a fresh `steam-test` stack works end to end;
- changing `DASHBOARD_DB_PASSWORD` and re-running `up` still lets the dashboard connect;
- the writer account cannot `DROP TABLE` (show the error).

**Done when:** the above is shown in the report. **Human checkpoint:** the human tests the upgrade path before merging.

---

## Phase 8 — CI/CD gating and image pinning

1. Make `tests.yml` reusable (`on: workflow_call`, plus its existing triggers).
2. In `publish-image.yml`, call the test workflow as a job and make `publish` depend on it with `needs:`. A failing test then blocks the image.
3. Use `docker/metadata-action` to add a semver tag (`vX.Y.Z`) when the push is a git tag. Keep the `sha` tag. Keep `latest` for main.
4. Change `deploy/truenas-compose.yml` to reference a pinned tag (`:vX.Y.Z`) instead of `:latest`. Document the release procedure in the README: tag, wait for the image, bump the tag in the TrueNAS YAML.

**Done when:** the workflow YAML passes `actionlint` (if available) and the PR shows the gated pipeline. **Human step:** cut the first release tag.

---

## Phase 9 — Dashboard fixes

1. **Caching.** Cache query results with `@st.cache_data(ttl=300)`, keyed on query text and a params tuple. Leave the "Run example" button uncached.
2. **Stop drawing lines across gaps.**
   - Build the series from successful `pipeline_runs` (starting at the first run that has any measurement), `LEFT JOIN`ed to `steam_game_measurements` for the selected app.
   - Runs with no measurement produce NaN, so Plotly breaks the line.
   - For the x value, use `COALESCE(source_measured_at, collected_at, runs.finished_at)`.
   - Put this logic in a pure function in a new `src/dashboard_data.py` so it can be unit-tested.
3. **Duplicate game names.** Have the selector use `appid` as the value, with `format_func` showing `"name (appid)"`.
4. **Pandas warning (optional).** Build DataFrames from a cursor instead of `pd.read_sql` to remove the non-SQLAlchemy warning. No new dependency.

Unit tests to add:
- gap insertion: a missing run produces a NaN row;
- the x-value fallback order is correct.

**Done when:** all tests pass and a `steam-test` stack renders the dashboard. Include a screenshot if the environment allows.

---

## Deferred — needs a human decision, do not implement

- **SteamSpy snapshot growth** (about 1,000 known-stale rows a day). Storing only changed rows would break `current_game_metrics`, which assumes one snapshot per game per run. Options:
  - (a) accept the growth;
  - (b) dedupe and rewrite the views to use the latest snapshot per game;
  - (c) stop collecting SteamSpy metrics and keep only catalog metadata.
- **Plaintext passwords in `deploy/truenas-compose.yml`.** This is a limitation of TrueNAS Custom Apps; revisit if TrueNAS adds secret support.

---
---

# Part 2 — Richer metrics and a data-engineering showcase

## Direction

The project should demonstrate the things a data-engineering reviewer looks for:

- **Data contracts:** each source is documented, with recorded examples of its responses.
- **A raw landing layer:** API responses are stored untouched, so any day can be reprocessed.
- **A layered model:** raw → normalized tables → analytics tables.
- **Incremental, idempotent loads:** re-running a load never duplicates data.
- **Backfills:** history can be loaded after the fact.
- **History tracking (SCD Type 2):** changing values such as price keep their past versions.
- **Request budgets:** API rate limits are planned for, not discovered.
- **Data observability:** freshness, volume, and staleness are monitored.

Every number shown in the GUI must say where it comes from and what its timestamp means. That honesty is already the project's strongest selling point, from the September stale-data incident; Part 2 builds on it.

## Extra rules for Part 2

1. **Undocumented endpoints are probed before they are built on.** Several Steam endpoints below are undocumented (notably `appreviewhistogram`). Phase 10 records real responses first. Later phases must use only the fields confirmed there.
2. **Respect rate limits with a margin.** Plan around:
   - SteamSpy `request=all`: 1 request per minute;
   - other SteamSpy requests: at most 1 per second;
   - Store `api/appdetails`: an informally reported limit of about 200 requests per 5 minutes, so budget **150 per 5 minutes**;
   - Valve Web API: no key is needed for the endpoints used here; keep calls modest.

   Every job has a written request budget (Phase 12).
3. **Never delete or rewrite existing history tables.** New tables may copy data out of old ones. Old tables stay, read-only, until a human decides otherwise.
4. **Every new table** gets:
   - an entry in `docs/data_dictionary.md`;
   - at least one data-quality check (Phase 17).

---

## Phase 10 — Source discovery and data contracts

There are no production code changes in this phase. The human may need to approve network access for the agent.

1. For 2–3 apps (730, 427520, 620), call each candidate endpoint **once** and save the response, trimmed to the relevant fields, under `tests/fixtures/sources/<source>/<appid>.json`:

   | Source | Endpoint | Why we want it |
   | --- | --- | --- |
   | Store app details | `store.steampowered.com/api/appdetails?appids=<id>&cc=<cc>` | Release date, genres, categories, platforms, Metacritic score, price with currency |
   | Store price batch | same, with `appids=<a>,<b>,…&filters=price_overview` | Cheap daily price check for many games. **Confirm that batching works.** |
   | Review histogram | `store.steampowered.com/appreviewhistogram/<id>?l=english` | Monthly/weekly review up/down counts with real source dates. **Undocumented.** |
   | Review summary | existing `appreviews` call | Confirm whether `review_score_desc` (e.g. "Very Positive") is present |
   | Most played (daily peak) | `api.steampowered.com/ISteamChartsService/GetMostPlayedGames/v1/` | Daily peak players and last week's rank. **Confirm the fields.** |
   | News / updates | `api.steampowered.com/ISteamNews/GetNewsForApp/v2/?appid=<id>` | Patch and event dates, used to annotate player charts |
   | SteamSpy app details | `steamspy.com/api.php?request=appdetails&appid=<id>` | User tags with vote counts |

2. Write `docs/sources.md` with one section per source:
   - URL and parameters;
   - the fields used;
   - **timestamp semantics** (source time vs collection time);
   - observed or assumed rate limit;
   - expected freshness;
   - known caveats.
3. If an endpoint fails, is missing a field, or looks unreliable, mark it **"rejected"** and say why. Later phases skip anything rejected.

**Done when:** fixtures and `docs/sources.md` are committed. **Human checkpoint:** approve the list of sources before Phase 11.

---

## Phase 11 — Raw landing layer

1. Add a migration for `raw_api_responses`, with these columns:
   - `response_id`, `run_id`, `source`, `endpoint`;
   - `request_params` (JSON), `http_status`, `fetched_at`;
   - `payload` (compressed `MEDIUMBLOB`) and `payload_sha256`.

   Add an index on `(source, request_params_hash, fetched_at)`.
2. Add one fetch helper that every API call goes through.
   - It writes the raw row **before** any parsing.
   - Parsers then read from the stored payload, not from the live response.
3. **Staleness detection for free.** If a payload's hash equals the previous payload for the same source and params:
   - store the metadata with `is_duplicate_of = <earlier response_id>`;
   - do not store the payload body a second time.
4. **Retention:**
   - delete payload bodies older than `RAW_RETENTION_DAYS` (new env var, default 30);
   - keep the metadata rows (hash, time, status) forever.

   Run this cleanup at the end of the daily job.
5. **Reprocessing:** add `python src/reprocess.py --run-id N`, which rebuilds that run's normalized rows from the stored raw payloads, idempotently.

**Done when:**
- integration tests show a run stores raw rows;
- a repeated identical payload is deduplicated;
- reprocessing a run produces identical normalized rows.

---

## Phase 12 — Split jobs and set their cadence

The current single daily run gives one player data point per day. Valve's top-100 feed is a single cheap request, so hourly collection costs almost nothing and reveals daily play patterns.

1. Add a `job_name` column to `pipeline_runs` (migration; existing rows become `'daily_legacy'`).
   - Make the run lock per job: `GET_LOCK('steam_pipeline:' + job_name)`.
   - Update `latest_successful_run` and the views that depend on it to filter by the catalog job.
2. Split collection into jobs:
   - **`players_hourly`:** Valve top-100 chart, direct lookups for tracked games, plus `GetMostPlayedGames` if Phase 10 accepted it.
   - **`catalog_daily`:** SteamSpy page, tracked-game backfill (Phase 6), store details refresh (Phase 13), price check (Phase 14).
   - **`reviews_daily`:** review summaries and histograms for tracked games (Phase 15), plus news.
3. **Generalize the scheduler.** Keep it home-grown and in-process; no new dependency.
   - It takes a small job table with job name, schedule, and the function to run.
   - Each job has its own next-run time, computed the DST-safe way from Phase 1.
   - Configure schedules via env vars: `JOB_PLAYERS_MINUTE`, `JOB_CATALOG_TIME`, `JOB_REVIEWS_TIME`.
4. **Split the measurement table by grain.**
   - New `player_observations`: `appid`, `run_id`, `collected_at`, `source_measured_at`, `current_players`, `player_rank`, `source` (top100 or direct).
   - New `review_observations`: the review totals, `review_score_desc`, `collected_at`.
   - Copy the existing `steam_game_measurements` rows into both. Leave the old table in place, read-only.
5. **Rollup and retention.**
   - Add `player_daily`: per game and day, peak, average, minimum, and number of observations, rebuilt for the affected days after each hourly run.
   - Delete hourly rows older than `PLAYER_HOURLY_RETENTION_DAYS` (default 90). Daily rollups are kept forever.
6. Write `docs/request_budget.md` with requests per job per day, against each source's limit.

**Done when:**
- the scheduler tests cover multiple jobs and DST;
- integration tests show hourly runs feeding `player_daily`;
- the migrated row counts match the old table.

**Human decision before this phase:** confirm hourly is the cadence you want.

---

## Phase 13 — Normalized dimensions (existing data first, then the Store)

**13a. Normalize what we already have** (no new API calls).

SteamSpy delivers genre, languages, developer and publisher as comma-separated strings. Split them into lookup tables plus bridge tables:

- `genres` + `game_genres`
- `languages` + `game_languages`
- `companies` + `game_developers` + `game_publishers`

Details:
- Trim and deduplicate values, and record the original raw string.
- Unit-test the parser on awkward cases: commas inside company names (e.g. "Valve, Inc."), empty strings, and HTML entities.
- Document any parsing rule that is a heuristic.

**13b. Add store details.**

- New table `game_details`:
  - `type`, `release_date`, `coming_soon`, `is_free`, `required_age`;
  - `metacritic_score`;
  - platform flags (Windows/Mac/Linux);
  - `recommendations_total`;
  - `details_fetched_at`.
- Store categories go into `categories` + `game_categories`.
- SteamSpy user tags go into `tags` + `game_tags`, with the vote count per tag.
- **Incremental refresh:** each `catalog_daily` run refreshes the **N stalest** games (`STORE_DETAILS_PER_RUN`, default 120, within the request budget), so all games are refreshed about every 9 days.
  - New games are refreshed first.
  - Parse release dates defensively (Steam formats vary by locale). An unparseable date becomes NULL plus a warning.

**Done when:**
- the parser unit tests pass;
- integration tests show the bridge tables populated from fixtures;
- a second run refreshes a different set of games.

---

## Phase 14 — Price history (SCD Type 2)

1. Add `game_price_history` with these columns:
   - `appid`, `country_code`, `currency`;
   - `initial_price`, `final_price`, `discount_percent`;
   - `valid_from`, `valid_to`, `is_current`.
2. **Daily check:**
   - use the batched `filters=price_overview` call if Phase 10 confirmed it works; otherwise, read price from the Phase 13b details refresh only;
   - when a game's price differs from its current row, close that row and open a new one; when it is unchanged, write nothing.
3. **Configuration:**
   - `STEAM_STORE_COUNTRY` (default `us`, so it lines up with SteamSpy's US-cent prices);
   - `currency` is stored explicitly, and amounts are never mixed across currencies.
4. **Derived values:**
   - lowest price ever recorded;
   - days since the last discount;
   - whether the game is currently on sale;
   - number of discount events.

**Done when** integration tests cover:
- a price change → a new version;
- no change → no new row;
- a free-to-paid transition;
- a game removed from sale.

---

## Phase 15 — Review history backfill and update events

1. **Review histogram backfill** (only if Phase 10 accepted the endpoint).
   - New table `review_rollups`: `appid`, `rollup_type` (month/week), `period_start`, `recommendations_up`, `recommendations_down`, `fetched_at`.
   - Upsert idempotently on `(appid, rollup_type, period_start)`.
   - This gives real, source-dated review history going back years, with no fabrication.
2. **Reconciliation check.** Compare the sum of the rollups with the latest `review_observations` totals, within a documented tolerance. The two endpoints may filter reviews differently; record the difference rather than forcing them to match.
3. **News and update events.**
   - New table `game_events`: `appid`, `gid` (unique), `title`, `event_time` (source time), `feed_label`, `is_patch_notes` (use a tag check if Phase 10 confirmed one exists, otherwise a documented heuristic).
   - Incremental: only insert `gid`s not seen before.
4. A one-off `python src/backfill.py --source review_histogram --appids …` command, also run inside `reviews_daily`.

**Done when:**
- running the backfill twice produces no duplicates;
- the reconciliation result is stored;
- events are deduplicated by `gid`.

---

## Phase 16 — Analytics layer (derived metrics)

MySQL has no materialized views. Instead, use **mart tables** rebuilt by SQL files in `db/transforms/` (numbered, run in order). A `build_marts` step runs them after each job, each in its own transaction. Every mart is documented with its grain, sources, and refresh trigger.

Metrics, mostly computed from data already collected:

- **Players** (`mart_player_trends`):
  - daily peak and average;
  - 7-day rolling average;
  - day-over-day and week-over-week % change;
  - rank movement;
  - days in the top 100, and entries into / exits from it;
  - typical peak hour of the day (Prague time; needs hourly data);
  - volatility (coefficient of variation over 7 days).
- **Reviews** (`mart_review_trends`):
  - new reviews per day (difference between consecutive totals);
  - positive share of the last 30 days vs lifetime, i.e. **sentiment drift**;
  - the review-score label over time.

  Uses the histogram backfill where available.
- **Price** (`mart_price_status`):
  - current vs lowest price;
  - discount depth;
  - days since last sale.
- **Catalog** (`mart_genre_summary`, `mart_tag_summary`):
  - by genre and by tag: game count, average review score, total top-100 players, release-year groups, and platform coverage.
- **Engagement** (`mart_engagement`), clearly labelled as rough:
  - peak players ÷ estimated owners;
  - median vs average playtime skew, from the **latest** SteamSpy snapshot only (not as a time series).
- **Update impact** (`mart_event_impact`):
  - average players in the 7 days after a patch event vs the 7 days before, per event;
  - labelled as correlation, not proof of cause.

Every mart gets SQL tests run after it builds: primary-key uniqueness, not-null on key columns, row count > 0, and value ranges (e.g. shares between 0 and 100).

**Done when:**
- marts build against the fixture data in integration tests;
- a rebuild is idempotent (same row counts and checksums).

**Deferred decision:** whether to move the transforms to dbt. The community `dbt-mysql` adapter's maintenance status should be checked first; switching to Postgres is the other option.

---

## Phase 17 — Data observability

1. New table `data_quality_results`: `check_name`, `target`, `run_id`, `status` (pass/warn/fail), `observed_value`, `threshold`, `checked_at`.
2. Checks:
   - **Freshness:** the newest successful observation per source is younger than its expected interval × 1.5.
   - **Volume:** today's row count per table is within ±30% of the median of the last 7 days.
   - **Staleness rate:** the % of SteamSpy fields unchanged run-over-run, plus the raw-payload duplicate rate per source (from Phase 11). This turns the September incident into a permanent monitor.
   - **Schema drift:** fields expected per `docs/sources.md` are missing from the newest raw payload.
   - The mart tests from Phase 16 also write here.
3. A `fail` result never deletes data. It marks the run with a warning, and the dashboard shows it.

**Done when** integration tests trigger each check type with crafted data.

---

## Phase 18 — Dashboard redesign

Use Streamlit multipage (`src/pages/`). Every chart has a caption giving its source and timestamp meaning.

1. **Overview:**
   - KPIs: games tracked, players now (Valve), sources healthy (n of m);
   - the top movers by week-over-week player change.
2. **Game detail** (choose a game):
   - hourly and daily players with the 7-day average;
   - rank history;
   - patch events drawn as vertical markers;
   - review sentiment drift;
   - price history as a step chart;
   - tags, genres, and platform badges.
3. **Market explorer:**
   - genre and tag summaries;
   - release-year groups;
   - free vs paid, using Store data.
4. **Pipeline health:**
   - run history by job (with durations);
   - freshness per source;
   - staleness rate over time;
   - rejected rows;
   - recent data-quality failures;
   - request budget used vs available.

Keep the chart-gap and caching rules from Phase 9.

**Done when** all pages render against a `steam-test` stack seeded by a fixture-replay command (`python src/reprocess.py --seed-fixtures`). Include screenshots in the PR if the environment allows.

---

## Phase 19 — Portfolio polish

1. Update the README architecture diagram to show the layers: sources → raw → normalized → marts → dashboard, with job cadences.
2. Add `docs/data_dictionary.md`. Generate it from `information_schema` with a small script plus hand-written descriptions, and check in CI that it's current.
3. Add `docs/lineage.md`: for each mart, which tables and sources feed it.
4. Add `docs/adr/` with short decision records:
   - why MySQL and in-process scheduling (fits a home server);
   - why a raw layer with hash deduplication;
   - why price history uses SCD Type 2;
   - why SteamSpy is kept for catalog data but not trusted for time series.
5. Add a README section **"What this project demonstrates"**: a bullet list linking each data-engineering skill to the file that shows it.

**Done when** the docs are complete and the CI docs-freshness check passes.

---

## Part 2 — open human decisions

- Hourly player collection (Phase 12): yes or no.
- Store country/currency (Phase 14): default `us`.
- `RAW_RETENTION_DAYS` (default 30) and `PLAYER_HOURLY_RETENTION_DAYS` (default 90).
- dbt or Postgres migration: deferred, revisit after Phase 16.
- An external orchestrator (Airflow, Dagster, Prefect): deferred. It would be a strong portfolio signal but heavy for a TrueNAS app; revisit after Phase 18.
