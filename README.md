# Steam Data Pipeline

A Dockerized data pipeline that extracts a SteamSpy game catalog plus focused measurements from Valve's Steam endpoints, validates and transforms them with Python, stores historical observations in MySQL, and presents SQL-driven analytics in a Streamlit dashboard.

The project is designed as a practical data-engineering portfolio project, with reproducible deployment, database migrations, audit history, data-quality checks, automated tests, and separate database permissions.

## Architecture

```mermaid
flowchart TD
    API["SteamSpy catalog"] --> Pipeline["Python pipeline"]
    Steam["Valve Steam measurements"] --> Pipeline
    Flyway["Flyway SQL migrations"] --> MySQL[("MySQL")]
    Pipeline --> MySQL
    MySQL --> Views["SQL analytics views"]
    Views --> Dashboard["Read-only Streamlit dashboard"]
```

Docker Compose manages these services:

- `mysql` — persistent database.
- `db-bootstrap` — creates or repairs the read-only dashboard account and the
  pipeline's writer account.
- `migration-assets` — copies migrations from the application image.
- `migrate` — applies pending Flyway migrations and exits.
- `scheduler` — runs the pipeline daily.
- `pipeline` — optional one-shot manual collection.
- `dashboard` — provides the interactive analytics interface.

Startup order is enforced:

```text
MySQL healthy → bootstrap and migrations complete → scheduler and dashboard start
```

## Features

- SteamSpy API extraction with retry and exponential backoff.
- Validation and transformation of 1,000 games per configured API page, plus
  any tracked games that are not on that page.
- Append-only historical metric snapshots.
- Valve current-player measurements for top-100 games present on the configured
  catalog page, plus Steam review measurements for configured app IDs.
- Audited pipeline runs with success/failure status.
- SQL window functions for changes between snapshots.
- Versioned and repeatable Flyway migrations.
- Separate database accounts: a schema owner used only by Flyway, a writer
  for the pipeline, and a read-only dashboard account.
- Docker-managed persistent storage.
- Non-root Python containers.
- Automated unit tests without live API requests.
- Integration tests against a real, migrated MySQL database in CI.
- Linux, macOS and Windows-compatible Docker Compose workflow.

## Technology

- Python 3.12
- Pandas
- Requests
- MySQL 8.0
- Flyway
- Streamlit
- Plotly
- Docker Compose
- `unittest`

## Data model

### `games`

Relatively stable game metadata:

- App ID and name
- Developer and publisher
- Languages and genre (empty for games loaded from the SteamSpy catalog page,
  which does not include these fields; see `docs/sources.md`)
- First and most recent appearance in the extraction

### `game_metric_snapshots`

Append-only SteamSpy values for each pipeline run. `snapshot_time` is the
collection time; SteamSpy does not expose the underlying source measurement
time:

- Owner estimate range and midpoint
- Positive and negative reviews
- Review score
- Lifetime and recent playtime
- SteamSpy-reported `ccu` (the source measurement time is unknown)
- Current and original price
- Discount percentage

### `steam_game_measurements`

Focused observations from Valve endpoints:

- Current players and rank for Valve's top 100 concurrent games that are also
  present on the configured SteamSpy catalog page
- Direct current-player measurements for configured tracked games outside the
  top 100
- Steam Store review totals for tracked games, across all languages and
  purchase types and including off-topic activity
- Explicit collection time
- Valve's source measurement time for the top-100 feed; direct calls retain a
  null source time and use collection time as the chart fallback

The Valve calls are intentionally limited to `STEAM_TRACKED_APPIDS`. The
default is Counter-Strike (`730`), Factorio (`427520`), and Portal 2 (`620`),
which adds review requests only for those games. The top-100 player coverage is
one bulk request per daily run.

A tracked game that is not on the configured SteamSpy page is fetched with
SteamSpy `request=appdetails`, one request per game with a pause of just over
one second before each (SteamSpy's limit is one such request per second). The
result is shaped like a page row, so the game gets a `games` row and a snapshot,
and its Valve measurement can be stored. If that lookup fails, the run records
a warning and skips the game. Because of this, `rows_extracted` can exceed
1,000.

Top-100 games that are neither on the page nor tracked are not fetched; their
Valve measurements are skipped and the run logs only their count.

Every source, including candidates for future phases, is documented in
[`docs/sources.md`](docs/sources.md) with its fields, timestamp meaning,
freshness, rate limit and caveats. Recorded example responses are in
`tests/fixtures/sources/`.

The endpoints currently called are:

- Catalog and owner estimates:
  `https://steamspy.com/api.php?request=all&page=<page>`, plus
  `https://steamspy.com/api.php?request=appdetails&appid=<appid>` for tracked
  games missing from that page
- Current players:
  `ISteamChartsService/GetGamesByConcurrentPlayers/v1/` for the top 100, then
  `ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid=<appid>`
  for configured games outside the top 100
- Review summary:
  `https://store.steampowered.com/appreviews/<appid>` with `filter=recent`,
  `language=all`, `review_type=all`, `purchase_type=all`,
  `filter_offtopic_activity=0`, and one returned review

SteamSpy is the core source: if the catalog request fails, the run fails.
Valve data is enrichment, so its failures degrade the run instead of aborting
it. The run still ends as `SUCCESS`, each problem is logged, and the joined
messages are stored in `pipeline_runs.warning_message` (up to 2,000
characters). The dashboard shows a warning when the latest run has one.

- Top-100 chart fails: every tracked app falls back to a direct player
  lookup, so those rows have no rank and no source timestamp.
- Player lookup fails for a tracked app: that app has no measurement row for
  the run.
- Review lookup fails or is inconsistent: the row is kept with empty review
  fields.

### `pipeline_runs`

Operational audit information:

- Start and finish timestamps
- Status
- Rows extracted from SteamSpy (`rows_extracted`) and loaded (`rows_loaded`)
- Valve measurements loaded (`measurements_loaded`; historical runs were
  backfilled from `steam_game_measurements`)
- Rows rejected by validation (`rows_rejected`); before this column existed,
  any invalid row failed the whole run, so historical runs show 0
- Failure message, and a separate `warning_message` for runs that succeeded
  with degraded input

SteamSpy rows are validated before loading. Problems with the whole response
fail the run: an empty response, missing columns, or duplicate app IDs.
Problems with a single row reject only that row: a missing or empty name, a
missing or malformed `owners` range, negative counts, a review score outside
0-100, or review totals that do not add up. Rejected rows are not loaded. The
run logs the first 20 rejected app IDs with their reasons, stores the count in
`rows_rejected`, and adds a short summary to `warning_message`. If more than
`PIPELINE_MAX_REJECTED_PERCENT` of rows are rejected, the run fails instead,
because that usually means the source format changed.

Only one pipeline run can be active at a time. Each run takes the MySQL named
lock `steam_pipeline_run` before it creates a run row; if another run holds the
lock, the new run logs a warning and exits without creating a row. While
holding the lock, a run marks any leftover `RUNNING` rows as `FAILED` with
`error_message = 'Abandoned: process ended before completion'`. Their
`finished_at` stays empty because the process never finished. This covers runs
interrupted by a crash or container restart.

If recording a failure itself fails (for example, the database connection is
lost), the pipeline logs that secondary error and re-raises the original one.

All database timestamps are stored in UTC. The dashboard converts displayed run times to `Europe/Prague`.

### Dashboard behaviour

- Query results are cached for 5 minutes, so a finished run can take up to
  5 minutes to appear. The SQL Explorer's "Run example" button always queries
  the database directly.
- The Valve measurement chart has one point per successful run, starting with
  the first run that stored any Valve measurement. A run that did not measure
  the selected game leaves a gap in the line instead of joining its neighbours.
- Each point is placed at Valve's source time when available, otherwise at the
  collection time, otherwise at the run's finish time.
- Games are selected by app ID and shown as `name (appid)`, so two games with
  the same name stay distinct.

## SQL analytics

The project includes views for:

- Latest successful pipeline run
- Current metrics from that run
- Top games by estimated owners
- Best-reviewed games
- Games with the highest SteamSpy-reported CCU
- Free, paid and unknown-price comparisons
- Publisher summaries
- Historical game trends
- Changes between consecutive snapshots using `LAG()`
- Daily pipeline reliability

Views contain reusable analytical logic. Final dashboard queries control sorting and row limits explicitly.

## Project structure

```text
.
├── .github
│   └── workflows
│       ├── publish-image.yml
│       └── tests.yml
├── db
│   └── migrations
│       ├── R__analytics_views.sql
│       ├── V1__initial_schema.sql
│       ├── V2__steam_measurements.sql
│       ├── V3__expand_valve_player_measurements.sql
│       └── V4__run_audit_columns.sql
├── deploy
│   └── truenas-compose.yml
├── docs
│   └── sources.md
├── src
│   ├── api
│   │   └── steam_api.py
│   ├── database
│   │   └── mysql_database.py
│   ├── quality
│   │   └── data_quality.py
│   ├── transform
│   │   └── data_transformer.py
│   ├── config.py
│   ├── dashboard.py
│   ├── dashboard_data.py
│   ├── main.py
│   └── scheduler.py
├── tests
│   ├── fixtures
│   │   └── sources        # recorded API responses, see docs/sources.md
│   ├── integration
│   │   ├── base.py
│   │   ├── test_run_lifecycle.py
│   │   └── test_run_pipeline.py
│   ├── test_dashboard_data.py
│   ├── test_data_quality.py
│   ├── test_main.py
│   ├── test_scheduler.py
│   ├── test_steam_api.py
│   └── test_transform.py
├── .env.example
├── CHANGELOG.md
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
```

## Prerequisites

Install:

- Git
- Docker
- Docker Compose

No local Python installation, MySQL server, or MySQL Workbench is required.

Verify:

```bash
git --version
docker --version
docker compose version
```

## Linux setup

Clone the repository:

```bash
git clone https://github.com/infish/steam-data-pipeline.git
cd steam-data-pipeline
```

Create the private configuration file:

```bash
cp .env.example .env
```

Generate four separate passwords:

```bash
openssl rand -hex 24
openssl rand -hex 24
openssl rand -hex 24
openssl rand -hex 24
```

Open `.env` and assign them to:

- `MYSQL_ROOT_PASSWORD`
- `PIPELINE_DB_PASSWORD` — schema owner, used only by Flyway migrations
- `WRITER_DB_PASSWORD` — used by the scheduler and manual pipeline runs
- `DASHBOARD_DB_PASSWORD` — read-only dashboard account

The bootstrap service writes the database name, the dashboard and writer user
names, and their passwords into SQL. It refuses to run unless each value
contains only letters, digits, `_` and `-`. Hex passwords from
`openssl rand -hex` always qualify.

Never commit `.env`. It is excluded by both Git and Docker.

Collection settings use these defaults:

| Variable | Default | Meaning |
| --- | --- | --- |
| `STEAMSPY_MODE` | `all` | SteamSpy catalog request |
| `STEAMSPY_PAGE` | `0` | SteamSpy catalog page |
| `STEAM_TRACKED_APPIDS` | `730,427520,620` | App IDs enriched with reviews and direct player fallback |
| `PIPELINE_MAX_REJECTED_PERCENT` | `5` | Fail the run if more than this % of SteamSpy rows fail validation |
| `PIPELINE_TIMEZONE` | `Europe/Prague` | Daily scheduler timezone |
| `PIPELINE_RUN_HOUR` | `20` | Daily local run hour |
| `PIPELINE_RUN_MINUTE` | `0` | Daily local run minute |
| `PIPELINE_RUN_ON_STARTUP` | `false` | Optional immediate collection on scheduler startup |

Keep `PIPELINE_RUN_ON_STARTUP=false` for persistent deployments. Container
restarts otherwise create extra observations unrelated to the daily schedule.

## Start the complete project

```bash
docker compose up --build -d
```

Check service states:

```bash
docker compose ps -a
```

Expected lifecycle:

- MySQL: running and healthy.
- Flyway: exited with code `0`.
- Scheduler: running and waiting for the configured daily time.
- Pipeline: absent unless a manual run was requested.
- Dashboard: running and healthy.

Open the dashboard:

[http://localhost:8501](http://localhost:8501)

The Compose port mapping publishes the dashboard on port `8501` on all host
interfaces. Restrict it with a firewall or change the mapping to
`127.0.0.1:8501:8501` if it should only be reachable locally.

## Run another snapshot

```bash
docker compose run --rm pipeline
```

This applies pending migrations, refreshes the configured SteamSpy catalog
page, and appends Valve measurements for the tracked app IDs.

The scheduler runs once daily. It does not collect on container startup, which
avoids duplicate observations during app restarts. Manual runs remain available
for focused validation.

## View logs

```bash
docker compose logs pipeline
docker compose logs dashboard
docker compose logs mysql
```

Follow dashboard logs continuously:

```bash
docker compose logs -f dashboard
```

## Stop the project

```bash
docker compose down
```

The MySQL named volume remains intact.

To intentionally delete the development database and recreate it from scratch:

```bash
docker compose down -v
```

Warning: `-v` permanently deletes all snapshots stored in the Docker volume.

## Query MySQL without Workbench

Open the MySQL command-line client as the pipeline account:

```bash
docker compose exec mysql sh -c \
  'MYSQL_PWD="$MYSQL_PASSWORD" mysql -u"$MYSQL_USER" "$MYSQL_DATABASE"'
```

Example SQL:

```sql
SELECT COUNT(*)
FROM current_game_metrics;

SELECT
    games.name,
    measurements.collected_at,
    measurements.source_measured_at,
    measurements.current_players,
    measurements.total_reviews
FROM steam_game_measurements AS measurements
JOIN games ON games.appid = measurements.appid
ORDER BY measurements.collected_at DESC
LIMIT 20;
```

Exit the MySQL client with:

```text
exit
```

## Database migrations

Flyway reads SQL from:

```text
db/migrations/
```

Migration types:

- `V1__description.sql` — versioned migration, executed once.
- `V2__description.sql` — next schema change.
- `R__description.sql` — repeatable migration, rerun when its contents change.

Do not edit an already-applied versioned migration. Create the next numbered migration instead.

## Run automated tests

Tests run inside the application image and do not require local Python packages:

```bash
docker compose run --build --rm --no-deps \
  -e PYTHONPATH=/app/src \
  pipeline \
  python -m unittest discover -s tests -v
```

The API tests use mocks and never contact SteamSpy or Valve endpoints.

### Integration tests

Integration tests in `tests/integration/` run the pipeline against a real,
migrated MySQL database with canned API payloads. They are skipped unless
`INTEGRATION_DB=1` is set, and they refuse to run unless `MYSQL_DATABASE` ends
in `_test`, because every test deletes all pipeline rows.

Run them in a throwaway Compose project with its own database volume. The
tests clear tables between runs, which the pipeline's writer account cannot
do, so they connect as the schema owner. Load `.env` into the shell first;
`-e MYSQL_USER -e MYSQL_PASSWORD` then passes those values through without
putting the password on the command line:

```bash
set -a; . ./.env; set +a
MYSQL_USER="$PIPELINE_DB_USER" MYSQL_PASSWORD="$PIPELINE_DB_PASSWORD" \
MYSQL_DATABASE=steam_pipeline_test docker compose -p steam-test \
  --profile manual run --build --rm \
  -e INTEGRATION_DB=1 -e PYTHONPATH=/app/src \
  -e MYSQL_USER -e MYSQL_PASSWORD \
  pipeline \
  python -m unittest discover -s tests/integration -t tests -v
```

Remove the throwaway project afterwards:

```bash
docker compose -p steam-test --profile manual down -v
```

Never run integration tests against the `steam_pipeline` database used by a
real deployment. CI runs the same tests in the `integration-tests` job against
a MySQL service container migrated with Flyway.

## Security

- MySQL is not published to the host network.
- Database files live in a Docker named volume.
- Credentials are stored in an ignored `.env` file.
- The dashboard uses an account restricted to `SELECT` and `SHOW VIEW`.
- The scheduler and manual pipeline use a writer account restricted to
  `SELECT`, `INSERT` and `UPDATE`; it cannot drop, alter or delete. The schema
  owner is used only by Flyway.
- `db-bootstrap` re-applies the dashboard and writer passwords on every start,
  so rotating a password in `.env` takes effect on the next
  `docker compose up -d`.
- The MySQL healthcheck and bootstrap pass the root password through the
  `MYSQL_PWD` environment variable rather than on a command line.
- Arbitrary SQL input is not accepted by the dashboard.
- Streamlit accepts connections on the container network; the host-side port
  mapping determines where the dashboard is exposed.
- Python services run as an unprivileged container user.

Do not expose the Streamlit port directly to the public internet. Use an authenticated reverse proxy or a private network such as Tailscale if remote access is required.

## Stale-data incident: September 2026

The original pipeline treated every successful SteamSpy response as a fresh
measurement. That was incorrect. Both SteamSpy's bulk and per-app endpoints
returned unchanged values across repeated collections, while the database gave
each response a new `snapshot_time`. The timestamp represented collection time,
not the time SteamSpy measured the underlying value. As a result, the dashboard
plotted flat series that looked current even though the source values were
stale. For example, 7 Days to Die repeatedly displayed SteamSpy `ccu` of
`17,045` while Valve reported roughly `29,000` current players during diagnosis.

The first correction added authoritative Valve measurements for three test
games, but the dashboard still exposed the legacy SteamSpy chart for every
catalog game. Games outside that initial set therefore continued to show the
same misleading flat series. The completed correction:

- collects Valve's top-100 concurrent-player feed once per run;
- uses direct Valve player lookups for configured games outside that feed;
- records Valve's source timestamp when the feed supplies one;
- keeps collection time separate and uses it only as an explicit fallback;
- charts only Valve measurements while retaining SteamSpy snapshots for audit;
- excludes stale SteamSpy player, review, and price fields from regular
  dashboard analyses and the in-dashboard SQL Explorer;
- avoids fabricating or backfilling historical Valve values.

Duplicate observations had two scheduling causes. The scheduler originally ran
on container startup, and its single long sleep could wake fractionally before
the intended deadline and run twice. A separate user-level systemd timer was
also invoking the manual pipeline alongside the Compose scheduler. Startup
collection is now disabled, the scheduler rechecks its deadline after waking,
and the duplicate local systemd timer was disabled. Persistent deployments
should run exactly one scheduling mechanism.

## Data limitations

SteamSpy remains useful for catalog metadata and broad comparisons, but its
values are not treated as authoritative time-series measurements.

Important limitations:

- `owners` is an estimated range.
- The midpoint is useful for comparisons but is not an exact count.
- Owned copies are not equivalent to sales.
- SteamSpy does not provide source measurement timestamps. `snapshot_time` is
  only when this pipeline collected the response, and a successful response
  does not establish freshness.
- Valve's current-player endpoint may be cached upstream for a short interval.
- Valve supplies `source_measured_at` for its top-100 player feed. Direct player
  and review-summary calls do not include a source timestamp, so charts fall
  back to the explicitly labeled `collected_at` value for those observations,
  and to the run's finish time for runs that did not measure the game.
- Steam review totals use `language=all`, `purchase_type=all`, and
  `filter_offtopic_activity=0`. Moderation or classification changes can make
  totals decrease as well as increase.
- Recently released and low-ownership games may have unreliable estimates.
- `request=all` returns 1,000 games per page and is rate-limited.

The dashboard warns when the latest three values of a selected Valve metric are
identical. Legacy SteamSpy snapshots remain queryable for audit purposes but
are no longer charted because their collection timestamps do not establish
freshness.

Run exactly one scheduler. The Compose `scheduler` is the supported default;
disable any older cron or systemd timer that also invokes the manual `pipeline`
service.

These limitations should be considered when interpreting charts and derived metrics.

## Releases and image tags

The `Publish Docker image` workflow builds the application image and pushes it
to GHCR. It first runs the full test workflow (unit and integration tests); if
any test fails, no image is published.

Each published image gets these tags:

| Tag | When |
| --- | --- |
| full commit SHA | every build |
| `latest` | builds from `main` only |
| `vX.Y.Z` | builds from a pushed `vX.Y.Z` git tag |

A release tag never moves `latest`. `deploy/truenas-compose.yml` references a
`vX.Y.Z` tag, never `latest`.

To cut a release:

1. On a branch, set the new version in all four `image:` lines of
   `deploy/truenas-compose.yml`, and add the version to the top `CHANGELOG.md`
   heading. Merge the branch into `main`.
2. Tag the merge commit on `main` and push the tag:

   ```bash
   git switch main && git pull
   git tag -a v1.0.1 -m "v1.0.1"
   git push origin v1.0.1
   ```

3. Wait for the `Publish Docker image` run for that tag to finish
   successfully, then confirm the image exists:

   ```bash
   docker pull ghcr.io/infish/steam-data-pipeline:v1.0.1
   ```

4. In TrueNAS, edit the app's YAML to use the new tag and save.

To roll back, change the tag back to the previous release. This is safe only
if the newer release added no database migrations, because Flyway migrations
are not reversed.

## TrueNAS SCALE deployment

The project can be deployed on TrueNAS SCALE as a single Custom App using the
**Install via YAML** interface.

### Architecture

The TrueNAS deployment runs:

- MySQL 8 with persistent storage
- a one-shot database bootstrap service
- Flyway migrations
- a persistent Python scheduler
- the Streamlit dashboard
- an optional manual one-shot pipeline service

The application image is pinned to a release tag:

```text
ghcr.io/infish/steam-data-pipeline:v1.0.0
```

Deploy only a tag whose image has been published (see
[Releases and image tags](#releases-and-image-tags)). Pinning means updating
the app never picks up unreleased changes from `main`.

Before installing `deploy/truenas-compose.yml`:

1. Replace all four `CHANGE_ME_*` password placeholders with distinct strong
   values, for example from `openssl rand -hex 24`. Passwords may contain only
   letters, digits, `_` and `-`.
2. Confirm `/mnt/Apps/steam-data-pipeline/mysql` is the intended persistent
   dataset path.
3. Keep the tracked app list small and leave startup collection disabled.

Paste the resulting YAML into the Custom App editor and deploy it. Flyway
applies additive migrations before the scheduler and dashboard start. Updating
the app recreates containers but preserves the MySQL host-path data.

### Upgrading an existing TrueNAS deployment

Deployments created before the writer account existed connect the scheduler as
the schema owner. To upgrade:

1. **Before** updating the app, prepare the new YAML: start from the current
   `deploy/truenas-compose.yml`, fill in your existing root, schema owner and
   dashboard passwords, and replace `CHANGE_ME_WRITER` with a new value such as
   `openssl rand -hex 24`. The writer user and password appear in
   `db-bootstrap`, `scheduler` and `pipeline` and must match in all three.
2. Check that the dashboard password contains only letters, digits, `_` and
   `-`. If not, replace it in both `db-bootstrap` and `dashboard`; bootstrap
   applies the new value.
3. Make sure the image tag in the YAML has been published (see
   [Releases and image tags](#releases-and-image-tags)).
4. Save the YAML. TrueNAS recreates the containers.

On start, `db-bootstrap`:

- refuses to run, with a message naming the variable, if a user name or
  password is missing or contains other characters, or if the writer user is
  the same as the schema owner or dashboard user;
- creates the writer account if it does not exist;
- sets the dashboard and writer passwords to the values in the YAML;
- grants the dashboard `SELECT, SHOW VIEW` and the writer
  `SELECT, INSERT, UPDATE` on the database.

It does not change the root or schema owner accounts and does not touch data.
The `mysql` container restarts once because its environment no longer carries
dashboard credentials.

If bootstrap fails, the scheduler does not start and no collection happens; the
dashboard and stored data are unaffected. Fix the YAML and save again. To roll
back, restore the previous YAML; the unused writer account is harmless.

A local Docker Compose deployment upgrades the same way: add `WRITER_DB_USER`
and `WRITER_DB_PASSWORD` to `.env` (see `.env.example`), then run
`docker compose up -d`.
