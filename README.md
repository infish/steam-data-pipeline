# Steam Data Pipeline

A learning ETL project that extracts top Steam game data from the SteamSpy API, transforms it with pandas, and loads it into MySQL.

The project is designed to run locally on Windows and in Docker Compose, with a structure that can later be extended toward Azure.

## What It Does

The pipeline performs this flow:

```text
SteamSpy API -> Python extract -> pandas transform -> data quality checks -> MySQL load -> run audit
```

It currently stores:

- transformed game data in `games`
- pipeline execution history in `pipeline_runs`

## Tech Stack

- Python 3.10
- pandas
- requests
- MySQL
- mysql-connector-python
- Docker
- Docker Compose
- Windows batch scripts

## Project Structure

```text
src/
  api/
    steam_api.py
  database/
    mysql_database.py
  quality/
    data_quality.py
  transform/
    data_transformer.py
  config.py
  main.py

init/
  init.sql

Dockerfile
docker-compose.yml
requirements.txt
run_pipeline.bat
setup_venv.bat
```

## Configuration

The app reads MySQL settings from environment variables:

```env
MYSQL_HOST=mysql
MYSQL_USER=root
MYSQL_PASSWORD=rootpassword
MYSQL_DATABASE=steam_pipeline
```

For Docker, create `.env.docker`.

For local Windows runs, create `.env.local`.

Both files are ignored by Git. Use `.env.example` as a template.

## Run With Docker Compose

From the project root:

```powershell
docker compose up --build
```

Expected behavior:

1. MySQL starts.
2. MySQL becomes healthy.
3. The pipeline container runs once.
4. The pipeline exits with code `0` after loading data.
5. MySQL keeps running.

To stop the containers:

```powershell
docker compose down
```

To run only the pipeline again while MySQL is already running:

```powershell
docker compose run --rm pipeline
```

## Verify The Load

Open a MySQL shell:

```powershell
docker compose exec mysql mysql -uroot -prootpassword steam_pipeline
```

Check loaded games:

```sql
SELECT COUNT(*) FROM games;

SELECT appid, name, owners, owners_low, owners_high, estimated_owners, ingestion_time
FROM games
ORDER BY estimated_owners DESC
LIMIT 10;
```

Check pipeline run history:

```sql
SELECT run_id, started_at, finished_at, status, rows_loaded, error_message
FROM pipeline_runs
ORDER BY run_id DESC;
```

A successful run should show:

```text
status       SUCCESS
rows_loaded  100
error        NULL
```

## Run Locally On Windows

Install Python 3.10 or newer, then create a virtual environment:

```bat
setup_venv.bat
```

Run the pipeline:

```bat
run_pipeline.bat
```

The local run expects `.env.local` to point to a reachable MySQL instance.

## Data Quality Checks

Before loading to MySQL, the pipeline validates that:

- the transformed DataFrame is not empty
- required columns exist
- `appid` is present and unique
- `name` is not empty
- owner fields are present
- `owners_low <= owners_high`
- `estimated_owners` is not negative

If validation fails, the pipeline writes a `FAILED` row to `pipeline_runs`.

## Notes For Learning

`games` answers: what data did we load?

`pipeline_runs` answers: what happened when the pipeline ran?

That separation is important in real ETL systems because it gives you observability and makes failures easier to debug.

## Future Improvements

- Add automated tests
- Add data quality result details per rule
- Add GitHub Actions CI
- Add scheduled execution
- Add Azure deployment
- Add Power BI dashboard
