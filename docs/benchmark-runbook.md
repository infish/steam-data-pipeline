# Capacity benchmark runbook

This runbook operates only on the isolated `steam-data-pipeline-bench` app and
the marked `steam_pipeline_bench` / `steam_catalog_stage` databases. Never point
these commands at `steam_pipeline`.

## Local validation

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
PYTHONPATH=src:. .venv/bin/python -m unittest discover -s tests -v
PYTHONPATH=src:. .venv/bin/python -m benchmarks.generate \
  --scenario scale-v1 --games 100000 --days 1 --seed 20260922 \
  --start-date 2025-01-01 --batch-size 1000 --resume --dry-run
```

Create a protected local environment file containing three distinct random
passwords, then start the integration stack:

```bash
docker compose --env-file .env.benchmark -f deploy/benchmark-compose.yml up \
  --build --wait mysql
docker compose --env-file .env.benchmark -f deploy/benchmark-compose.yml up \
  --abort-on-container-exit migrate-bench migrate-catalog
```

Run a small integration scenario from the worker container first. The loader
refuses any database other than a marked `steam_pipeline_bench` database.

```bash
docker compose --env-file .env.benchmark -f deploy/benchmark-compose.yml run --rm worker \
  python -m benchmarks.generate --scenario integration-v1 --games 1000 --days 2 \
  --seed 20260922 --start-date 2025-01-01 --batch-size 1000 --resume
```

## TrueNAS preflight and deployment gate

Run outside 19:45–20:30 Europe/Prague. Do not stop production to make the gate
pass.

```bash
ssh -o BatchMode=yes -o ConnectTimeout=10 truenas_admin@192.168.0.3 \
  'free -m; df -h /mnt/Apps; midclt call app.get_instance steam-data-pipeline'
```

Proceed only when available memory is at least 3,072 MiB, projected post-run
pool free space remains at least 20%, and a supported middleware-created 30 GiB
quota/monitor is established for `/mnt/Apps/steam-data-pipeline-bench`. Inspect
the current `app.create` and dataset method schemas before writes. Create the
Custom App through middleware; do not edit `/mnt/.ix-apps`.

Publish only the manual workflow `.github/workflows/publish-benchmark.yml`.
Replace `benchmark-REPLACE_COMMIT` in the deployment input with the immutable
commit tag, then resolve and record the registry digest. The workflow never
moves `latest`.

## Staged synthetic runs

Inside the benchmark worker, use exactly the same scenario so later commands
extend its day prefix:

```bash
PYTHONPATH=src:. python -m benchmarks.generate --scenario scale-v1 --games 100000 --days 1 --seed 20260922 --start-date 2025-01-01 --batch-size 1000 --resume
PYTHONPATH=src:. python -m benchmarks.generate --scenario scale-v1 --games 100000 --days 10 --seed 20260922 --start-date 2025-01-01 --batch-size 1000 --resume
PYTHONPATH=src:. python -m benchmarks.generate --scenario scale-v1 --games 100000 --days 100 --seed 20260922 --start-date 2025-01-01 --batch-size 1000 --resume
```

After the one-million-row stage, record logical data/index bytes, dataset and
binlog bytes, worker/MySQL RSS, elapsed load time and row throughput. Project
the next peak with at least 2x temporary/index/redo headroom. Continue only if
it remains below 24 GiB within the 30 GiB quota and preserves the pool floor.
The 400-day command is conditional on that measured gate.

SIGTERM stops between batches. A hard-killed worker is resumed with the same
command; batch facts and checkpoints commit atomically. A second writer fails
the scenario advisory lock. Verify exact counts and a second no-op invocation
before advancing.

## Query and catalog workloads

```bash
PYTHONPATH=src:. python -m benchmarks.run_queries --scenario scale-v1 \
  --identity 10m-REPLACE_COMMIT --repetitions 30 \
  --output /artifacts/10m/query-results.json
```

For the staging database, set its dedicated credentials in the worker and run:

```bash
PYTHONPATH=src:. python src/catalog_import.py --import-key smoke-20260922 --pages 10 \
  --cache-dir /cache --cache-budget 536870912
```

Every bulk request attempt, including retries, is separated by at least 65
seconds. The result is API-observed bounded coverage, not a complete Steam
census. Do not run concurrently with another known bulk client.

## Pause and resume

Stop only the benchmark worker/app through current TrueNAS middleware methods;
retain `/mnt/Apps/steam-data-pipeline-bench`. Resume by re-running the same
scenario command after the gates pass. Conflicting seed/game/schema settings
require a new scenario and are never resolved by truncation.
