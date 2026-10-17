# Scale tests

`scripts/scale_test.py` pushes a synthetic catalog through every job, with
the schedule simulated hour by hour and every API pause switched off. It
measures the pipeline's own work (parsing, MySQL writes, marts, checks) at a
given size. The time the API pacing would add when live is estimated from
the request counts. The synthetic answers (`src/ingest/synthetic.py`) are
generated from each app ID: the same ID always gets the same answer. They are
shaped like the recorded responses in `tests/fixtures/sources/`.

Run it against an empty, migrated database whose name ends in `_test`:

```bash
PYTHONPATH=src MYSQL_HOST=... MYSQL_DATABASE=steam_pipeline_test MYSQL_USER=... MYSQL_PASSWORD=... python scripts/scale_test.py --pages 5 --hours 24
```

Measured on 2026-10-17 on a laptop, with MySQL 8.0 in Docker.

## Summary

- **At 5,000 games (the default, ADR 6),** the pipeline's own work is about
  4 s per hourly refresh run and 5 s for the catalog run, against about 9
  minutes of API pacing per run when live: **under 1% of a run is the
  pipeline itself**. After one simulated day every game has a review summary
  (5,043 rows), and 720 games have fresh Store details, 1/7 of the catalog.
- **At 20,000 games,** processing stays under 15 s per run. The API is the
  limit: the catalog run would take about 34 minutes live (20 SteamSpy pages
  at one a minute and 400 price batches). Review summaries would reach every
  game only every ~3 days, within the 300-request ceiling per hourly run.
  That is why the default is 5,000.
- No database-side optimisation was needed at these sizes. The largest
  processing step, `review_summaries`, is about 13 ms per summary, mostly
  the raw landing layer's insert and commit, against 2.1 s of pacing per
  Store request.
- Two review-history reconciliation warnings in the 20,000-game run come
  from the synthetic data: its histograms and summaries are independent
  random numbers, so they disagree, and the check reports that as designed.

## 5,000 games (default), 24 simulated hours

| Job | Runs | Processing, total | Processing, slowest run | API pauses if live, per run |
| --- | ---: | ---: | ---: | ---: |
| `catalog_daily` | 1 | 5.1 s | 5.1 s | up to 8.6 min |
| `players_hourly` | 24 | 13.0 s | 1.7 s | up to 0.0 min |
| `refresh_hourly` | 24 | 103.3 s | 5.2 s | up to 8.9 min |
| `reviews_daily` | 1 | 0.4 s | 0.4 s | up to 0.2 min |

### Slowest steps

| Step | Inside | Runs | Total | Slowest |
| --- | --- | ---: | ---: | ---: |
| `review_summaries` | `job` | 24 | 64.8 s | 2.9 s |
| `store_details_and_tags` | `job` | 24 | 25.7 s | 1.2 s |
| `06_mart_tag_summary` | `marts` | 50 | 7.8 s | 1.2 s |
| `04_mart_price_status` | `marts` | 50 | 4.7 s | 0.4 s |
| `prices` | `job` | 1 | 2.4 s | 2.4 s |
| `03_mart_review_trends` | `marts` | 50 | 2.0 s | 0.1 s |
| `05_mart_genre_summary` | `marts` | 50 | 1.7 s | 0.3 s |
| `07_mart_release_year_summary` | `marts` | 50 | 1.0 s | 0.1 s |
| `01_mart_player_trends` | `marts` | 50 | 0.9 s | 0.0 s |
| `02_mart_player_peak_hours` | `marts` | 50 | 0.8 s | 0.0 s |
| `08_mart_engagement` | `marts` | 50 | 0.7 s | 0.0 s |
| `load_snapshots` | `job` | 1 | 0.6 s | 0.6 s |

### Rows after the simulation

| Table | Rows |
| --- | ---: |
| `games` | 5,030 |
| `game_details` | 720 |
| `game_tags` | 14,400 |
| `game_price_history` | 5,000 |
| `review_observations` | 5,043 |
| `player_observations` | 2,400 |
| `raw_api_responses` | 6,642 |
| `data_quality_results` | 3,467 |
| `run_steps` | 1,229 |

## 20,000 games, 6 simulated hours

| Job | Runs | Processing, total | Processing, slowest run | API pauses if live, per run |
| --- | ---: | ---: | ---: | ---: |
| `catalog_daily` | 1 | 13.2 s | 13.2 s | up to 34.3 min |
| `players_hourly` | 6 | 3.4 s | 0.6 s | up to 0.0 min |
| `refresh_hourly` | 6 | 34.0 s | 5.8 s | up to 11.2 min |
| `reviews_daily` | 1 | 0.7 s | 0.7 s | up to 0.2 min |

### Slowest steps

| Step | Inside | Runs | Total | Slowest |
| --- | --- | ---: | ---: | ---: |
| `review_summaries` | `job` | 6 | 22.0 s | 3.8 s |
| `prices` | `job` | 1 | 8.3 s | 8.3 s |
| `store_details_and_tags` | `job` | 6 | 8.2 s | 1.5 s |
| `04_mart_price_status` | `marts` | 14 | 4.7 s | 0.6 s |
| `load_company_links` | `job` | 1 | 1.6 s | 1.6 s |
| `fetch_catalog` | `job` | 1 | 0.9 s | 0.9 s |
| `load_snapshots` | `job` | 1 | 0.8 s | 0.8 s |
| `load_games` | `job` | 1 | 0.6 s | 0.6 s |
| `06_mart_tag_summary` | `marts` | 14 | 0.6 s | 0.1 s |
| `03_mart_review_trends` | `marts` | 14 | 0.3 s | 0.0 s |
| `05_mart_genre_summary` | `marts` | 14 | 0.2 s | 0.0 s |
| `01_mart_player_trends` | `marts` | 14 | 0.2 s | 0.0 s |

### Rows after the simulation

| Table | Rows |
| --- | ---: |
| `games` | 20,030 |
| `game_details` | 222 |
| `game_tags` | 4,440 |
| `game_price_history` | 20,000 |
| `review_observations` | 1,581 |
| `player_observations` | 600 |
| `raw_api_responses` | 2,463 |
| `data_quality_results` | 965 |
| `run_steps` | 347 |
