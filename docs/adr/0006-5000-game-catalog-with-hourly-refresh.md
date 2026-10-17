# 6. A 5,000-game catalog refreshed in hourly slices

Status: accepted (2026-10-17), implemented in v1.2.0.

## Context

The project's goal is as much data as the free APIs allow, collected in a
way that visibly holds up at a larger size. After ADR 5 the catalog was
about 1,000 SteamSpy games plus Valve's chart games, with Store details and
review summaries for 120 games a day each.

The limiting resource is the Steam Store's request rate (about 150 requests
per 5 minutes per IP address; `docs/sources.md`). The pipeline paces Store
requests at 2.1 s, which is about 41,000 requests a day, of which it used
about 270. The daily volume was set by our own settings, not by the API.

The scheduler runs jobs one at a time. One long nightly Store run would
therefore delay `players_hourly`, and leave gaps in the hourly player series.

## Decision

- **5,000 games:** SteamSpy pages 0–4 (`STEAMSPY_PAGES=5`, its ranking by
  owners, frozen but still the best free list of which games matter), plus
  Valve's chart games and tracked apps. SteamSpy allows one page a minute
  and the page is fetched weekly, so this costs about 5 minutes a week. A
  failed page is a warning, not a failed run.
- **A new hourly job, `refresh_hourly`** (minute 37, half an hour after the
  players job), fetches a slice of the catalog every hour:
  - Store details and tags, so every game is refreshed every
    `DETAILS_REFRESH_DAYS` (7): about 31 a run at 5,000 games;
  - Steam review summaries, so every game is refreshed every
    `REVIEWS_REFRESH_DAYS` (1): about 211 a run. This gives a **daily review
    time series for every game in the catalog**.

  The slice sizes follow from the catalog size, within
  `REFRESH_MAX_REQUESTS` (300, about 10.5 minutes) per run. Games are taken
  oldest first, so a missed or failed run is caught up by the next ones
  without any bookkeeping.
- **`catalog_daily`** keeps the game list and the prices for every game
  (100 batches of 50, about 3.5 minutes). **`reviews_daily`** keeps the
  tracked games' summaries, histories and news.
- Store details and review checks belong to `refresh_hourly`, and their
  freshness interval is one hour.
- `STORE_DETAILS_PER_RUN` and `REVIEW_SUMMARIES_PER_RUN` stay as optional
  fixed overrides, empty by default.

## Request budget at 5,000 games

| Job | Store requests per day | Time per run, live |
| --- | ---: | --- |
| `refresh_hourly` (24 runs) | about 5,800 | about 8.5 min |
| `catalog_daily` (prices) | about 100 | about 3.5 min (plus about 5 min of SteamSpy pages weekly) |
| `reviews_daily` (tracked) | 6 | under 1 min |
| **Total** | **about 5,900** (14% of the paced maximum) | |

Every run stays under 143 Store requests in any 5 minutes, because of the
pacer.

## Evidence

`scripts/scale_test.py` runs every job against synthetic responses
(`src/ingest/synthetic.py`) with the API pauses switched off, so it measures
only the pipeline's own work; results are in `docs/scale.md`. At 5,000 games
over a simulated day, processing was about 4 s per refresh run and 5 s for
the catalog, against about 9 minutes of API pacing: **under 1% of a run is
the pipeline itself**. At 20,000 games processing stayed under 15 s per run;
there the API time (34 minutes for the catalog, review summaries every 3
days) is the limit, which is why the default is 5,000.

## Consequences

- About 5,000 review summaries and 700 Store details a day; the catalog
  marts and the dashboard cover 5,000 games.
- Raw storage grows with the request count; identical responses are stored
  once (ADR 2), and payloads are purged after `RAW_RETENTION_DAYS`.
- Player counts stay hourly for Valve's top 100 and the tracked games. Daily
  counts for every game would mix a single sample a day with the hourly
  peaks, and need their own design.
- A SteamSpy outage no longer fails the catalog run. This replaces ADR 4's
  "a SteamSpy outage still fails the catalog job".
