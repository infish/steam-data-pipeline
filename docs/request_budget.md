# Request budget

How many requests each job makes, against each source's limit. The limits
and their basis are in [`sources.md`](sources.md#rate-limits). "Tracked" is
the number of app IDs in `STEAM_TRACKED_APPIDS` (default 3).

## Per job

| Job | Runs per day | Source | Requests per run | Requests per day |
| --- | --- | --- | --- | --- |
| `players_hourly` | 24 | Valve top concurrent players | 1 | 24 |
| | | Valve current players | tracked apps outside the top 100 (≤ 3) | ≤ 72 |
| | | Valve most played (daily peak) | 1 | 24 |
| `catalog_daily` | 1 | SteamSpy `request=all` | 1 | 1 |
| | | SteamSpy `request=appdetails` | tracked apps off the catalog page (≤ 3), plus tags for the refreshed games whose tags are missing or older than `TAG_REFRESH_DAYS` (30): about 120 × 9 / 30 ≈ 36 a day once every game has tags | ≤ 123, typically ≈ 40 |
| | | Steam Store `api/appdetails` | `STORE_DETAILS_PER_RUN` (120) | 120 |
| | | Steam Store price batch | catalog games ÷ 50 (≈ 20) | ≈ 20 |
| `reviews_daily` | 1 | Steam review summary | tracked apps (3) | 3 |
| | | Steam review histogram | tracked apps (3) | 3 |
| | | Steam news | tracked apps (3) | 3 |
| `backfill` (manual) | on demand | review histogram or news | the given apps | — |

## Per source

| Source | Requests per day | Peak rate | Limit used for planning | Headroom |
| --- | --- | --- | --- | --- |
| Valve Web API (top concurrent, current players, most played, news) | ≤ 123 | ≤ 5 per hourly run | keep modest; no published keyless limit | n/a |
| SteamSpy `request=all` | 1 | 1 per day | 1 per 60 s | ample |
| SteamSpy other requests | ≤ 123 | at most 1 per 1.1 s (paced) | 1 per s | the interval is 10% above the limit |
| Steam Store (`api/appdetails`, price batches, `appreviews`, `appreviewhistogram`) | ≤ 146 | at most 1 per 2.1 s for details, prices and histograms (paced) | 150 per 5 min | the whole day's Store requests fit inside one 5-minute budget |

## How the limits are enforced

Each rate-limited API has a pacer per run (`src/api/pacing.py`,
`RunContext.pace`): before a request it waits until the API's interval has
passed since the **start** of the previous request to the same API, and no
longer. Time spent on the previous response, on parsing and loading, or on
requests to other APIs counts towards the interval. Request starts are
therefore never closer than 2.1 s for the Store or 1.1 s for SteamSpy, the
same guarantee as the fixed pause before every request used before v1.1.0,
without its extra waiting ([`optimizations.md`](optimizations.md)). The
SteamSpy page request counts towards SteamSpy's interval, and one Store
pacer covers details, price batches and histograms in a run. Review
summaries (`appreviews`, 3 per day) are not paced.

## Notes

- The catalog job alternates one Store request with one SteamSpy request
  per refreshed game. The Store interval sets the pace: 120 games take about
  4.2 minutes (2.1 s each) and never exceed either limit. At 120 games per
  run, a 1,000-game catalog is fully refreshed about every 9 days.
  `STORE_DETAILS_PER_RUN` is capped at 140 so a run stays inside the Store
  budget.
- The most-played leaderboard changes once a day but is fetched every hour,
  because a missed day cannot be fetched later. Repeated identical responses
  cost only a metadata row in the raw landing layer.
- Jobs run one at a time in the scheduler, so their budgets do not overlap.
  A manual `docker compose run pipeline` can overlap the scheduler; the
  per-job locks keep the same job from running twice, but two different jobs
  may run at once. Their combined Store requests stay far below the budget.
