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
| `catalog_daily` | 1 | SteamSpy `request=all` | `STEAMSPY_PAGES` (5) every `STEAMSPY_PAGE_INTERVAL_DAYS` (7) | ≈ 0.7 |
| | | SteamSpy `request=appdetails` | tracked apps off the catalog pages (≤ 3), on page days | ≤ 3 |
| | | Steam Store price batch | catalog games ÷ 200 (≈ 26 at 5,000 games) | ≈ 26 |
| `refresh_hourly` | 24 | Steam Store `api/appdetails` | catalog ÷ (`DETAILS_REFRESH_DAYS` × 24): ≈ 31 at 5,000 games | ≈ 740 |
| | | SteamSpy `request=appdetails` (tags) | the refreshed games whose tags are missing or older than `TAG_REFRESH_DAYS` (30) | ≈ 170 once every game has tags |
| | | Steam review summary | catalog ÷ (`REVIEWS_REFRESH_DAYS` × 24): ≈ 211 at 5,000 games | ≈ 5,040 |
| `reviews_daily` | 1 | Steam review summary | tracked apps (3) | 3 |
| | | Steam review histogram | tracked apps (3) | 3 |
| | | Steam news | tracked apps (3) | 3 |
| `backfill` (manual) | on demand | review histogram or news | the given apps | — |

## Per source

| Source | Requests per day | Peak rate | Limit used for planning | Headroom |
| --- | --- | --- | --- | --- |
| Valve Web API (top concurrent, current players, most played, news) | ≤ 123 | ≤ 5 per hourly run | keep modest; no published keyless limit | n/a |
| SteamSpy `request=all` | ≈ 0.14 | 1 per week | 1 per 60 s | ample |
| SteamSpy other requests | ≤ 123 | at most 1 per 1.1 s (paced) | 1 per s | the interval is 10% above the limit |
| Steam Store (`api/appdetails`, price batches, `appreviews`, `appreviewhistogram`) | ≈ 5,830 at 5,000 games | at most 1 per 2.1 s (paced, all four) | 150 per 5 min | every run stays under 143 per 5 minutes by its pacing, and the scheduler runs one job at a time |

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
summaries (`appreviews`) are paced with the same Store pacer since v1.2.0,
when they grew from 3 to 123 a day.

## Notes

- `refresh_hourly` alternates one Store details request with one SteamSpy
  tag request per refreshed game, then fetches its review summaries. The
  Store interval sets the pace: about 242 Store requests take about 8.5
  minutes and never exceed either limit. `REFRESH_MAX_REQUESTS` (300, about
  10.5 minutes) caps a run; both slices shrink in proportion above it
  (ADR 6, docs/scale.md).
- At 5,000 games the Store total is about 5,830 requests a day: 14% of the
  roughly 41,000 that 2.1 s pacing would allow.
- The most-played leaderboard changes once a day but is fetched every hour,
  because a missed day cannot be fetched later. Repeated identical responses
  cost only a metadata row in the raw landing layer.
- Jobs run one at a time in the scheduler, so their budgets do not overlap.
  A manual `docker compose run pipeline` can overlap the scheduler; the
  per-job locks keep the same job from running twice, but two different jobs
  may run at once. Their combined Store requests stay far below the budget.
