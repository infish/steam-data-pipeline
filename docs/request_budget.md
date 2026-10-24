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
| | | Steam review summary | the games due: daily, or every `QUIET_REVIEWS_REFRESH_DAYS` (3) for games whose total did not change; at most catalog ÷ (`REVIEWS_REFRESH_DAYS` × 24) ≈ 211 at 5,000 games | ≤ 5,040; ≈ 4,330 if 21% are quiet |
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
| Steam Store (`api/appdetails`, price batches, `appreviewhistogram`) | ≈ 770 at 5,000 games | at most 1 per 2.1 s (paced) | 150 per 5 min | every run stays under 143 per 5 minutes by its pacing, and the scheduler runs one job at a time |
| Web API `GetAppReviews` (review summaries) | ≤ 5,040 at 5,000 games | at most 1 per 1.2 s (paced) | 250 per 5 min | tested without refusal at 300 per 5 minutes (sources.md, rate limits) |

## How the limits are enforced

Each rate-limited API has a pacer per run (`src/api/pacing.py`,
`RunContext.pace`): before a request it waits until the API's interval has
passed since the **start** of the previous request to the same API, and no
longer. Time spent on the previous response, on parsing and loading, or on
requests to other APIs counts towards the interval. Request starts are
therefore never closer than 2.1 s for the Store (1.2 s for review
summaries) or 1.1 s for SteamSpy, the same guarantee as the fixed pause before every request used before v1.1.0,
without its extra waiting ([`optimizations.md`](optimizations.md)). The
SteamSpy page request counts towards SteamSpy's interval, and one Store
pacer covers details, price batches and histograms in a run. Review
summaries shared that pacer in v1.2.0; since v1.2.1 they have their own
(`reviews`, 1.2 s), because a live probe showed the endpoint accepts 300 per
5 minutes. Since v1.2.2 they come from the Web API's GetAppReviews, probed
the same way. Since v1.2.4 they are fetched on a second thread while the
main thread does the Store details and tags. The two go to different
servers with separate limits, and each keeps its own pacer, so neither
sees a faster pace than before.

## When an API fails

Since v1.3.0 every failure is bounded, so a struggling API cannot hold up
the jobs after it (the scheduler runs one job at a time):

- **One request** waits at most 5 s to connect and 20 s for data, and is
  tried at most three times. Retries wait 5 s and then 10 s, never
  immediately, which is longer than any pacer's interval, so retries never
  exceed an API's paced rate. A `Retry-After` header is honoured up to
  30 s. Worst case about 90 s; before v1.3.0 it was 253 s (five tries of up
  to 45 s, an immediate first retry and no cap on `Retry-After`).
- **Retried attempts are recorded** in `raw_api_responses` as failed rows
  (`error_message` "retried: HTTP 503"), so the request counts in
  `run_steps` and on the dashboard show the real load.
- **Five struggling answers in a row** (no connection, a timeout, or 429/5xx
  after retries) stop a run's requests to that API, with a warning; the
  games not reached keep their previous data and are next in line in the
  following run. An answer about one game (a 404, an empty summary) shows
  the API is up and does not count.
- **Each job has a time limit** (`Job.time_limit_minutes`): `refresh_hourly`
  20 minutes (it starts at :37 and the next players run is at :07),
  `catalog_daily` 25, `reviews_daily` 15. Past it, a run makes no new
  requests, loads what it has and succeeds with a warning.
  `players_hourly`, with a handful of requests, has none.

## Notes

- `refresh_hourly` alternates one Store details request with one SteamSpy
  tag request per refreshed game, while a second thread fetches the review
  summaries (since v1.2.4). About 31 details take about 1 minute at 2.1 s,
  and at most 211 summaries about 4.2 minutes at 1.2 s, so a run takes as
  long as the longer of the two; with quiet games (entry 5 in
  optimizations.md) it is often about a minute. `REFRESH_MAX_REQUESTS`
  (300) caps a run's slices; both shrink in proportion above it (ADR 6,
  docs/scale.md).
- At 5,000 games the Store total is at most about 5,810 requests a day:
  about 770 at 2.1 s (details, prices, histograms) and at most 5,040 review
  summaries at 1.2 s. That is 2% and 7% of what each pacer would allow in a
  day (about 41,000 and 72,000).
- The most-played leaderboard changes once a day but is fetched every hour,
  because a missed day cannot be fetched later. Repeated identical responses
  cost only a metadata row in the raw landing layer.
- Jobs run one at a time in the scheduler, so their budgets do not overlap.
  A manual `docker compose run pipeline` can overlap the scheduler; the
  per-job locks keep the same job from running twice, but two different jobs
  may run at once. Their combined Store requests stay far below the budget.
