# Optimizations

Changes made to cut processing time, each found and checked with the run
step timings in `run_steps` (see [`observability.md`](observability.md#run-step-timing)
and the **Steps** tab on Pipeline). Every entry records what was
slow, what changed, the measured effect, and why the change is safe.

Measure a change the same way: compare the job's steps before and after,
for example

```sql
SELECT run_id, step, duration_ms / 1000 AS seconds, wait_ms / 1000 AS waiting,
       requests
FROM run_steps
WHERE run_id IN (63, 66) AND (parent_step IS NULL OR parent_step = 'job')
ORDER BY run_id, position;
```

## 1. Pace each API instead of pausing before every request (v1.1.0)

**Found:** the first timed `catalog_daily` run (local stack, live APIs, run
63) took 468.7 s. `store_details_and_tags` took 419.1 s, and 384.0 s of that
was deliberate waiting. For each of the 120 refreshed games the job paused
2.1 s before the Store request and then another 1.1 s before the SteamSpy
request: 3.2 s of pauses per game on top of the requests themselves. The
SteamSpy pause was redundant, because the Store pause had already spaced the
SteamSpy requests further apart than SteamSpy needs. The fixed pauses also
ignored the time the previous request and its loading had already taken.
`prices` (48.7 s, 42.0 s waiting) and the review histograms used the same
pattern.

**Changed:** a pacer per rate-limited API and run (`src/api/pacing.py`,
`RunContext.pace("store")`, `RunContext.pace("steamspy")`). Before a request
it waits only until the API's interval has passed since the start of that
API's previous request. Time spent on responses, loading and other APIs'
requests counts towards the interval, so the SteamSpy wait inside the
details loop is usually zero, and the Store wait is 2.1 s minus whatever the
game's two requests and loading took. One Store pacer is shared by details,
price batches and histograms, and the SteamSpy page request counts towards
SteamSpy's interval. Replaced the fixed pauses in `dimensions.py`,
`prices.py`, `review_history.py` and the tracked-app SteamSpy lookups.

**Measured** (local stack, live APIs, same request count):

| Step | Before (run 63) | After (run 66) |
| --- | --- | --- |
| `catalog_daily` total | 468.7 s | 292.8 s (−37%) |
| `store_details_and_tags` | 419.1 s, 384.0 s waiting | 250.2 s, 216.5 s waiting |
| `prices` | 48.7 s, 42.0 s waiting | 42.0 s, 35.7 s waiting |
| Store requests | 140 | 140 |
| SteamSpy requests | 121 | 121 |

The remaining wait is the Store limit itself: 140 Store requests at one per
2.1 s is about 294 s.

**Why it is safe:** the guarantee is unchanged. Request starts to one API are
never closer than its interval (2.1 s Store, 1.1 s SteamSpy), which
`tests/test_pacing.py` checks. In run 66 the Store requests arrived on
average 2.10 s apart (2.00 s at least, measured at response time, which
jitters with latency), and at most 140 fell into any 5-minute window against
the limit of 150. A 2.1 s interval allows at most 143 in any 5 minutes,
whatever `STORE_DETAILS_PER_RUN` is set to.

## 2. Fetch SteamSpy tags only when they are due (v1.2.0)

**Found:** SteamSpy is frozen (see [`sources.md`](sources.md#steamspy-catalog-page)).
Its per-game `appdetails`, which the catalog job called for every game whose
Store details it refreshed, returned the same values as the frozen page for
240 of 240 games. Each game's Store details are refreshed about every 8–9
days (1,000 games, 120 a day), so its tags were fetched again every 8–9 days
with nothing new in them.

**Changed:** tags are fetched only for refreshed games whose tags are missing
or older than `TAG_REFRESH_DAYS` (default 30; 0 restores fetching them with
every refresh). Reprocessing a run fetches tags for the same games the run
did.

**Measured:** this saves requests, not time. In the details loop each SteamSpy
request already ran inside the Store's 2.1 s interval (entry 1), so skipping
it shortens the run by at most its response time, about 0.1–0.3 s per game.
Requests per day, once every game has tags:

| | Before | After |
| --- | --- | --- |
| SteamSpy `appdetails` for tags | 120 a day | about 33 a day (120 refreshes × 9 / 30) |

The first run with the change (local stack, run 67) still fetched 120 tags,
because its 120 games had none yet. `integration/test_dimensions.py` checks
that a second refresh within `TAG_REFRESH_DAYS` makes no tag requests.

**Why it is safe:** games without tags get them on their first refresh as
before, and every game's tags are refreshed at least every
`TAG_REFRESH_DAYS`, so SteamSpy updating again would reach every game's tags
within 30 days, and the staleness check would show it.

## 3. Price batches of 200 instead of 50 (v1.2.1)

**Found:** on TrueNAS after the 5,000-game upgrade, `catalog_daily` took about
212 s on days without SteamSpy pages, and almost all of it was the `prices`
step: 101 batch requests of 50 games at the Store's 2.1 s interval. The
batch size of 50 was a cautious guess, because the maximum is undocumented.

**Tested** (live Store, 2026-10-20): batches of 50, 100, 200, 300 and 500 app
IDs each returned an entry for every requested ID (a delisted game answers
`success: false`), in 0.35 s to 1.1 s per request. 1,000 IDs failed with
`414 Request-URI Too Large`: the limit is the length of the URL, not the API.

**Changed:** `PRICE_BATCH_SIZE` from 50 to 200. That is under half the URL
length that worked, leaving room for 7-digit app IDs.

**Measured** (local stack, live Store, the same 5,040 games):

| `prices` step | Before (run 72) | After (run 75) |
| --- | --- | --- |
| Requests | 101 | 26 |
| Duration | 211.0 s | 52.9 s (−75%) |
| Waiting between requests | 178.0 s | 37.0 s |
| Games priced | 5,040 | 5,040 (324 new versions, 4,716 unchanged) |

On days without SteamSpy pages the whole catalog run goes from about 212 s
to about 54 s.

**Why it is safe:** the same games are priced with the same parser. The
requests are still paced at 2.1 s, there are fewer of them, and each answer
is about 1 s even at 500 IDs. A batch that fails is a run warning and keeps
the previous prices, as before, so a future URL limit would show up as a
warning, not as wrong data.

## 4. Review summaries at 1.2 s instead of 2.1 s (v1.2.1)

**Found:** on TrueNAS, `refresh_hourly` took about 508 s an hour, and 443 s of
it was 211 review summaries at the 2.1 s Store interval. That interval comes
from the reported limit for `api/appdetails`; the review endpoint
(`appreviews`) was only *assumed* to share it.

**Tested** (live, 2026-10-20, 11:47–12:07, a window in which TrueNAS sent no
Store requests): a probe asked for review summaries at rising rates, five
minutes each, without automatic retries, ready to stop at the first refusal
or slowdown.

| Spacing | Requests in 5 minutes | Refusals | Median answer |
| --- | ---: | ---: | ---: |
| 1.8 s | 168 | 0 | 0.21 s |
| 1.5 s | 200 | 0 | 0.21 s |
| 1.2 s | 250 | 0 | 0.21 s |
| 1.0 s | 300 | 0 | 0.22 s |

**Changed:** review summaries have their own pacer, `store_reviews`, at 1.2 s
(250 per 5 minutes), one step below the fastest rate tested. Store details,
prices and histograms stay at 2.1 s. In `refresh_hourly` the two kinds of
request run one after the other, never interleaved, so their rates do not
add up.

**Measured** (local stack, live APIs, 30 Store details and 210 summaries per
run):

| | Before (run 74) | After (run 76) |
| --- | --- | --- |
| `review_summaries` step | 441.0 s | 251.0 s (−43%) |
| Whole `refresh_hourly` run | 502.7 s | 312.6 s (−38%) |

**Since v1.2.2:** Valve switched `appreviews` off on 2026-10-22; summaries
come from the Web API's GetAppReviews. A probe of that endpoint on
2026-10-23 (144, 201, 251, 301 requests per 5 minutes, no refusal) kept the
1.2 s pacing; the pacer is now called `reviews`.

**Why it is safe:** the rate stays 20% below a rate that ran for five minutes
without refusal. A 429 answer is retried with growing pauses (honouring
`Retry-After`) and then becomes a run warning that keeps the previous data,
so overshooting would cost a delay, not data.

## 5. Quiet games get their review summary every 3 days (v1.2.1)

**Found:** `refresh_hourly` asks for every game's review summary every day
(`REVIEWS_REFRESH_DAYS` 1), but many games get no new reviews from one day
to the next. On TrueNAS about 45 of the 211 summaries in a run (21%) had
the same review total as the game's previous summary: the request told us
nothing new.

**Changed:** a game whose last two summaries had the same `total_reviews`
is *quiet* and waits `QUIET_REVIEWS_REFRESH_DAYS` (default 3) for its next
summary; every other game stays daily. As soon as a total changes, the game
is daily again. Each run picks the games that are due, the longest-overdue
first, up to the same slice size as before, so a run with fewer games due
simply makes fewer requests. `0` turns quiet games off.

**Measured** (scale test, 1,000 synthetic games, 6 simulated days; the
synthetic totals never change, so every game turns quiet after its second
summary):

| Simulated day | 1 | 2 | 3 | 4 | 5 | 6 | 7 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `refresh_hourly` runs | 4 | 24 | 24 | 24 | 24 | 24 | 20 |
| Review summaries | 172 | 1,027 | 855 | 0 | 172 | 984 | 860 |

Each game gets its first and second summary in the first two days, then
waits three days; the next round starts on day 5, three days after the
second. The schedule ran 6 days from 20:00, hence the partial first and
last days. A quiet game costs a third of the requests of a daily one.

**Expected on live data:** if 21% of games are quiet, the daily review
summaries drop from about 5,040 to about 4,330 (0.79 + 0.21 ÷ 3 = 0.86,
−14%), and `review_summaries` from about 251 s to about 216 s a run. Games
go in and out of quiet as their reviews come and go, so the share is
measured after the release on TrueNAS, not assumed.

**Measured on TrueNAS** (the first full days after v1.2.1, from
`daily_pipeline_summary`: the refresh's rows minus its 31 Store details a
run):

| Day | `refresh_hourly` rows | Review summaries | Against daily for every game |
| --- | ---: | ---: | ---: |
| 2026-10-21 | 3,092 | about 2,350 | −53% |
| 2026-10-22 | 3,694 | about 2,950 | −41% |

Far more games are quiet than the 21% unchanged in one day's raw answers
suggested: most of the catalog gets no new review for days at a time.

**Why it is safe:** active games keep their daily series. A quiet game's
newest review total can be up to three days old instead of one; its last
two summaries said it was not changing, and the first changed total puts
it back on the daily schedule. The tracked games are not affected:
`reviews_daily` fetches theirs every day.

## 6. Latest-review lookup in the catalog marts (v1.2.1)

**Found:** a 48-hour scale test at 5,000 games (docs/scale.md) slowed down
run after run. `run_steps` showed the cause: rebuilding `mart_tag_summary`
took 47.8 s, then 55.3 s, growing with every run, and the genre and
release-year marts grew too. On TrueNAS the mart phase had grown from 0.5 s
to 1.6 s in three days. The marts joined a derived "latest review summary
per game" table (`MAX(observation_id) ... GROUP BY appid`). MySQL evaluated
it against every game-tag row, so the cost grew with the review history
times the tag links. Marts are rebuilt after every job, so every hourly run
would have slowed down as the daily review series grew.

**Changed:** each mart looks up a game's latest summary with a correlated
subquery (`ORDER BY collected_at DESC, observation_id DESC LIMIT 1`). That
query is answered from the existing `(appid, collected_at)` index, one short
index read per game.

**Measured** (the scale-test database, 26,400 tag links and 9,230 review
summaries; the tag mart's query):

| `mart_tag_summary` query | Before | After |
| --- | --- | --- |
| Duration | 49.4 s | 0.11 s |

**Why it is safe:** it is the same definition of "latest summary", read
through an index. The rebuilt marts are compared row for row with the old
query in the same database, and the mart tests (keys, ranges, not empty)
still pass.

## 7. Review summaries alongside the Store work (v1.2.4)

**Found:** `refresh_hourly` fetched its Store details and tags (about 31
games, paced at 2.1 s) and then its review summaries (paced at 1.2 s), one
after the other. Since v1.2.2 the summaries come from the Web API
(GetAppReviews), a different server with its own limit, so nothing made
them wait for the Store work.

**Changed:** the summaries are fetched on a second thread while the main
thread does the Store details and tags. The worker has its own HTTP
session, its own raw-store connection and its own pacer; it loads nothing,
and the main thread loads its summaries after joining it. If the Store
step fails, the worker stops before its next request. Reprocessing replays
on one thread, as before. The `review_summaries` step is timed on the
worker and recorded under `job` next to `store_details_and_tags`, so in
`run_steps` the two overlap.

**Measured** (local stack, live APIs, the same slice: 31 Store details
with tags and 120 review summaries; run 79 with the v1.2.3 image, run 78
with this change):

| | Before (run 79) | After (run 78) |
| --- | --- | --- |
| `store_details_and_tags` | 63.3 s, from 0 s | 63.3 s, from 0 s |
| `review_summaries` | 143.1 s, from 63 s | 143.1 s, from 0 s |
| Whole run | 206.8 s | 143.7 s (−31%) |
| Waiting per pacer (Store / reviews) | 54.4 s / 110.7 s | 54.7 s / 111.0 s |
| Requests | 182 | 182 |

A run now takes as long as its longer part, the review summaries.

**Why it is safe:** each API sees exactly the pace it saw before: the same
pacers, the same intervals and the same waiting, only no longer in a queue
behind the other. The worker shares no connection or session with the main
thread, and every request is still counted once (`job` requests equal the
run's raw rows, tested).

## 8. Work that grew with history (v1.3.0)

**Found** by an independent code review: some queries read whole tables that
grow forever (raw metadata is kept after payloads are purged; review
observations have no retention), so their cost would keep rising, run after
run.

**Changed:**

- `mart_review_trends` is rebuilt incrementally after each job: the newest
  day of observations and the day before, plus every day of the apps with
  histogram rollups (their 30-day share can change for earlier days).
  Reprocessing rebuilds it in full. A game's first row in the window looks
  up its previous total through the `(appid, collected_at)` index.
- Migration V14 indexes the rest: freshness and schema drift read the
  newest answer per source through `(source, http_status, fetched_at)`
  ("Using index for group-by", one entry per source); the daily purge only
  looks at originals still holding a body; the review rotation's recent
  window uses `review_observations (collected_at)`.

**Measured** (the scale-test database with 100 days of review history,
512,907 observations; five consecutive builds after a warm-up, median;
the machine was busy with other programs, so compare the rows, not the
absolute seconds):

| `mart_review_trends` | Median per build |
| --- | ---: |
| Before: full rebuild after every job | 25.0 s |
| After: incremental after every job | 2.2 s (−91%) |
| After: full rebuild (reprocessing only) | 25.6 s |

The old cost grows with the history; the incremental build stays the size
of two days.

**Why it is safe:** after 100 days of history, the old full rebuild and the
new one give identical rows (same count and checksum), and an integration
test checks that six days of incremental builds equal a full rebuild row for
row. Rows outside the window cannot change, because a new observation always
lands on the current day; reprocessing, which can change any day, rebuilds
in full.

