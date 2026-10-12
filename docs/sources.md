# Data sources

This is the data contract for every external endpoint the pipeline uses or may
use. Each section says what is requested, which fields are used, what the
timestamps mean, how often the data can change, and what to watch out for.

Recorded responses live in `tests/fixtures/sources/<source>/`. Each was fetched
once on **2026-10-01 around 17:47 UTC** for Counter-Strike 2 (`730`), Factorio
(`427520`) and Portal 2 (`620`). `tests/fixtures/sources/manifest.json` lists
the URL, parameters, HTTP status, fetch time and response size of every
recording, and how it was trimmed. Later phases may rely only on fields
confirmed here.

Status values:

- **in use**: already called by the pipeline.
- **accepted**: confirmed and available to later phases.
- **accepted with caveats**: usable only as described in its caveats.
- **rejected**: do not build on it.

## Summary

| Source | Status | Timestamp from the source | Fresh at most every |
| --- | --- | --- | --- |
| [SteamSpy catalog page](#steamspy-catalog-page) | in use | none | 1 day |
| [SteamSpy app details](#steamspy-app-details) | in use; tags accepted | none | 1 day |
| [Valve top concurrent players](#valve-top-concurrent-players) | in use | `last_update` | a few minutes |
| [Valve current players](#valve-current-players) | in use | none | ~8 minutes |
| [Steam review summary](#steam-review-summary) | in use; `review_score_desc` accepted | none | 10 minutes |
| [Store app details](#store-app-details) | accepted | none | 1 hour |
| [Store price batch](#store-price-batch) | accepted (only with `filters=price_overview`) | none | 1 hour |
| [Steam review histogram](#steam-review-histogram) | accepted with caveats | bucket dates | 1 day |
| [Valve most played (daily peak)](#valve-most-played-daily-peak) | accepted | `rollup_date` | 1 day |
| [Steam news](#steam-news) | accepted | `date` per item | as published |

Rejected:

- Store `appdetails` with several app IDs **without** `filters=price_overview`:
  HTTP 400, body `null`.
- Review histogram weekly rollups: `results.weeks` was empty for all three
  apps and `rollup_type` was `month`. Plan only for monthly and daily buckets.
- Review histogram absolute counts as a substitute for review totals: they
  differ from the review summary by up to 15% (see its caveats).
- `genre` and `languages` from the SteamSpy catalog page: those fields are not
  in `request=all` rows (see below).

## Rate limits

No endpoint returned rate-limit headers. The budgets below are the ones the
pipeline plans around; Phase 12 turns them into a per-job request budget.

| Source | Limit used for planning | Basis |
| --- | --- | --- |
| SteamSpy `request=all` | 1 request per 60 s | SteamSpy API documentation |
| SteamSpy other requests | 1 request per s | SteamSpy API documentation |
| Store `api/appdetails` (incl. price batch) | 150 requests per 5 min | informally reported ~200 per 5 min, with a margin |
| Store `appreviews`, `appreviewhistogram` | shares the Store budget | undocumented; assumed |
| Valve Web API (`api.steampowered.com`) | keep modest, no key used | no published keyless limit |

All responses in the recording took under 0.3 s.

---

## SteamSpy catalog page

**Status:** in use.

- URL: `https://steamspy.com/api.php?request=all&page=<page>`
- Returns 1,000 games per page as an object keyed by app ID string.
- Fields used: `appid`, `name`, `developer`, `publisher`, `owners`,
  `positive`, `negative`, `average_forever`, `average_2weeks`,
  `median_forever`, `median_2weeks`, `ccu`, `price`, `initialprice`,
  `discount`.
- Fields present but unused: `score_rank`, `userscore`.
- **Not present:** `genre`, `languages`, `tags`. The pipeline reads `genre`
  and `languages` from each row, so for page rows they have always been
  stored as NULL. A read-only check of a local deployment on 2026-10-01 found
  0 of 1,000 games with either value.

Timestamps: none. The pipeline's `snapshot_time` is collection time only.

Freshness: `Cache-Control: max-age=86400`; the response carried an `Age` and a
`Last-Modified` header, so it is served from a cache that refreshes at most
daily. Values can stay unchanged for long periods; see the September 2026
stale-data incident in the README. SteamSpy values are treated as catalog
metadata, not as a time series.

Caveats: `owners` is a range string (`"100,000,000 .. 200,000,000"`); prices
are US cents as strings; `ccu` is not a reliable current-player count.

Fixture: `steamspy_all/page0_tracked.json` (the three tracked apps out of the
1,000 rows). All three were on page 0 at recording time.

## SteamSpy app details

**Status:** in use (backfill of tracked apps missing from the page, Phase 6);
`tags` accepted for Phase 13.

- URL: `https://steamspy.com/api.php?request=appdetails&appid=<appid>`
- Fields: everything in a page row, plus `genre` (comma-separated),
  `languages` (comma-separated), and `tags` (object of tag name → vote count;
  20 tags per app in the recording).

Timestamps: none. Freshness: `Cache-Control: max-age=86400`; one recording had
`Age: 8353` (served from a cache refreshed about 2.3 hours earlier).

Caveats: an unknown app returns an object without a usable `name`; the Phase 6
backfill treats that as a failure. Tag vote counts are SteamSpy's, not
Valve's, and inherit SteamSpy's staleness. At 1 request per second, fetching
all 1,000 catalog games takes about 17 minutes.

Fixtures: `steamspy_appdetails/<appid>.json`.

## Valve top concurrent players

**Status:** in use.

- URL: `https://api.steampowered.com/ISteamChartsService/GetGamesByConcurrentPlayers/v1/`
- Fields used: `response.last_update`, `response.ranks[].rank`, `appid`,
  `concurrent_in_game`.
- Field present but unused: `peak_in_game`, which the recording shows as
  higher than `concurrent_in_game`, apparently the peak over a recent window.
  Its window is not documented, so it is not used.
- Exactly 100 rows.

Timestamps: `last_update` is a Unix time for when Valve measured the values.
The pipeline stores it as `source_measured_at`. In the recording it was 4 min
51 s before the fetch.

Freshness: `Cache-Control: no-cache, no-store`; the update interval is not
documented but is a matter of minutes. One request covers all 100 games, which
makes hourly collection cheap.

Fixture: `top_concurrent/all.json`.

## Valve current players

**Status:** in use (tracked apps outside the top 100).

- URL: `https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid=<appid>`
- Fields used: `response.player_count`, `response.result` (must be `1`).

Timestamps: none; the pipeline uses collection time.

Freshness: `Cache-Control: public, max-age=466`, so a value can be up to about
8 minutes old.

Fixtures: `current_players/<appid>.json`.

## Steam review summary

**Status:** in use; `review_score_desc` accepted.

- URL: `https://store.steampowered.com/appreviews/<appid>`
- Parameters: `json=1`, `filter=recent`, `language=all`, `review_type=all`,
  `purchase_type=all`, `num_per_page=1`, `filter_offtopic_activity=0`.
- Fields used: `success` (must be `1`), `query_summary.total_positive`,
  `total_negative`, `total_reviews`.
- **Confirmed present:** `query_summary.review_score` (integer, e.g. `9`) and
  `review_score_desc` (e.g. `"Very Positive"`, `"Overwhelmingly Positive"`).

Timestamps: none; totals are as of the fetch.

Freshness: `Cache-Control: private, max-age=600` (10 minutes).

Caveats: the response also contains one full user review and a paging cursor;
neither is used or stored. Totals can decrease when Steam reclassifies
reviews.

Fixtures: `review_summary/<appid>.json` (the review text is removed).

## Store app details

**Status:** accepted (Phase 13b).

- URL: `https://store.steampowered.com/api/appdetails?appids=<appid>&cc=us&l=english`
- One app ID per request.
- Fields to use from `<appid>.data`: `type`, `name`, `is_free`,
  `required_age`, `release_date.coming_soon`, `release_date.date`,
  `metacritic.score`, `platforms.windows`, `platforms.mac`, `platforms.linux`,
  `recommendations.total`, `price_overview` (see the price batch),
  `genres[].id`/`description`, `categories[].id`/`description`,
  `developers[]`, `publishers[]`, `supported_languages`.
- `<appid>.success` must be `true`.

Timestamps: none. `release_date.date` is display text in the language from
`l` (observed: `"Aug 21, 2012"`). Counter-Strike 2 shows its predecessor's 2012
date.

Freshness: `Cache-Control: public, max-age=3600` with `Last-Modified` on the
hour, so the data is an hourly snapshot. Refreshing more often than daily has
no value except for prices during sales.

Caveats:

- Keys can be missing entirely: Counter-Strike 2 has no `metacritic` and no
  `price_overview` (free to play). Treat a missing key as unknown, not zero.
- `release_date.date` is free text. Only exact dates were observed; parse
  defensively and store NULL with a warning when it does not parse.
- `required_age` was an integer here, but parse it defensively.
- `supported_languages` is an HTML string (`"English<strong>*</strong>, ..."`,
  where `*` marks full audio). Strip tags before splitting.
- Genre IDs are strings (`"1"`); category IDs are integers.

Fixtures: `store_appdetails/<appid>.json` (trimmed to the fields above).

## Store price batch

**Status:** accepted (Phase 14), only with `filters=price_overview`.

- URL: `https://store.steampowered.com/api/appdetails?appids=<a>,<b>,...&cc=us&filters=price_overview`
- **Batching confirmed** with `filters=price_overview`: three app IDs in one
  request returned three entries. Without the filter the same request returned
  HTTP 400 with body `null`.
- Fields: `<appid>.success`, `<appid>.data.price_overview.currency`,
  `initial`, `final` (both in cents), `discount_percent`.

Timestamps: none. Freshness: `max-age=3600`, same hourly snapshot as app
details.

Caveats:

- A game with no price (free to play) returns `"data": []`, **an empty list,
  not an object**. Parsers must handle both.
- The currency follows `cc`; `cc=us` returned `USD`. Never mix currencies.
- The maximum number of app IDs per request is not documented; test before
  using batches larger than about 100.

Fixtures: `store_price_batch/batch.json`, `batch_without_filter.json`.

## Steam review histogram

**Status:** accepted with caveats (Phase 15). Undocumented endpoint.

- URL: `https://store.steampowered.com/appreviewhistogram/<appid>?l=english&review_score_preference=0`
- Fields: `success` (must be `1`), `count_all_reviews` (boolean),
  `results.rollup_type` (`"month"` for all three apps),
  `results.start_date`, `results.end_date`,
  `results.rollups[]` and `results.recent[]`, each with `date`,
  `recommendations_up`, `recommendations_down`.
- `rollups` covered each game's whole life (172 monthly rows for
  Counter-Strike 2, from 2012-05); `recent` covered the last 30 days, one row
  per day.

Timestamps: each bucket's `date` is a Unix time at **00:00 UTC on the first
day of the bucket** (month start for `rollups`, day for `recent`). These are
real source dates, so history can be backfilled without inventing values.

Freshness: buckets are daily at best. **The current month and the current day
are partial** and change on every fetch; store them but mark them as
incomplete, or re-fetch them later.

Caveats:

- Weekly rollups (`results.weeks`) were empty; do not plan for them.
- The sum of `rollups` does **not** match the review summary's
  `total_reviews`: +7.7% for 730, −14.8% for 427520, −8.5% for 620. The sign
  follows `count_all_reviews` (`true` only for 730), so the two endpoints
  count different review sets. The **positive share** agreed within 0.3
  percentage points for all three apps (86.1% vs 85.8%, 96.9% vs 97.2%, 98.5%
  vs 98.7%). Use the histogram for sentiment trends and relative volume, and
  reconcile shares, not counts.
- Some responses have `past_events`, a list of `{type, start_date, end_date}`
  windows whose meaning is undocumented (possibly off-topic review periods).
  Not used.
- Being undocumented, the endpoint may change or disappear; the Phase 17
  schema-drift check should watch it.

Fixtures: `review_histogram/<appid>.json`.

## Valve most played (daily peak)

**Status:** accepted (Phase 12).

- URL: `https://api.steampowered.com/ISteamChartsService/GetMostPlayedGames/v1/`
- Fields: `response.rollup_date`, `response.ranks[].rank`, `appid`,
  `last_week_rank`, `peak_in_game`.
- Exactly 100 rows.

Timestamps: `rollup_date` is 00:00 UTC of the **day the peaks refer to**. A
fetch on 2026-10-01 17:46 UTC returned `rollup_date` 2026-09-30, so the data is
for the previous complete UTC day.

Freshness: `Cache-Control: no-cache, no-store`, but the content changes once a
day. One request per day is enough.

Caveats: only the top 100 by daily peak; Factorio and Portal 2 were not
included. `last_week_rank` is `-1` for games that were not ranked the week
before (5 of 100 in the recording); store it as NULL. `peak_in_game` here is a daily peak and is not comparable with the
instantaneous `concurrent_in_game` from the top concurrent feed.

Fixture: `most_played/all.json`.

## Steam news

**Status:** accepted (Phase 15).

- URL: `https://api.steampowered.com/ISteamNews/GetNewsForApp/v2/?appid=<appid>&count=20`
- Fields to use: `appnews.newsitems[].gid` (unique ID, string), `title`,
  `url`, `date`, `feedlabel`, `feedname`, `feed_type`, `tags`.
- `appnews.count` is the total number of items Steam holds for the app (1,766
  for 730); one request returns the newest `count` items.

Timestamps: `date` is a Unix time of publication (source time).

Freshness: `Expires` one hour after the fetch.

Caveats:

- **Patch notes are tagged:** `tags` contains `"patchnotes"` on update posts
  (15 of 20 items for 730, 9 of 20 for 427520 and 620). `is_patch_notes` can be
  a tag check rather than a title heuristic.
- `tags` is missing entirely on items without tags.
- `feed_type` `1` = the developer's Steam community announcements; `0` =
  external news sites (`feedlabel` such as `"PC Gamer"`). Only `feed_type = 1`
  should count as a developer event.
- `contents` holds the full article text, including third-party articles; it
  is not stored.

Fixtures: `news/<appid>.json` (without `contents` and `author`).
