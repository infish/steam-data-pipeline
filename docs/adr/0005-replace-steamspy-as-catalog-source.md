# 5. Replace SteamSpy as the source of the game list

Status: **accepted** (2026-10-17), implemented in v1.2.0 (see
"Implementation" at the end for what differs from the proposal and the
answers chosen for the open questions).

## Context

SteamSpy is frozen (`docs/sources.md`). From 2026-09-01 to 2026-10-14 its
owner ranges, review counts, playtimes and `ccu` never changed. The page
always held the same 1,000 app IDs, and names predate 2023. The
per-game endpoint returns the same values. v1.2.0 treats it as a frozen
reference (ADR 4, update of 2026-10-17), but it still decides **which games
the pipeline knows about**:

- `catalog_daily` loads `games` and `game_metric_snapshots` from the SteamSpy
  page; `current_game_metrics` and the catalog marts (genres, tags, release
  years, engagement) are built on that list.
- `players_hourly` skips top-100 games that are not in `games`: about 30 of
  Valve's top 100 every hour ("top-100 games not in catalog page; skipped"),
  so currently popular games that SteamSpy's frozen list lacks are never
  measured.
- `games.name`, `developer` and `publisher` come from SteamSpy strings, so
  names are stale and the company links inherit that.
- Owner estimates and playtimes exist **only** in SteamSpy; user tags too.

## Proposal

Build the game list from what Valve and the Store report now, and keep
SteamSpy only as a frozen, labelled reference.

1. **The list grows from Valve's charts.** `players_hourly` adds any app in
   the top-100 concurrent chart or the most-played leaderboard to `games`
   instead of skipping it, with `catalog_source = 'valve_chart'` and a
   placeholder name until its Store details arrive. Tracked apps stay in.
   Existing SteamSpy members stay as `catalog_source = 'steamspy'`, so
   history is kept. Expected growth: a few games a day at first, then
   slower.
2. **Names and companies from the Store.** Store details already include the
   name, developers and publishers. Store them (migration V13:
   `game_details.store_name`, and developer and publisher links from the Store
   lists instead of parsed SteamSpy strings). The views prefer the Store name
   when present. New games get Store details first, because `select_stalest`
   already puts games without details first.
3. **Review scores from Steam.** Fetch the Steam review summary for the whole
   list on a rotation (`REVIEW_SUMMARIES_PER_RUN`, for example 120 a day),
   alongside the three tracked games done daily now. The Market explorer's
   "SteamSpy score" becomes the Steam positive share.
4. **SteamSpy's own figures** (owner estimates, playtimes, tags) stay as the
   last snapshot for games that have them, labelled frozen. Games added from
   Valve's charts have none. The engagement mart (peak players versus
   estimated owners) only covers SteamSpy-era games, and says so.
5. **SteamSpy requests:** the page request can drop to weekly, as a cheap
   "has SteamSpy come back?" probe that feeds the staleness check. Tags stay
   on `TAG_REFRESH_DAYS`.

## Request budget impact

| Source | Today (v1.2.0) | Proposed |
| --- | --- | --- |
| Store `appdetails` | 120 a day | 120 a day (new games first) |
| Store `appreviews` (summary) | 3 a day | 3 tracked + 120 rotating = 123 a day |
| Store price batches | ≈ 20 a day | ≈ 20 a day, growing with the list |
| SteamSpy page | 1 a day | 1 a week |
| SteamSpy `appdetails` | ≈ 40 a day | ≈ 40 a day, then fewer |

The review summaries would run in `reviews_daily`, paced with the shared
Store interval. That adds about 4.3 minutes (123 × 2.1 s) to that job and
stays inside the Store's 150 per 5 minutes, because the catalog and reviews
jobs run 30 minutes apart.

## What is lost

- Owner estimates and playtimes for games added after the switch. No public
  API replaces them.
- User tags for those games. The Store's genres and categories remain.
- Comparability of "catalog size" over time: the list grows instead of
  staying at 1,000. Market explorer counts would say which source a game
  came from.

## Rollout

Each step is separately releasable and reversible:

1. V13 and the Store name and companies (no behaviour change for the list).
2. Valve-chart games added by `players_hourly` (behind a setting,
   `CATALOG_FROM_VALVE_CHARTS=true`, default off for one release).
3. Rotating review summaries, and the dashboard switched to the Steam review
   share.
4. Weekly SteamSpy page, and the dashboard showing SteamSpy figures only
   where they exist.

Each step gets integration tests, a measured entry in `optimizations.md`
where it changes run time, and a CHANGELOG entry.

## Open questions for the reviewer

- Should the list be capped? For example, drop games from the "active" set
  after N days without appearing in Valve's charts, while keeping their
  history.
- Keep the SteamSpy-era 1,000 games forever as members, or only while they
  still appear in Valve's charts or are tracked?
- How many review summaries a day? 120 refreshes a 1,000-game list about
  every 8 days.
- Is losing owner estimates and playtime for new games acceptable, or should
  the engagement view be dropped altogether once most games lack them?

## Implementation (v1.2.0)

All four rollout steps shipped together, after review:

- **V13:** `games.catalog_source` (`steamspy`, `valve_chart`, `tracked`),
  `games.name_source` (`store`, `steamspy`, `placeholder`), and
  `game_details.companies_from_store`. The Store name is written to
  `games.name` itself, not a separate column, so every query and mart that
  shows a name gets the current one without changes.
- **Valve's charts:** `players_hourly` adds games from the top-100 chart and
  the most-played leaderboard, and measures them in the same run.
  `CATALOG_FROM_VALVE_CHARTS` defaults to **true**, not false as proposed:
  the change was asked for, and setting it to false restores the old
  behaviour without a rollback. Tracked apps without a games row get one
  (`tracked`), even when SteamSpy has no row for them.
- **The SteamSpy page** is fetched every `STEAMSPY_PAGE_INTERVAL_DAYS`
  (default 7; 1 = every run). `latest_successful_run` now means the latest
  catalog run with snapshots, so `current_game_metrics` keeps working on the
  other days. The snapshot volume check was removed, and SteamSpy's
  freshness interval follows the setting.
- **Review summaries** for the catalog games on a rotation, paced with the
  Store limit. ADR 6 moved the rotation, and the Store details, into the
  hourly `refresh_hourly` job, with a daily summary for every game. The genre, tag *and release-year*
  marts take their review score from them; the release-year mart was not
  named in the proposal but used the same SteamSpy score.
- A new check, `new_games_have_store_details`, warns when a game added from
  the charts still has no Store details after 3 days.

Answers chosen for the open questions (each can change later without a
migration):

1. **No cap.** Games stay in the catalog; `last_seen_at` records when a chart
   game was last seen, for an "active" filter later.
2. **The SteamSpy-era games stay** as members, with their frozen snapshot.
3. **120 review summaries a day**, which refreshes a 1,000-game catalog about
   every 8 days.
4. **The engagement mart stays** for games with a SteamSpy snapshot, labelled
   as SteamSpy-based; revisit once most games lack one.

