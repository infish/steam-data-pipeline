# 3. Price history as a Type 2 slowly changing dimension

Status: accepted (2026-10)

## Context

Prices change rarely but matter when they do (sales, price cuts, a game
going free or being removed). The questions are historical: what is the
lowest price ever, how often is a game discounted, how long since the last
sale. A daily snapshot of 1,000 prices would repeat the same value almost
every day.

## Decision

Keep `game_price_history` as a Type 2 dimension per game and Store country:
each version has `valid_from`, `valid_to` and `is_current`. The daily check
(batched `price_overview` requests) closes the current version and opens a
new one only when something changed; otherwise it writes nothing.

- `price_state` (`priced`, `no_price`, `unavailable`) makes free-to-paid
  changes and removals from sale versions like any other.
- Amounts are stored in minor units with their currency and never compared
  across currencies; the Store country is configurable.
- A generated unique key allows only one current version per game and
  country.

## Consequences

- History is compact and answers "as of" questions directly; derived values
  live in the `game_price_summary` view and `mart_price_status`.
- A change between two daily checks is seen only at the next check, so
  `valid_from` is the check time, not the moment the Store changed the price.
- History only moves forward: reprocessing an old catalog run does not
  replay its price check, because applying old prices after newer ones would
  corrupt the versions.
