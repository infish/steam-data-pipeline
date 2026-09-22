"""Deterministic, streaming synthetic history generator."""

import argparse
import hashlib
import json
from datetime import datetime, timedelta

GENERATOR_VERSION = "scale-v1.0"
APPID_BASE = 2_000_000_000


def _number(seed, appid, day, label, modulus):
    payload = f"{GENERATOR_VERSION}|{seed}|{appid}|{day}|{label}".encode()
    return int.from_bytes(hashlib.blake2b(payload, digest_size=8).digest(), "big") % modulus


def synthetic_row(seed, game_index, day_index):
    """Return one stable logical row independent of batch/order/process."""
    appid = APPID_BASE + game_index
    popularity = max(1, 100_000 // (1 + game_index // 25))
    seasonal = (day_index % 30) * (1 + _number(seed, appid, 0, "season", 20))
    owners_low = popularity * 100 + day_index * (popularity // 8 + 1) + seasonal
    owners_high = owners_low + max(20_000, owners_low // 4)
    positive = max(0, owners_low // 35 + day_index * 3 + _number(seed, appid, day_index, "pos", 80))
    negative = max(0, positive // (5 + _number(seed, appid, 0, "ratio", 15)))
    total = positive + negative
    price = (0, 499, 999, 1499, 1999, 2999)[_number(seed, appid, 0, "price", 6)]
    discount = (0, 0, 0, 10, 20, 25, 50)[_number(seed, appid, day_index, "discount", 7)]
    return {
        "appid": appid,
        "name": f"Synthetic Game {game_index:06d}",
        "developer": f"Synthetic Developer {game_index % 200:03d}",
        "publisher": f"Synthetic Publisher {game_index % 50:02d}",
        "languages": "English",
        "genre": ("Action", "Indie", "Strategy", "RPG", "Simulation")[_number(seed, appid, 0, "genre", 5)],
        "owners": f"{owners_low:,} .. {owners_high:,}",
        "owners_low": owners_low,
        "owners_high": owners_high,
        "estimated_owners": (owners_low + owners_high) // 2,
        "positive_reviews": positive,
        "negative_reviews": negative,
        "total_reviews": total,
        "review_score_percent": round(positive / total * 100, 2) if total else None,
        "average_playtime_forever_minutes": _number(seed, appid, day_index, "avgf", 12000),
        "average_playtime_2weeks_minutes": _number(seed, appid, day_index, "avg2", 1200),
        "median_playtime_forever_minutes": _number(seed, appid, day_index, "medf", 8000),
        "median_playtime_2weeks_minutes": _number(seed, appid, day_index, "med2", 900),
        "ccu": max(0, popularity // 3 + _number(seed, appid, day_index, "ccu", 250)),
        "price_cents": price * (100 - discount) // 100,
        "initial_price_cents": price,
        "discount_percent": discount,
    }


def batches(seed, games, day_index, batch_size, start_game=1):
    if not 1 <= batch_size <= 1000:
        raise ValueError("batch size must be between 1 and 1000")
    for first in range(start_game, games + 1, batch_size):
        yield [synthetic_row(seed, index, day_index)
               for index in range(first, min(games + 1, first + batch_size))]


def batch_checksum(rows):
    encoded = json.dumps(rows, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def snapshot_time(start_date, day_index):
    return datetime.fromisoformat(start_date) + timedelta(days=day_index)


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", required=True)
    parser.add_argument("--games", type=int, required=True)
    parser.add_argument("--days", type=int, required=True)
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    if args.games < 1 or args.days < 1:
        raise SystemExit("games and days must be positive")
    if not 1 <= args.batch_size <= 1000:
        raise SystemExit("batch size must be between 1 and 1000")
    if args.dry_run:
        sample = synthetic_row(args.seed, 1, 0)
        print(json.dumps({"generator_version": GENERATOR_VERSION,
                          "expected_rows": args.games * args.days,
                          "sample_digest": batch_checksum([sample])}, sort_keys=True))
        return
    from benchmarks.load import load_scenario
    load_scenario(args)


if __name__ == "__main__":
    main()
