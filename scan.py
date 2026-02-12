#!/usr/bin/env python3
"""Kalshi cross-market subset arbitrage scanner.

Scans Kalshi sports markets to discover pairs where one contract resolving YES
logically guarantees another also resolves YES (e.g., winning the French Open
implies winning a Grand Slam). Uses programmatic pre-filtering to narrow
candidates, then Claude Haiku for implication checking.
"""

import argparse
import csv
import json
import logging
import re
import sys
import time
from itertools import combinations

import requests
from dotenv import load_dotenv

from kalshi import KALSHI_BASE

load_dotenv()

log = logging.getLogger("scan")


# ── Fetching ─────────────────────────────────────────────────────────────────


def fetch_series(category: str, filter_term: str | None) -> list[dict]:
    """Fetch series for a category, optionally filtering by keyword."""
    series = []
    cursor = None
    while True:
        params = {"limit": 200, "category": category}
        if cursor:
            params["cursor"] = cursor
        resp = requests.get(f"{KALSHI_BASE}/series", params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        batch = data.get("series", [])
        series.extend(batch)
        cursor = data.get("cursor")
        if not cursor or not batch:
            break

    if filter_term:
        terms = [t.strip().lower() for t in filter_term.split(",")]
        series = [
            s for s in series
            if any(
                t in s.get("ticker", "").lower() or t in s.get("title", "").lower()
                for t in terms
            )
        ]
    return series


def fetch_events_with_markets(series_ticker: str) -> list[dict]:
    """Fetch events (with nested markets) for a series."""
    events = []
    cursor = None
    while True:
        params = {
            "limit": 200,
            "series_ticker": series_ticker,
            "with_nested_markets": "true",
        }
        if cursor:
            params["cursor"] = cursor
        resp = requests.get(f"{KALSHI_BASE}/events", params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        batch = data.get("events", [])
        events.extend(batch)
        cursor = data.get("cursor")
        if not cursor or not batch:
            break
    return events


def fetch_markets(
    category: str,
    filter_term: str | None,
    min_volume: int = 0,
) -> list[dict]:
    """Fetch open markets by iterating filtered series.

    When filter_term is provided, only series matching the keyword are queried,
    making this very fast for narrow scans (e.g., 'tennis' -> ~20 series).
    """
    print(f"Fetching {category} series...")
    all_series = fetch_series(category, filter_term)
    print(f"  {len(all_series)} series{f' matching \"{filter_term}\"' if filter_term else ''}")

    markets = []
    for i, s in enumerate(all_series):
        sticker = s["ticker"]
        for attempt in range(3):
            try:
                events = fetch_events_with_markets(sticker)
                break
            except (requests.HTTPError, requests.ConnectionError, requests.Timeout) as e:
                if attempt < 2 and ("429" in str(e) or "timed out" in str(e).lower() or "No route" in str(e)):
                    time.sleep(2 ** attempt)  # 1s, 2s backoff
                    continue
                print(f"  Warning: failed to fetch events for {sticker}: {e}")
                events = []
                break
        time.sleep(0.2)  # rate limit between series
        for event in events:
            for m in event.get("markets", []):
                if m.get("status") not in ("open", "active"):
                    continue
                vol = int(m.get("volume", 0) or 0)
                if vol < min_volume:
                    continue
                markets.append({
                    "ticker": m["ticker"],
                    "series_ticker": sticker,
                    "event_ticker": event["event_ticker"],
                    "title": m.get("title", ""),
                    "yes_sub_title": m.get("yes_sub_title", ""),
                    "rules_primary": m.get("rules_primary", ""),
                    "expected_expiration_time": m.get("expected_expiration_time", ""),
                    "close_time": m.get("close_time", ""),
                    "last_price_dollars": m.get("last_price_dollars"),
                    "yes_ask_dollars": m.get("yes_ask_dollars"),
                    "no_ask_dollars": m.get("no_ask_dollars"),
                    "volume": vol,
                })
        if (i + 1) % 10 == 0 or i + 1 == len(all_series):
            print(f"  Processed {i + 1}/{len(all_series)} series ({len(markets)} markets)")

    print(f"  Total open markets: {len(markets)}")
    return markets


# ── Pre-filtering ────────────────────────────────────────────────────────────


def group_by_entity(markets: list[dict]) -> dict[str, list[dict]]:
    """Group markets by yes_sub_title (entity).

    Only keeps entities that appear in 2+ different series, since same-series
    markets can't form subset relationships.
    """
    groups: dict[str, list[dict]] = {}
    for m in markets:
        entity = m.get("yes_sub_title", "").strip()
        if not entity:
            continue
        groups.setdefault(entity, []).append(m)

    # Filter to entities in 2+ series
    multi_series = {}
    for entity, entity_markets in groups.items():
        series_set = {m["series_ticker"] for m in entity_markets}
        if len(series_set) >= 2:
            multi_series[entity] = entity_markets

    return multi_series


def generate_candidate_pairs(groups: dict[str, list[dict]]) -> list[tuple[dict, dict]]:
    """Generate cross-series candidate pairs for each entity.

    Only pairs markets from different series — same-series pairs are never
    subset relationships.
    """
    pairs = []
    for entity, entity_markets in groups.items():
        for a, b in combinations(entity_markets, 2):
            if a["series_ticker"] != b["series_ticker"]:
                pairs.append((a, b))
    return pairs


# ── LLM screening ───────────────────────────────────────────────────────────

SCREENING_PROMPT = """\
/no_think
You are analyzing Kalshi prediction market contracts to find "subset" relationships.

A subset relationship exists when one contract resolving YES **logically guarantees** another also resolves YES. For example:
- "Player X wins the French Open" YES → "Player X wins a Grand Slam" YES (winning FO is one way to win a GS)
- "Team Y wins the Super Bowl" YES → "Team Y makes the playoffs" YES

For each candidate pair below, determine if such an implication exists in either direction.

IMPORTANT:
- Read the rules_primary carefully — some markets have non-obvious resolution conditions
- If uncertain, use "low" confidence rather than "none" — err toward inclusion
- Think about logical necessity, not just correlation

Return a JSON object (no markdown fencing) with a "results" key containing an array with one object per pair:
{"results": [
  {
    "subset_ticker": "..." or null,
    "superset_ticker": "..." or null,
    "confidence": "high" | "medium" | "low" | "none",
    "reasoning": "short explanation"
  }
]}

"confidence" of "none" means no subset relationship exists — set subset_ticker and superset_ticker to null.
"subset_ticker" is the MORE SPECIFIC market (e.g., "wins French Open"), "superset_ticker" is the BROADER market (e.g., "wins a Grand Slam").

CANDIDATE PAIRS:
"""


def format_pair_for_llm(idx: int, a: dict, b: dict) -> str:
    """Format a candidate pair for the LLM prompt."""
    return (
        f"\n--- Pair {idx} ---\n"
        f"Market A:\n"
        f"  ticker: {a['ticker']}\n"
        f"  title: {a['title']}\n"
        f"  rules: {a['rules_primary'][:500]}\n"
        f"  expiration: {a['expected_expiration_time']}\n"
        f"\n"
        f"Market B:\n"
        f"  ticker: {b['ticker']}\n"
        f"  title: {b['title']}\n"
        f"  rules: {b['rules_primary'][:500]}\n"
        f"  expiration: {b['expected_expiration_time']}\n"
    )


def _call_ollama(prompt: str, model: str) -> str:
    """Call Ollama's OpenAI-compatible chat API."""
    resp = requests.post(
        "http://localhost:11434/api/chat",
        json={
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "stream": False,
            "options": {"temperature": 0.2, "num_predict": 4096},
            "format": "json",
        },
        timeout=600,
    )
    resp.raise_for_status()
    return resp.json()["message"]["content"]


def _call_anthropic(prompt: str, model: str) -> str:
    """Call Anthropic Messages API."""
    import anthropic
    client = anthropic.Anthropic()
    response = client.messages.create(
        model=model,
        max_tokens=4096,
        messages=[{"role": "user", "content": prompt}],
    )
    return response.content[0].text


def _extract_json(text: str) -> list[dict]:
    """Extract a JSON array from LLM output, stripping think tags and markdown fencing."""
    text = text.strip()
    # Strip Qwen3 <think>...</think> reasoning block
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL).strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1]
        if text.endswith("```"):
            text = text[: text.rfind("```")]
        text = text.strip()
    parsed = json.loads(text)
    # Ollama with format=json may wrap in an object
    if isinstance(parsed, dict):
        for key in ("pairs", "results", "data"):
            if isinstance(parsed.get(key), list):
                return parsed[key]
        # If it's a single result dict, wrap it
        if "subset_ticker" in parsed:
            return [parsed]
    return parsed


def screen_pairs_with_llm(
    pairs: list[tuple[dict, dict]],
    provider: str,
    model: str,
    batch_size: int = 12,
) -> list[dict]:
    """Screen candidate pairs using an LLM for implication checking.

    Supports 'ollama' and 'anthropic' providers. Batches pairs and returns
    confirmed + uncertain results.
    """
    call_fn = _call_ollama if provider == "ollama" else _call_anthropic
    results = []
    total_batches = (len(pairs) + batch_size - 1) // batch_size

    for batch_idx in range(0, len(pairs), batch_size):
        batch = pairs[batch_idx : batch_idx + batch_size]
        batch_num = batch_idx // batch_size + 1
        print(f"  LLM batch {batch_num}/{total_batches} ({len(batch)} pairs)...")

        prompt = SCREENING_PROMPT
        for i, (a, b) in enumerate(batch, 1):
            prompt += format_pair_for_llm(i, a, b)

        try:
            text = call_fn(prompt, model)
            log.debug("Batch %d raw response:\n%s", batch_num, text)
            batch_results = _extract_json(text)
            for r in batch_results:
                if r.get("confidence") != "none" and r.get("subset_ticker") and r.get("superset_ticker"):
                    log.info("ACCEPTED: %s -> %s [%s] %s",
                             r.get("subset_ticker"), r.get("superset_ticker"),
                             r.get("confidence"), r.get("reasoning", ""))
                    results.append(r)
                else:
                    log.info("REJECTED: %s -> %s [%s] %s",
                             r.get("subset_ticker"), r.get("superset_ticker"),
                             r.get("confidence"), r.get("reasoning", ""))
        except (json.JSONDecodeError, requests.RequestException, KeyError) as e:
            log.warning("Batch %d failed: %s", batch_num, e)
            print(f"    Warning: batch {batch_num} failed: {e}")
            continue

        if provider == "anthropic":
            time.sleep(0.5)

    return results


# ── Enrichment ───────────────────────────────────────────────────────────────


def enrich_with_prices(results: list[dict], markets_by_ticker: dict[str, dict]) -> list[dict]:
    """Add current ask prices and compute top-of-book arb cost for each pair."""
    enriched = []
    for r in results:
        subset_ticker = r.get("subset_ticker")
        superset_ticker = r.get("superset_ticker")
        if not subset_ticker or not superset_ticker:
            enriched.append(r)
            continue

        sub_market = markets_by_ticker.get(subset_ticker, {})
        sup_market = markets_by_ticker.get(superset_ticker, {})

        # Always: buy NO on subset, YES on superset
        sub_ask = sub_market.get("no_ask_dollars")
        sup_ask = sup_market.get("yes_ask_dollars")

        if sub_ask is not None and sup_ask is not None:
            sub_ask = float(sub_ask)
            sup_ask = float(sup_ask)
            arb_cost = sub_ask + sup_ask
            r["subset_ask"] = sub_ask
            r["superset_ask"] = sup_ask
            r["arb_cost"] = round(arb_cost, 4)
        else:
            r["subset_ask"] = sub_ask
            r["superset_ask"] = sup_ask
            r["arb_cost"] = None

        r["subset_title"] = sub_market.get("title", "")
        r["superset_title"] = sup_market.get("title", "")
        exp = sub_market.get("expected_expiration_time", "")
        r["payoff_date"] = exp[:10] if exp else None
        enriched.append(r)

    return enriched


# ── Output ───────────────────────────────────────────────────────────────────


CSV_COLUMNS = [
    "confidence", "arb_cost",
    "subset_ticker", "subset_title", "subset_ask",
    "superset_ticker", "superset_title", "superset_ask",
    "payoff_date", "reasoning",
]


def write_results(results: list[dict], path: str) -> None:
    """Write scan results to a JSON file."""
    with open(path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults written to {path}")


def write_csv(results: list[dict], path: str) -> None:
    """Write scan results to a CSV file."""
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for r in sorted(results, key=lambda x: x.get("arb_cost") or 999):
            writer.writerow(r)
    print(f"CSV written to {path}")


def print_summary(results: list[dict]) -> None:
    """Print a terminal summary of scan results."""
    if not results:
        print("\nNo subset relationships found.")
        return

    confirmed = [r for r in results if r.get("confidence") in ("high", "medium")]
    uncertain = [r for r in results if r.get("confidence") == "low"]

    print(f"\n{'='*80}")
    print(f"SCAN RESULTS: {len(confirmed)} confirmed, {len(uncertain)} uncertain")
    print(f"{'='*80}")

    for label, group in [("CONFIRMED", confirmed), ("UNCERTAIN", uncertain)]:
        if not group:
            continue
        print(f"\n  {label}:")
        print(f"  {'─'*76}")
        for r in sorted(group, key=lambda x: x.get("arb_cost") or 999):
            sub = r.get("subset_ticker", "?")
            sup = r.get("superset_ticker", "?")
            sub_title = r.get("subset_title", "")
            sup_title = r.get("superset_title", "")
            cost = r.get("arb_cost")
            conf = r.get("confidence", "?")
            date = r.get("payoff_date", "?")
            reasoning = r.get("reasoning", "")

            cost_str = f"${cost:.4f}" if cost is not None else "N/A"
            arb_flag = " << ARB" if cost is not None and cost < 1.0 else ""

            print(f"\n    Subset:   {sub:<30} {sub_title}")
            print(f"    Superset: {sup:<30} {sup_title}")
            print(f"    Buy: no subset + yes superset")
            print(f"    Cost: {cost_str}  (need < $1.00){arb_flag}")
            print(f"    Payoff date: {date}  |  Confidence: {conf}")
            print(f"    Reasoning: {reasoning[:120]}")


CACHE_FILE = "market_cache.json"


def save_cache(markets: list[dict], pairs: list[tuple[dict, dict]], path: str = CACHE_FILE) -> None:
    """Save fetched markets and candidate pairs to disk."""
    data = {
        "markets": markets,
        "pairs": [{"a": a, "b": b} for a, b in pairs],
    }
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"Cache written to {path} ({len(markets)} markets, {len(pairs)} pairs)")


def load_cache(path: str = CACHE_FILE) -> tuple[list[dict], list[tuple[dict, dict]]]:
    """Load markets and candidate pairs from disk cache."""
    with open(path) as f:
        data = json.load(f)
    markets = data["markets"]
    pairs = [(p["a"], p["b"]) for p in data["pairs"]]
    print(f"Loaded cache from {path} ({len(markets)} markets, {len(pairs)} pairs)")
    return markets, pairs


# ── Main ─────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scan Kalshi markets for subset arbitrage opportunities"
    )
    # Fetch options
    parser.add_argument(
        "--category", default="Sports",
        help="Kalshi category to scan (default: Sports)",
    )
    parser.add_argument(
        "--filter", "-f", default=None, metavar="KEYWORD",
        help="filter series by keyword (e.g. 'tennis,atp,grand slam')",
    )
    parser.add_argument(
        "--min-volume", type=int, default=0,
        help="exclude markets below this volume (default: 0)",
    )
    # Cache options
    parser.add_argument(
        "--fetch-only", action="store_true",
        help="fetch markets and save cache, skip LLM screening",
    )
    parser.add_argument(
        "--from-cache", action="store_true",
        help="load markets from cache instead of fetching",
    )
    parser.add_argument(
        "--cache-file", default=CACHE_FILE,
        help=f"cache file path (default: {CACHE_FILE})",
    )
    # LLM options
    parser.add_argument(
        "--provider", default="ollama", choices=["ollama", "anthropic"],
        help="LLM provider (default: ollama)",
    )
    parser.add_argument(
        "--model", default=None,
        help="model name (default: qwen3-coder:30b for ollama, claude-haiku-4-5-20251001 for anthropic)",
    )
    parser.add_argument(
        "--batch-size", type=int, default=None,
        help="pairs per LLM batch (default: 12 for anthropic, 1 for ollama)",
    )
    # Output options
    parser.add_argument(
        "--output", "-o", default="scan_results.json",
        help="output JSON file path (default: scan_results.json)",
    )
    parser.add_argument(
        "--csv", default=None, metavar="PATH",
        help="also write results to a CSV file",
    )
    parser.add_argument(
        "--log-file", default="scan.log",
        help="log file path (default: scan.log)",
    )
    args = parser.parse_args()

    # ── Logging setup ─────────────────────────────────────────────────────
    logging.basicConfig(
        filename=args.log_file,
        filemode="w",
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    # ── Fetch or load cache ──────────────────────────────────────────────
    if args.from_cache:
        markets, pairs = load_cache(args.cache_file)
    else:
        markets = fetch_markets(args.category, args.filter, args.min_volume)
        if not markets:
            print("No open markets found.")
            sys.exit(0)

        print("\nGrouping markets by entity...")
        groups = group_by_entity(markets)
        print(f"  Entities in 2+ series: {len(groups)}")

        if not groups:
            print("No cross-series entities found. Nothing to scan.")
            sys.exit(0)

        pairs = generate_candidate_pairs(groups)
        print(f"  Cross-series candidate pairs: {len(pairs)}")

        # Always save cache after fetching
        save_cache(markets, pairs, args.cache_file)

        if args.fetch_only:
            return

    if not pairs:
        print("No candidate pairs to screen.")
        sys.exit(0)

    # ── LLM screening ────────────────────────────────────────────────────
    model = args.model or (
        "qwen3-coder:30b" if args.provider == "ollama"
        else "claude-haiku-4-5-20251001"
    )
    batch_size = args.batch_size or (12 if args.provider == "anthropic" else 1)
    print(f"\nScreening {len(pairs)} pairs with {args.provider}/{model} (batch_size={batch_size})...")
    results = screen_pairs_with_llm(pairs, args.provider, model, batch_size)
    print(f"  Pairs with implication: {len(results)}")

    # ── Enrich with prices ───────────────────────────────────────────────
    print("\nEnriching with current prices...")
    markets_by_ticker = {m["ticker"]: m for m in markets}
    results = enrich_with_prices(results, markets_by_ticker)

    # ── Output ───────────────────────────────────────────────────────────
    write_results(results, args.output)
    if args.csv:
        write_csv(results, args.csv)
    print_summary(results)


if __name__ == "__main__":
    main()
