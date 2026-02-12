#!/usr/bin/env python3
"""Kalshi cross-market arbitrage scanner.

Scans Kalshi sports markets to discover pairs where one contract resolving YES
logically guarantees another also resolves YES (e.g., winning the French Open
implies winning a Grand Slam). Uses programmatic pre-filtering to narrow
candidates, then Claude Haiku for implication checking.

Results are persisted to a SQLite database for incremental scanning and human
review via the companion Flask webapp (app.py).
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

import db as db_mod
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
    markets can't form implication relationships.
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
    implication relationships.
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
You are analyzing Kalshi prediction market contracts to find implication relationships.

An implication relationship exists when one contract resolving YES **logically guarantees** another also resolves YES. For example:
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
    "antecedent_ticker": "..." or null,
    "consequent_ticker": "..." or null,
    "confidence": "high" | "medium" | "low" | "none",
    "reasoning": "short explanation"
  }
]}

"confidence" of "none" means no implication relationship exists — set antecedent_ticker and consequent_ticker to null.
"antecedent_ticker" is the MORE SPECIFIC market (e.g., "wins French Open"), "consequent_ticker" is the BROADER market (e.g., "wins a Grand Slam").

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
        if "antecedent_ticker" in parsed:
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
    ALL results (including confidence="none") so they can be stored in the DB
    to avoid re-screening. Each result dict includes ticker_a/ticker_b from
    the input pair.
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
            for i, r in enumerate(batch_results):
                # Attach input pair tickers so DB can store the canonical pair
                if i < len(batch):
                    r["ticker_a"] = batch[i][0]["ticker"]
                    r["ticker_b"] = batch[i][1]["ticker"]

                if r.get("confidence") != "none" and r.get("antecedent_ticker") and r.get("consequent_ticker"):
                    log.info("ACCEPTED: %s -> %s [%s] %s",
                             r.get("antecedent_ticker"), r.get("consequent_ticker"),
                             r.get("confidence"), r.get("reasoning", ""))
                else:
                    log.info("REJECTED: %s -> %s [%s] %s",
                             r.get("antecedent_ticker"), r.get("consequent_ticker"),
                             r.get("confidence"), r.get("reasoning", ""))
                results.append(r)
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
        antecedent_ticker = r.get("antecedent_ticker")
        consequent_ticker = r.get("consequent_ticker")
        if not antecedent_ticker or not consequent_ticker:
            enriched.append(r)
            continue

        ant_market = markets_by_ticker.get(antecedent_ticker, {})
        con_market = markets_by_ticker.get(consequent_ticker, {})

        # Always: buy NO on antecedent, YES on consequent
        ant_ask = ant_market.get("no_ask_dollars")
        con_ask = con_market.get("yes_ask_dollars")

        if ant_ask is not None and con_ask is not None:
            ant_ask = float(ant_ask)
            con_ask = float(con_ask)
            arb_cost = ant_ask + con_ask
            r["antecedent_ask"] = ant_ask
            r["consequent_ask"] = con_ask
            r["arb_cost"] = round(arb_cost, 4)
        else:
            r["antecedent_ask"] = ant_ask
            r["consequent_ask"] = con_ask
            r["arb_cost"] = None

        r["antecedent_title"] = ant_market.get("title", "")
        r["consequent_title"] = con_market.get("title", "")
        exp = ant_market.get("expected_expiration_time", "")
        r["payoff_date"] = exp[:10] if exp else None
        enriched.append(r)

    return enriched


# ── Output ───────────────────────────────────────────────────────────────────


CSV_COLUMNS = [
    "confidence", "arb_cost",
    "antecedent_ticker", "antecedent_title", "antecedent_ask",
    "consequent_ticker", "consequent_title", "consequent_ask",
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
        print("\nNo implication relationships found.")
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
            ant = r.get("antecedent_ticker", "?")
            con = r.get("consequent_ticker", "?")
            ant_title = r.get("antecedent_title", "")
            con_title = r.get("consequent_title", "")
            cost = r.get("arb_cost")
            conf = r.get("confidence", "?")
            date = r.get("payoff_date", "?")
            reasoning = r.get("reasoning", "")

            cost_str = f"${cost:.4f}" if cost is not None else "N/A"
            arb_flag = " << ARB" if cost is not None and cost < 1.0 else ""

            print(f"\n    Antecedent: {ant:<30} {ant_title}")
            print(f"    Consequent: {con:<30} {con_title}")
            print(f"    Buy: no antecedent + yes consequent")
            print(f"    Cost: {cost_str}  (need < $1.00){arb_flag}")
            print(f"    Payoff date: {date}  |  Confidence: {conf}")
            print(f"    Reasoning: {reasoning[:120]}")


# ── Main ─────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scan Kalshi markets for arbitrage opportunities"
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
    # DB options
    parser.add_argument(
        "--db", default="kalshi_arb.db",
        help="SQLite database path (default: kalshi_arb.db)",
    )
    parser.add_argument(
        "--from-db", action="store_true",
        help="skip fetching, use tickers already in DB",
    )
    parser.add_argument(
        "--rescan", action="store_true",
        help="re-screen all pairs even if already in DB",
    )
    parser.add_argument(
        "--max-pairs", type=int, default=None,
        help="max number of new pairs to screen per run (caps LLM calls)",
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

    # ── Database setup ────────────────────────────────────────────────────
    conn = db_mod.get_connection(args.db)

    # ── Fetch or use DB ──────────────────────────────────────────────────
    if not args.from_db:
        markets = fetch_markets(args.category, args.filter, args.min_volume)
        if not markets:
            print("No open markets found.")
            sys.exit(0)

        new, updated = db_mod.upsert_tickers(conn, markets)
        recorded = db_mod.record_prices(conn, markets)
        print(f"  DB: {new} new tickers, {updated} updated, {recorded} price snapshots")

    # ── Generate candidates from DB ──────────────────────────────────────
    print("\nGrouping markets by entity (from DB)...")
    groups = db_mod.get_tickers_by_entity(conn)
    print(f"  Entities in 2+ series: {len(groups)}")

    if not groups:
        print("No cross-series entities found. Nothing to scan.")
        sys.exit(0)

    all_pairs = generate_candidate_pairs(groups)
    print(f"  Cross-series candidate pairs: {len(all_pairs)}")

    # ── Filter to unscreened pairs ───────────────────────────────────────
    if not args.rescan:
        screened = db_mod.get_screened_pair_keys(conn)
        pairs = [p for p in all_pairs if db_mod.sorted_key(p) not in screened]
        skipped = len(all_pairs) - len(pairs)
        if skipped:
            print(f"  Skipping {skipped} already-screened pairs")
    else:
        pairs = all_pairs

    if not pairs:
        print("No new pairs to screen.")
        sys.exit(0)

    if args.max_pairs and len(pairs) > args.max_pairs:
        print(f"  Capping to {args.max_pairs} pairs (--max-pairs)")
        pairs = pairs[:args.max_pairs]

    # ── LLM screening ────────────────────────────────────────────────────
    model = args.model or (
        "qwen3-coder:30b" if args.provider == "ollama"
        else "claude-haiku-4-5-20251001"
    )
    batch_size = args.batch_size or (12 if args.provider == "anthropic" else 1)
    print(f"\nScreening {len(pairs)} pairs with {args.provider}/{model} (batch_size={batch_size})...")
    all_results = screen_pairs_with_llm(pairs, args.provider, model, batch_size)

    # ── Store all results in DB ──────────────────────────────────────────
    stored = db_mod.bulk_upsert_pair_results(conn, all_results, model)
    print(f"  DB: {stored} pair results stored")

    # ── Filter to confirmed for output ───────────────────────────────────
    confirmed = [r for r in all_results if r.get("confidence") != "none" and r.get("antecedent_ticker") and r.get("consequent_ticker")]
    print(f"  Pairs with implication: {len(confirmed)}")

    # ── Enrich with prices ───────────────────────────────────────────────
    print("\nEnriching with current prices...")
    # Build ticker lookup from DB
    all_tickers = conn.execute("SELECT * FROM tickers").fetchall()
    markets_by_ticker = {dict(r)["ticker"]: dict(r) for r in all_tickers}
    confirmed = enrich_with_prices(confirmed, markets_by_ticker)

    # ── Output ───────────────────────────────────────────────────────────
    write_results(confirmed, args.output)
    if args.csv:
        write_csv(confirmed, args.csv)
    print_summary(confirmed)

    conn.close()


if __name__ == "__main__":
    main()
