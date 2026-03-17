#!/usr/bin/env python3
"""Kalshi cross-market arbitrage scanner.

Scans Kalshi sports markets to discover pairs where one contract resolving YES
logically guarantees another also resolves YES (e.g., winning the French Open
implies winning a Grand Slam). Uses programmatic pre-filtering to narrow
candidates, then Claude Sonnet for implication checking.

Results are persisted to a SQLite database for incremental scanning and human
review via the companion Flask webapp (app.py).
"""

import argparse
import json
import logging
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


def fetch_series(category: str, filter_tags: list[str] | None = None) -> list[dict]:
    """Fetch series for a category, optionally filtering by Kalshi API tags.

    When filter_tags is provided, makes a separate paginated API call per tag
    and merges results (deduped by series ticker).
    """
    tags_to_query = filter_tags or [None]
    seen: set[str] = set()
    series: list[dict] = []
    for tag in tags_to_query:
        cursor = None
        while True:
            params = {"limit": 200, "category": category}
            if tag:
                params["tags"] = tag
            if cursor:
                params["cursor"] = cursor
            resp = requests.get(f"{KALSHI_BASE}/series", params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            batch = data.get("series", [])
            for s in batch:
                if s["ticker"] not in seen:
                    seen.add(s["ticker"])
                    series.append(s)
            cursor = data.get("cursor")
            if not cursor or not batch:
                break
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


def fetch_and_store_markets(category: str, conn, filter_tags: list[str] | None = None) -> set[str]:
    """Fetch open markets for a category and upsert into DB incrementally.

    Upserts each series' markets immediately so we never hold all 17K+
    markets in memory at once. Returns the set of active ticker strings
    (for deactivation tracking).
    """
    print(f"Fetching {category} series...", flush=True)
    all_series = fetch_series(category, filter_tags)
    print(f"  {len(all_series)} series")

    active_tickers: set[str] = set()
    total_markets = 0
    new_total = 0
    updated_total = 0
    recorded_total = 0
    for i, s in enumerate(all_series):
        sticker = s["ticker"]
        tags = s.get("tags", [])
        sport_tag = tags[0] if tags else ""
        for attempt in range(3):
            try:
                events = fetch_events_with_markets(sticker)
                break
            except (requests.HTTPError, requests.ConnectionError, requests.Timeout) as e:
                if attempt < 2 and ("429" in str(e) or "timed out" in str(e).lower() or "No route" in str(e)):
                    time.sleep(2 ** attempt)  # 1s, 2s backoff
                    continue
                log.warning("Failed to fetch events for %s: %s", sticker, e)
                print(f"  Warning: failed to fetch events for {sticker}: {e}")
                events = []
                break
        time.sleep(0.2)  # rate limit between series
        batch = []
        for event in events:
            for m in event.get("markets", []):
                if m.get("status") not in ("open", "active"):
                    continue
                vol = int(float(m.get("volume") or m.get("volume_fp") or 0))
                batch.append({
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
                    "sport_tag": sport_tag,
                })
        if batch:
            new, updated = db_mod.upsert_tickers(conn, batch)
            recorded = db_mod.record_prices(conn, batch)
            new_total += new
            updated_total += updated
            recorded_total += recorded
            active_tickers.update(m["ticker"] for m in batch)
            total_markets += len(batch)
        if (i + 1) % 10 == 0 or i + 1 == len(all_series):
            print(f"  Processed {i + 1}/{len(all_series)} series ({total_markets} markets)", flush=True)

    print(f"  Total open markets: {total_markets}")
    print(f"  DB: {new_total} new tickers, {updated_total} updated, {recorded_total} price snapshots")
    return active_tickers


# ── Pre-filtering ────────────────────────────────────────────────────────────


ENTITY_BLOCKLIST = {
    "Tie", "Yes",
    "Before 2025", "Before 2026", "Before 2027", "Before 2028",
    "Before 2029", "Before 2030", "Before 2031", "Before 2032",
    "Before 2033", "Before 2034", "Before 2035",
}


def generate_candidate_pairs(groups: dict[str, list[dict]]) -> list[tuple[dict, dict]]:
    """Generate cross-series candidate pairs for each entity.

    Only pairs markets from different series — same-series pairs are never
    implication relationships. Skips pairs where both markets have a known
    sport and the sports differ (cross-sport noise). Also skips entities
    in the ENTITY_BLOCKLIST that never produce real implications.
    """
    pairs = []
    filtered_count = 0
    blocklist_count = 0
    for entity, entity_markets in groups.items():
        if entity in ENTITY_BLOCKLIST:
            blocklist_count += len(list(combinations(entity_markets, 2)))
            continue
        for a, b in combinations(entity_markets, 2):
            if a["series_ticker"] != b["series_ticker"] and a["event_ticker"] != b["event_ticker"]:
                sport_a = a.get("sport_tag") or None
                sport_b = b.get("sport_tag") or None
                if sport_a and sport_b and sport_a != sport_b:
                    filtered_count += 1
                    continue
                pairs.append((a, b))
    if blocklist_count:
        log.info("Skipped %d pairs from blocklisted entities", blocklist_count)
        print(f"  Skipped {blocklist_count} pairs from blocklisted entities")
    if filtered_count:
        log.info("Filtered %d cross-sport pairs", filtered_count)
        print(f"  Filtered {filtered_count} cross-sport pairs")
    return pairs


# ── LLM screening ───────────────────────────────────────────────────────────

SCREENING_PROMPT = """\
For each pair of events below, determine if one event logically NECESSITATES the other. Check both directions.

"A implies B" means: if A resolves YES, then B MUST ALSO resolve YES as a matter of logical necessity — not probability, correlation, or likelihood. The implication must hold in every possible scenario.

Examples of TRUE implications:
- "Player X wins the French Open" → "Player X wins a Grand Slam" (the French Open IS a Grand Slam)
- "Team Y wins the Super Bowl" → "Team Y wins the AFC/NFC Championship" (must win conference to reach Super Bowl)
- "Team Y wins the Super Bowl" → "Team Y makes the playoffs" (must make playoffs to win)

Note: the implication can go in EITHER direction. "Player X wins a tennis major" does NOT imply "Player X wins the French Open" — could win a different major. But reversed: "Player X wins the French Open" DOES imply "Player X wins a major". Always check BOTH directions.

Examples of FALSE implications:
- "Team Y wins the AFC Championship" → "Team Y wins the Super Bowl" ✗ (they could LOSE the Super Bowl)
- "Team Y wins their division" → "Team Y wins the championship" ✗ (could lose in playoffs)
- "Team Y wins the championship" → "Team Y wins their division" ✗ (wild card teams exist)
- "Player X leads in stats" → "Player X wins MVP" ✗ (correlation, not necessity)

Return a JSON object (no markdown fencing) with a "results" key containing one object per pair:
{"results": [
  {
    "ticker_a": "Event A ticker (copied exactly from input)",
    "ticker_b": "Event B ticker (copied exactly from input)",
    "antecedent_ticker": "..." or null,
    "consequent_ticker": "..." or null,
    "confidence": "high" | "medium" | "low" | "none" | "need_more_info",
    "reasoning": "short explanation"
  }
]}

IMPORTANT: "ticker_a" and "ticker_b" MUST be copied exactly from the Event A and Event B tickers shown in each pair.

Confidence levels:
- "high": the implication is a logical certainty based on the rules of the sport/competition
- "medium": the implication is very likely logically necessary but depends on specific rule interpretations
- "low": there may be an implication but it's unclear — err toward inclusion
- "need_more_info": the events MIGHT be logically dependent but you cannot determine the relationship without additional context (e.g., depends on tournament brackets, contingencies, or ambiguous rules)
- "none": no implication in either direction — set antecedent_ticker and consequent_ticker to null

"antecedent_ticker" is the event that IMPLIES the other (if this is YES, the other MUST be YES).
"consequent_ticker" is the event that is IMPLIED (necessarily YES when the antecedent is YES).

CANDIDATE PAIRS:
"""


def format_pair_for_llm(idx: int, a: dict, b: dict) -> str:
    """Format a candidate pair for the LLM prompt."""
    return (
        f"\n--- Pair {idx} ---\n"
        f"Event A:\n"
        f"  ticker: {a['ticker']}\n"
        f"  title: {a['title']}\n"
        f"  rules: {a['rules_primary'][:500]}\n"
        f"\n"
        f"Event B:\n"
        f"  ticker: {b['ticker']}\n"
        f"  title: {b['title']}\n"
        f"  rules: {b['rules_primary'][:500]}\n"
    )


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
    """Extract a JSON array from LLM output, stripping markdown fencing."""
    text = text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1]
        if text.endswith("```"):
            text = text[: text.rfind("```")]
        text = text.strip()
    parsed = json.loads(text)
    if isinstance(parsed, dict):
        for key in ("results", "pairs", "data"):
            if isinstance(parsed.get(key), list):
                return parsed[key]
        if "antecedent_ticker" in parsed:
            return [parsed]
    return parsed


def screen_pairs_with_llm(
    pairs: list[tuple[dict, dict]],
    model: str,
    batch_size: int = 12,
    conn: "sqlite3.Connection | None" = None,
) -> list[dict]:
    """Screen candidate pairs using Claude for implication checking.

    Batches pairs and returns ALL results (including confidence="none") so
    they can be stored in the DB to avoid re-screening. Each result dict
    includes ticker_a/ticker_b from the input pair.

    If conn is provided, writes results to the DB after each batch.
    """
    results = []
    total_batches = (len(pairs) + batch_size - 1) // batch_size

    for batch_idx in range(0, len(pairs), batch_size):
        batch = pairs[batch_idx : batch_idx + batch_size]
        batch_num = batch_idx // batch_size + 1
        print(f"  LLM batch {batch_num}/{total_batches} ({len(batch)} pairs)...", flush=True)

        prompt = SCREENING_PROMPT
        for i, (a, b) in enumerate(batch, 1):
            prompt += format_pair_for_llm(i, a, b)

        results_start = len(results)
        try:
            text = _call_anthropic(prompt, model)
            log.debug("Batch %d raw response:\n%s", batch_num, text)
            batch_results = _extract_json(text)

            if len(batch_results) != len(batch):
                log.warning("Batch %d: expected %d results, got %d",
                            batch_num, len(batch), len(batch_results))

            # Build lookup from input pairs for ticker-based matching
            batch_lookup = {(a["ticker"], b["ticker"]): (a, b) for a, b in batch}
            matched_keys: set[tuple[str, str]] = set()
            accepted, rejected, unmatched_count = 0, 0, 0

            for r in batch_results:
                ra = r.get("ticker_a", "")
                rb = r.get("ticker_b", "")

                # Try direct match, then reversed order
                key = None
                if (ra, rb) in batch_lookup:
                    key = (ra, rb)
                elif (rb, ra) in batch_lookup:
                    key = (rb, ra)
                else:
                    # Fallback for non-"none" results: match via antecedent/consequent tickers
                    ant = r.get("antecedent_ticker")
                    con = r.get("consequent_ticker")
                    if ant and con:
                        result_tickers = {ant, con}
                        for bk in batch_lookup:
                            if bk not in matched_keys and {bk[0], bk[1]} == result_tickers:
                                key = bk
                                break

                if key is None:
                    log.warning("Batch %d: unmatched LLM result ticker_a=%s ticker_b=%s ant=%s con=%s",
                                batch_num, ra, rb,
                                r.get("antecedent_ticker"), r.get("consequent_ticker"))
                    unmatched_count += 1
                    continue

                matched_keys.add(key)
                # Set canonical input pair tickers
                r["ticker_a"] = key[0]
                r["ticker_b"] = key[1]

                conf = r.get("confidence")
                if conf not in ("none", "need_more_info") and r.get("antecedent_ticker") and r.get("consequent_ticker"):
                    log.info("ACCEPTED: %s -> %s [%s] %s",
                             r.get("antecedent_ticker"), r.get("consequent_ticker"),
                             conf, r.get("reasoning", ""))
                    accepted += 1
                elif conf == "need_more_info":
                    log.info("NEED_INFO: %s / %s [%s] %s",
                             r.get("ticker_a"), r.get("ticker_b"),
                             conf, r.get("reasoning", ""))
                    rejected += 1
                else:
                    log.info("REJECTED: %s -> %s [%s] %s",
                             r.get("antecedent_ticker"), r.get("consequent_ticker"),
                             conf, r.get("reasoning", ""))
                    rejected += 1
                results.append(r)

            # Warn about input pairs with no LLM result
            unresulted = set(batch_lookup.keys()) - matched_keys
            for uk in unresulted:
                log.warning("Batch %d: no LLM result for input pair %s / %s",
                            batch_num, uk[0], uk[1])

            log.info("Batch %d summary: %d accepted, %d rejected, %d unmatched, %d missing",
                     batch_num, accepted, rejected, unmatched_count, len(unresulted))

            # Write this batch's results to DB immediately
            if conn is not None:
                batch_stored = results[results_start:]
                db_mod.bulk_upsert_pair_results(conn, batch_stored, model)
        except (json.JSONDecodeError, requests.RequestException, KeyError) as e:
            log.warning("Batch %d failed: %s", batch_num, e)
            print(f"    Warning: batch {batch_num} failed: {e}")
            continue

        time.sleep(0.5)

    return results


# ── Output ───────────────────────────────────────────────────────────────────


def print_summary(results: list[dict]) -> None:
    """Print a terminal summary of scan results."""
    if not results:
        print("\nNo implication relationships found.")
        return

    confirmed = [r for r in results if r.get("confidence") in ("high", "medium")]
    uncertain = [r for r in results if r.get("confidence") == "low"]
    need_info = [r for r in results if r.get("confidence") == "need_more_info"]

    print(f"\n{'='*80}")
    print(f"SCAN RESULTS: {len(confirmed)} confirmed, {len(uncertain)} uncertain, {len(need_info)} need info")
    print(f"{'='*80}")

    for label, group in [("CONFIRMED", confirmed), ("UNCERTAIN", uncertain), ("NEED MORE INFO", need_info)]:
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
        "--filter", "-f", default=None, metavar="TAG",
        help="filter series by Kalshi API tag (e.g. 'tennis' or 'Tennis,Soccer')",
    )
    parser.add_argument(
        "--min-volume", type=int, default=200,
        help="exclude markets below this volume (default: 200)",
    )
    # DB options
    parser.add_argument(
        "--db", default="slonk_arb.db",
        help="SQLite database path (default: slonk_arb.db)",
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
        "--model", default="claude-sonnet-4-6",
        help="Anthropic model name (default: claude-sonnet-4-6)",
    )
    parser.add_argument(
        "--batch-size", type=int, default=12,
        help="pairs per LLM batch (default: 12)",
    )
    # Output options
    parser.add_argument(
        "--log-file", default="scan.log",
        help="log file path (default: scan.log)",
    )
    args = parser.parse_args()

    # ── Logging setup ─────────────────────────────────────────────────────
    handler = logging.FileHandler(args.log_file, mode="a")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S"))
    logging.basicConfig(level=logging.DEBUG, handlers=[handler])
    log.info("=== scan.py started: %s ===", " ".join(sys.argv[1:]))

    # ── Database setup ────────────────────────────────────────────────────
    conn = db_mod.get_connection(args.db)

    # ── Fetch or use DB ──────────────────────────────────────────────────
    if not args.from_db:
        t0 = time.time()
        filter_tags = [t.strip().title() for t in args.filter.split(",")] if args.filter else None
        active_tickers = fetch_and_store_markets(args.category, conn, filter_tags=filter_tags)
        if not active_tickers:
            print("No open markets found.")
            sys.exit(0)

        # Only deactivate when we fetched ALL tickers (no filter), otherwise
        # we'd wrongly deactivate tickers outside the filter.
        if not args.filter:
            deactivated = db_mod.deactivate_missing_tickers(conn, active_tickers)
            print(f"  {deactivated} tickers deactivated")
        print(f"  Fetch completed in {time.time() - t0:.0f}s", flush=True)

    # ── Generate candidates from DB ──────────────────────────────────────
    print("\nGrouping markets by entity (from DB)...")
    groups = db_mod.get_tickers_by_entity(conn, min_volume=args.min_volume)

    # Apply --filter to restrict which entity groups go to LLM screening
    if args.filter:
        filter_tags_lower = [t.strip().lower() for t in args.filter.split(",")]
        filtered_groups = {}
        for entity, entity_markets in groups.items():
            if any(
                m.get("sport_tag", "").lower() in filter_tags_lower
                for m in entity_markets
            ):
                filtered_groups[entity] = entity_markets
        skipped_entities = len(groups) - len(filtered_groups)
        groups = filtered_groups
        if skipped_entities:
            print(f"  Filter '{args.filter}': kept {len(groups)} entities, skipped {skipped_entities}")
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

    if args.max_pairs is not None:
        if args.max_pairs == 0:
            print("--max-pairs 0: skipping LLM screening.")
            print_summary([])
            conn.close()
            return
        if len(pairs) > args.max_pairs:
            print(f"  Capping to {args.max_pairs} pairs (--max-pairs)")
            pairs = pairs[:args.max_pairs]

    # ── LLM screening ────────────────────────────────────────────────────
    model = args.model
    batch_size = args.batch_size
    print(f"\nScreening {len(pairs)} pairs with {model} (batch_size={batch_size})...", flush=True)
    t0 = time.time()
    all_results = screen_pairs_with_llm(pairs, model, batch_size, conn=conn)
    print(f"  DB: {len(all_results)} pair results stored (incremental)")
    print(f"  LLM screening completed in {time.time() - t0:.0f}s", flush=True)

    # ── Filter to confirmed for output ───────────────────────────────────
    confirmed = [r for r in all_results if r.get("confidence") not in ("none", "need_more_info") and r.get("antecedent_ticker") and r.get("consequent_ticker")]
    print(f"  Pairs with implication: {len(confirmed)}")

    # ── Output ───────────────────────────────────────────────────────────
    print_summary(confirmed)

    conn.close()


if __name__ == "__main__":
    main()
