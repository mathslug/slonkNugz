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
                log.warning("Failed to fetch events for %s: %s", sticker, e)
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


SPORT_FAMILIES = {
    # ── Racket sports ─────────────────────────────────────────────────
    "tennis": [
        "KXATP", "KXWTA",
        "KXFOMEN", "KXFOWOMEN", "KXFOPEN",             # French Open
        "KXGRANDSLAM",
        "KXAOMEN", "KXAOWOMEN",                         # Australian Open
        "KXUSOMEN", "KXUSOPEN", "KXUSOWOMEN",           # US Open (KXUSOPENCUP → soccer via longer match)
        "KXWMENSINGLES", "KXWWOMENSINGLES",              # Wimbledon
        "KXDAVISCUP", "KXUNITEDCUP", "KXLAVERCUP",      # team events
        "KXTENNIS", "KXEXHIBITION", "KXBATTLEOFSEXES", "KXSIXKINGS",
        "KXDDF", "KXCHALLENGERMATCH",
    ],
    "table_tennis": ["KXTABLETENNIS", "KXTTELITE"],

    # ── American pro team sports ──────────────────────────────────────
    "mlb": [
        "KXMLB", "KXLEADERMLB",
        "KXNEXTTEAMMLB", "KXCITYMLBEXPAND",
        "KXTEAMSINWS", "KXWSAL", "KXWSNL",
        "KXNLGAME", "KXNLMVP", "KXALMVP",
    ],
    "nba": [
        "KXNBA", "KXRECORDNBA", "KXLEADERNBA",
        "KXCITYNBAEXPAND", "KXNEXTTEAMNBA",
        "KXCOACHOUTNBA", "KXNEXTCOACHOUTNBA",
        "KXTRADEOFFNBA", "KXTEAMSINNBAF",
        "KXMVENBA",
        "KXFIRSTPICKNBA", "KXTOP3NBADRAFT", "KXPLAYEROPTIONNBA",
        "KXALLSTARROSTER",
    ],
    "wnba": ["KXWNBA"],
    "nfl": [
        "KXNFL", "KXSB", "KXRECORDNFL", "KXLEADERNFL",
        "KXSUPERBOWL",                                  # NOT KXSUP (collides with KXSUPERLIG soccer)
        "KXNEXTTEAMNFL", "KXCOACHOUTNFL", "KXNEXTCOACHOUTNFL", "KXNEXTNFLCOACH",
        "KXSTARTINGQB",
        "KXTRADEOFFNFL", "KXTEAMSINSB",
        "KXMVENFL",
        "KXAFC", "KXNFC",                               # conference champs (KXAFCON/KXAFCCL → soccer via longer match)
    ],
    "nhl": [
        "KXNHL",
        "KXTEAMSINSC", "KXCONNSMYTHE", "KXCANADACUP",
    ],
    "ufl": ["KXUFL"],

    # ── NCAA ──────────────────────────────────────────────────────────
    "ncaa_bball": [
        "KXNCAAMB", "KXNCAAWB", "KXNCAAB",              # men's / women's / conf tournaments
        "KXMARMAD", "KXMAKEMARMAD", "KXWMARMAD",         # March Madness
        "KXBIG12", "KXBIG10", "KXBIGEAST",               # conf reg season
        "KXACC", "KXSEC", "KXWCC", "KXA10", "KXAAC",
        "KXWMA", "KXMWR",
        "KXNCAANIT",
    ],
    "ncaa_football": [
        "KXNCAAF",
        "KXNCAAPLAYOFF",
        "KXCOACHOUTNCAAFB",
        "KXCFB", "KXCFP",                               # college football playoff
        "KXHEISMAN", "KXDEFHEISMAN",
    ],
    "ncaa_baseball": ["KXNCAABASE", "KXNCAAMBACH"],
    "ncaa_hockey":   ["KXNCAAHOCK"],
    "ncaa_lacrosse": ["KXNCAALAX", "KXNCAAMLAX", "KXLAX"],

    # ── Soccer ────────────────────────────────────────────────────────
    "soccer": [
        # England
        "KXEPL", "KXPREMIERLEAGUE",
        "KXEFL", "KXFACUP", "KXEFLCUP",
        "KXEWSL", "KXPFAPOY",
        # Spain
        "KXLALIGA", "KXCOPADELREY", "KXESPSUPERCUP",
        # Germany
        "KXBUNDESLIGA", "KXDFBPOKAL",
        # Italy
        "KXSERIEA", "KXSERIEB", "KXCOPPAITALIA", "KXITASUPERCUP",
        # France
        "KXLIGUE1", "KXCOUPEDEFRANCE", "KXFRASUPERCUP",
        # Netherlands
        "KXEREDIVISIE", "KXKNVBCUP",
        # Portugal
        "KXLIGAPORTUGAL", "KXTACAPORT",
        # Other European
        "KXBEL",                                         # Belgian Pro League
        "KXSUPERLIG",                                    # Turkish Super Lig (NOT KXSUP!)
        "KXDENSUPERLIGA", "KXDANISHSUPERLIGA",
        "KXEKSTRAKLASA",
        "KXSLGREECE", "KXSCOTTISHPREM", "KXSWISSLEAGUE", "KXHNL",
        # Americas
        "KXMLS", "KXNWSL", "KXLIGAMX", "KXBRASILEIRO", "KXARGPREMDIV",
        "KXUSOPENCUP",
        # Asia / Middle East / Africa
        "KXSAUDIPL", "KXJLEAGUE", "KXKLEAGUE", "KXALEAGUE",
        "KXAFCON", "KXAFCCL",                            # longer than KXAFC (→ NFL)
        # European cups
        "KXUCL", "KXUEL", "KXUECL", "KXUEFA", "KXCLUBWC",
        # International
        "KXFIFA", "KXMENWORLDCUP", "KXMWORLDCUP",
        "KXWCGAME", "KXWCGOAL", "KXWCGROUP", "KXWCROUND",
        "KXINTLFRIENDLY",
        # Generic
        "KXSOCCER", "KXLEADERUCLGOALS", "KXBALLONDOR",
    ],

    # ── Combat sports ─────────────────────────────────────────────────
    "boxing":  ["KXBOXING"],
    "ufc":     ["KXUFC"],

    # ── Motorsport ────────────────────────────────────────────────────
    "f1":      ["KXF1"],
    "nascar":  ["KXNASCAR"],
    "indycar": ["KXINDY500"],

    # ── Golf ──────────────────────────────────────────────────────────
    "golf": [
        "KXPGA", "KXTGL", "KXLIV", "KXDPWORLDTOUR",
        "KXMASTERS", "KXTHEOPEN",
        "KXSCOTTIESLAM", "KXHOLEINONE", "KXRYDERCUP",
        "KXGENESISINVITATIONAL", "KXPHOENIXOPEN",
    ],

    # ── Other individual sports ───────────────────────────────────────
    "darts":   ["KXPREMDARTS", "KXDARTSMATCH"],
    "chess": [
        "KXCHESS", "KXFIDE", "KXFCSOUTHAFRICA", "KXWEISSENHAUS",
        "KXNORWAYCHESS", "KXSINQUEFIELD", "KXSPEEDCHESS", "KXSAGRANDSLAM",
    ],
    "cricket": [
        "KXCRICKET", "KXIPL", "KXWPL",
        "KXASIACUPCRICKET", "KXT20", "KXSSHIELD",
    ],
    "rugby":   ["KXRUGBY", "KXSIXNATIONS"],
    "surfing": ["KXSURF", "KXWOTW"],
    "cycling": ["KXTOURDEFRANCE"],
    "pickleball": ["KXPICKLEBALL"],
    "padel":   ["KXPPL"],

    # ── Esports ───────────────────────────────────────────────────────
    "esports": [
        "KXCS2", "KXCSGO", "KXVALORANT", "KXLOL", "KXLEAGUE",
        "KXDOTA2", "KXOVERWATCH", "KXCOD",
        "KXBRAWLSTARS", "KXPUBG", "KXR6", "KXCROSSFIRE",
        "KXEWC",                                         # Esports World Cup
        "KXPGL", "KXSTARLADDER",
        "KXMIDSEASONINVITATIONAL", "KXBLASTRIVALS", "KXBOUNTY",
        "KXVALGC", "KXVALPL", "KXVCCHAMPIONS",
        "KXWORLDSMVP",                                   # must be longer than KXWO (→ winter olympics)
    ],

    # ── International basketball ──────────────────────────────────────
    "basketball_intl": [
        "KXACBGAME", "KXBBLGAME", "KXGBLGAME", "KXNBLGAME",
        "KXARGLNB", "KXCBA",
        "KXEUROCUP", "KXEUROLEAGUE",
        "KXFIBACHAMP", "KXFIBAECUP",
        "KXJBLEAGUE", "KXKBL", "KXBSL", "KXVTB", "KXLNBELITE",
        "KXUNRIVALED", "KXBBSERIEA", "KXABAGAME", "KXTNCBASKETBALL",
    ],

    # ── International hockey ──────────────────────────────────────────
    "hockey_intl": [
        "KXIIHF", "KXDEL", "KXELH", "KXKHL", "KXLIIGA", "KXSHL", "KXAHL",
    ],

    # ── Winter Olympics ───────────────────────────────────────────────
    "winter_olympics": ["KXWO"],

    # ── Other leagues ─────────────────────────────────────────────────
    "cfl":     ["KXGREYCUP"],
    "frisbee": ["KXUFA"],
}

# Reverse lookup: prefix -> sport (longest prefixes first for correct matching)
_PREFIX_TO_SPORT: dict[str, str] = {}
for _sport, _prefixes in SPORT_FAMILIES.items():
    for _prefix in _prefixes:
        _PREFIX_TO_SPORT[_prefix] = _sport
_SORTED_PREFIXES = sorted(_PREFIX_TO_SPORT.keys(), key=len, reverse=True)


def _get_sport(series_ticker: str) -> str | None:
    """Return the sport family for a series ticker, or None if unknown."""
    for prefix in _SORTED_PREFIXES:
        if series_ticker.startswith(prefix):
            return _PREFIX_TO_SPORT[prefix]
    return None


def generate_candidate_pairs(groups: dict[str, list[dict]]) -> list[tuple[dict, dict]]:
    """Generate cross-series candidate pairs for each entity.

    Only pairs markets from different series — same-series pairs are never
    implication relationships. Skips pairs where both markets have a known
    sport and the sports differ (cross-sport noise).
    """
    pairs = []
    filtered_count = 0
    for entity, entity_markets in groups.items():
        for a, b in combinations(entity_markets, 2):
            if a["series_ticker"] != b["series_ticker"]:
                sport_a = _get_sport(a["series_ticker"])
                sport_b = _get_sport(b["series_ticker"])
                if sport_a and sport_b and sport_a != sport_b:
                    filtered_count += 1
                    continue
                pairs.append((a, b))
    if filtered_count:
        log.info("Filtered %d cross-sport pairs", filtered_count)
        print(f"  Filtered {filtered_count} cross-sport pairs")
    return pairs


# ── LLM screening ───────────────────────────────────────────────────────────

SCREENING_PROMPT = """\
/no_think
For each pair of events below, determine if one event logically implies the other. Check both directions.

"A implies B" means: if A happened, B **must** also have happened (or must happen). It is a strict logical guarantee, not just likely or correlated.

Examples:
- "Player X wins the French Open" implies "Player X wins a Grand Slam" ✓ (the French Open IS a Grand Slam)
- "Team Y wins the Super Bowl" implies "Team Y wins the AFC/NFC Championship" ✓ (you must win your conference to reach the Super Bowl)
- "Team Y wins the Super Bowl" implies "Team Y makes the playoffs" ✓ (same logic)
- "Team Y wins the AFC Championship" implies "Team Y wins the Super Bowl" ✗ (they APPEAR in the Super Bowl but could LOSE — appearing != winning)
- "Team Y wins their division" implies "Team Y wins the championship" ✗ (they could lose in the playoffs)
- "Team Y wins the championship" implies "Team Y wins their division" ✗ (wild card teams can win championships without winning their division)

Return a JSON object (no markdown fencing) with a "results" key containing one object per pair:
{"results": [
  {
    "ticker_a": "Event A ticker (copied exactly from input)",
    "ticker_b": "Event B ticker (copied exactly from input)",
    "antecedent_ticker": "..." or null,
    "consequent_ticker": "..." or null,
    "confidence": "high" | "medium" | "low" | "none",
    "reasoning": "short explanation"
  }
]}

IMPORTANT: "ticker_a" and "ticker_b" MUST be copied exactly from the Event A and Event B tickers shown in each pair.

"confidence" of "none" means no implication in either direction — set antecedent_ticker and consequent_ticker to null.
"antecedent_ticker" is the event that implies the other. "consequent_ticker" is the event that is implied.
If uncertain, use "low" confidence rather than "none" — err toward inclusion.

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
        f"  expiration: {a['expected_expiration_time']}\n"
        f"\n"
        f"Event B:\n"
        f"  ticker: {b['ticker']}\n"
        f"  title: {b['title']}\n"
        f"  rules: {b['rules_primary'][:500]}\n"
        f"  expiration: {b['expected_expiration_time']}\n"
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
) -> list[dict]:
    """Screen candidate pairs using Claude for implication checking.

    Batches pairs and returns ALL results (including confidence="none") so
    they can be stored in the DB to avoid re-screening. Each result dict
    includes ticker_a/ticker_b from the input pair.
    """
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

                if r.get("confidence") != "none" and r.get("antecedent_ticker") and r.get("consequent_ticker"):
                    log.info("ACCEPTED: %s -> %s [%s] %s",
                             r.get("antecedent_ticker"), r.get("consequent_ticker"),
                             r.get("confidence"), r.get("reasoning", ""))
                    accepted += 1
                else:
                    log.info("REJECTED: %s -> %s [%s] %s",
                             r.get("antecedent_ticker"), r.get("consequent_ticker"),
                             r.get("confidence"), r.get("reasoning", ""))
                    rejected += 1
                results.append(r)

            # Warn about input pairs with no LLM result
            unresulted = set(batch_lookup.keys()) - matched_keys
            for uk in unresulted:
                log.warning("Batch %d: no LLM result for input pair %s / %s",
                            batch_num, uk[0], uk[1])

            log.info("Batch %d summary: %d accepted, %d rejected, %d unmatched, %d missing",
                     batch_num, accepted, rejected, unmatched_count, len(unresulted))
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
        "--model", default="claude-haiku-4-5-20251001",
        help="Anthropic model name (default: claude-haiku-4-5-20251001)",
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
    print(f"\nScreening {len(pairs)} pairs with {model} (batch_size={batch_size})...")
    all_results = screen_pairs_with_llm(pairs, model, batch_size)

    # ── Store all results in DB ──────────────────────────────────────────
    stored = db_mod.bulk_upsert_pair_results(conn, all_results, model)
    print(f"  DB: {stored} pair results stored")

    # ── Filter to confirmed for output ───────────────────────────────────
    confirmed = [r for r in all_results if r.get("confidence") != "none" and r.get("antecedent_ticker") and r.get("consequent_ticker")]
    print(f"  Pairs with implication: {len(confirmed)}")

    # ── Output ───────────────────────────────────────────────────────────
    print_summary(confirmed)

    conn.close()


if __name__ == "__main__":
    main()
