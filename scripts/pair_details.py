#!/usr/bin/env python3
"""Print full details for BUY signal pairs, sorted by excess yield.

Usage:
    uv run scripts/pair_details.py                      # all BUYs from kalshi_arb_prod.db
    uv run scripts/pair_details.py --db slonk_arb.db    # custom DB
    uv run scripts/pair_details.py --limit 10            # top 10 only
    uv run scripts/pair_details.py --pair-id 5829        # single pair deep dive
"""
import argparse
import sys
import textwrap

sys.path.insert(0, ".")
import db


def truncate(text: str | None, max_len: int = 200) -> str:
    if not text:
        return ""
    text = " ".join(text.split())  # collapse whitespace
    if len(text) <= max_len:
        return text
    return text[:max_len] + "..."


def fmt_yield(val: float | None, cap: float = 10.0) -> str:
    if val is None:
        return "n/a"
    if abs(val) >= cap:
        return ">999%"
    return f"{val * 100:.1f}%"


def fmt_dollars(val: float | None) -> str:
    if val is None:
        return "n/a"
    return f"${val:.2f}"


def get_rules(conn, tickers: list[str]) -> dict[str, str]:
    """Fetch rules_primary for a list of tickers."""
    if not tickers:
        return {}
    placeholders = ",".join("?" * len(tickers))
    rows = conn.execute(
        f"SELECT ticker, rules_primary FROM tickers WHERE ticker IN ({placeholders})",
        tickers,
    ).fetchall()
    return {r["ticker"]: r["rules_primary"] for r in rows}


def print_pair(e: dict, rules: dict[str, str], index: int | None = None) -> None:
    prefix = f"#{index} " if index is not None else ""
    pair_id = e["pair_id"]
    print(f"{'=' * 78}")
    print(f"{prefix}Pair #{pair_id}  |  confidence: {e.get('confidence', '?')}"
          f"  |  evaluated: {e.get('evaluated_at', '?')[:10]}")
    print(f"{'-' * 78}")

    y = fmt_yield(e.get("annualized_yield"))
    ex = fmt_yield(e.get("excess_yield"))
    hurdle = fmt_yield(e.get("hurdle_yield"))
    cost = fmt_dollars(e.get("total_cost"))
    cpp = fmt_dollars(e.get("cost_per_pair"))
    days = e.get("days_to_maturity", "?")
    n = e.get("n_contracts", 0)
    fill = e.get("max_fillable", "?")

    print(f"  Yield: {y}  |  Excess: {ex}  |  Hurdle: {hurdle}")
    print(f"  Cost:  {cost} ({cpp}/pair x {n})  |  Days: {days}  |  Max fill: {fill}")
    print()

    ant_ticker = e.get("antecedent_ticker", "?")
    con_ticker = e.get("consequent_ticker", "?")
    ant_event = e.get("antecedent_event_ticker", "")
    con_event = e.get("consequent_event_ticker", "")

    print(f"  ANTECEDENT (buy NO):  {ant_ticker}")
    print(f"    Title: {e.get('antecedent_title', '?')}")
    print(f"    Event: {ant_event}  ->  https://kalshi.com/markets/{ant_event}")
    ant_rules = rules.get(ant_ticker, "")
    if ant_rules:
        print(f"    Rules: {truncate(ant_rules, 300)}")
    print()

    print(f"  CONSEQUENT (buy YES): {con_ticker}")
    print(f"    Title: {e.get('consequent_title', '?')}")
    print(f"    Event: {con_event}  ->  https://kalshi.com/markets/{con_event}")
    con_rules = rules.get(con_ticker, "")
    if con_rules:
        print(f"    Rules: {truncate(con_rules, 300)}")
    print()

    reasoning = e.get("reasoning", "")
    if reasoning:
        wrapped = textwrap.fill(reasoning, width=74, initial_indent="    ", subsequent_indent="    ")
        print(f"  LLM Reasoning:")
        print(wrapped)
    print()


def main():
    parser = argparse.ArgumentParser(description="Print full details for BUY signal pairs")
    parser.add_argument("--db", default="kalshi_arb_prod.db", help="SQLite database path")
    parser.add_argument("--limit", type=int, default=0, help="Show only top N pairs")
    parser.add_argument("--pair-id", type=int, default=0, help="Show a single pair by ID")
    args = parser.parse_args()

    conn = db.get_connection(args.db)

    if args.pair_id:
        # Single pair mode: use get_latest_evaluations and filter
        evals = db.get_latest_evaluations(conn)
        evals = [e for e in evals if e["pair_id"] == args.pair_id]
        if not evals:
            print(f"No BUY evaluation found for pair #{args.pair_id}")
            conn.close()
            return
    else:
        evals = db.get_latest_evaluations(conn)

    if args.limit > 0:
        evals = evals[:args.limit]

    # Collect all tickers for rules lookup
    all_tickers = []
    for e in evals:
        if e.get("antecedent_ticker"):
            all_tickers.append(e["antecedent_ticker"])
        if e.get("consequent_ticker"):
            all_tickers.append(e["consequent_ticker"])
    rules = get_rules(conn, list(set(all_tickers)))

    print(f"BUY signals: {len(evals)} pairs (from {args.db})")
    print()

    for i, e in enumerate(evals, 1):
        print_pair(e, rules, index=i)

    conn.close()


if __name__ == "__main__":
    main()
