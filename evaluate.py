#!/usr/bin/env python3
"""Evaluate arb pairs against live orderbooks.

Fetches orderbooks for confirmed or high-confidence pairs, finds optimal
contract counts where yield exceeds the hurdle, and stores results in the DB.

Usage:
    uv run evaluate.py                      # human-confirmed pairs (default)
    uv run evaluate.py --mode high          # high-confidence unreviewed pairs
    uv run evaluate.py --mode high --max-n 500
"""

import argparse
import logging
import sys
import time

import requests
from dotenv import load_dotenv

log = logging.getLogger("evaluate")

import db as db_mod
from main import evaluate_pair

load_dotenv()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Evaluate confirmed arb pairs against live orderbooks"
    )
    parser.add_argument(
        "--db", default="kalshi_arb.db",
        help="SQLite database path (default: kalshi_arb.db)",
    )
    parser.add_argument(
        "--max-n", type=int, default=500,
        help="max contracts to search for optimal fill (default: 500)",
    )
    parser.add_argument(
        "--mode", choices=["confirmed", "high"], default="confirmed",
        help="confirmed = human-approved pairs, high = high-confidence unreviewed (default: confirmed)",
    )
    parser.add_argument(
        "--log-file", default="evaluate.log",
        help="log file path (default: evaluate.log)",
    )
    args = parser.parse_args()

    # ── Logging setup ────────────────────────────────────────────────────
    logging.basicConfig(
        filename=args.log_file,
        filemode="w",
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%H:%M:%S",
    )

    conn = db_mod.get_connection(args.db)
    db_status = "confirmed" if args.mode == "confirmed" else "high_unreviewed"
    pairs = db_mod.get_pairs_for_review(conn, db_status)

    if not pairs:
        print(f"No {args.mode} pairs to evaluate.")
        conn.close()
        sys.exit(0)

    label = "human-confirmed" if args.mode == "confirmed" else "high-confidence unreviewed"
    print(f"Evaluating {len(pairs)} {label} pairs (max_n={args.max_n})...\n")

    results = []
    for i, pair in enumerate(pairs, 1):
        ant = pair["antecedent_ticker"]
        con = pair["consequent_ticker"]
        print(f"  [{i}/{len(pairs)}] {ant} / {con} ...", end=" ", flush=True)

        # Compute hurdle from DB settings
        hurdle = pair.get("hurdle_yield")
        if hurdle is None:
            hurdle = 0.06  # fallback

        try:
            result = evaluate_pair(pair, hurdle, args.max_n)
            results.append(result)
            rec = result["recommendation"].upper()
            if rec == "BUY":
                y = result.get("annualized_yield")
                n = result.get("n_contracts", 0)
                cost = result.get("total_cost", 0)
                log.info("BUY pair_id=%s %s/%s yield=%.4f n=%d cost=%.2f",
                         pair["id"], ant, con, y, n, cost)
                print(f"{rec}  n={n}  yield={y*100:.2f}%  cost=${cost:.2f}")
            else:
                y = result.get("annualized_yield")
                y_str = f"{y*100:.2f}%" if y is not None else "N/A"
                log.info("PASS pair_id=%s %s/%s yield=%s",
                         pair["id"], ant, con, y_str)
                print(f"PASS  yield={y_str}")
        except requests.HTTPError as e:
            log.warning("API error pair_id=%s %s/%s: %s", pair["id"], ant, con, e)
            print(f"API error: {e}")
            continue
        except Exception as e:
            log.warning("Error pair_id=%s %s/%s: %s", pair["id"], ant, con, e)
            print(f"error: {e}")
            continue

        # Rate limit between pairs (4 API calls per pair)
        if i < len(pairs):
            time.sleep(0.5)

    if not results:
        print("\nNo evaluations completed.")
        conn.close()
        sys.exit(1)

    # Store results
    stored = db_mod.bulk_insert_evaluations(conn, results)
    print(f"\n{'='*60}")
    print(f"Results: {stored} evaluations stored")

    buys = [r for r in results if r["recommendation"] == "buy"]
    passes = [r for r in results if r["recommendation"] == "pass"]
    print(f"  BUY:  {len(buys)}")
    print(f"  PASS: {len(passes)}")

    if buys:
        total_capital = sum(r.get("total_cost", 0) for r in buys)
        print(f"\n  Total capital needed: ${total_capital:.2f}")
        for r in sorted(buys, key=lambda x: -(x.get("excess_yield") or 0)):
            print(
                f"    Pair #{r['pair_id']:>3}  "
                f"n={r['n_contracts']:>4}  "
                f"yield={r['annualized_yield']*100:>6.2f}%  "
                f"excess={r['excess_yield']*100:>+6.2f}%  "
                f"cost=${r['total_cost']:>8.2f}"
            )

    conn.close()


if __name__ == "__main__":
    main()
