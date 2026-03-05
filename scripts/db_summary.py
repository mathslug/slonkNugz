#!/usr/bin/env python3
"""Print a summary of the production DB: counts, pair breakdown, top BUYs, yields."""
import sys
sys.path.insert(0, ".")
import db

DB = sys.argv[1] if len(sys.argv) > 1 else "kalshi_arb_prod.db"
conn = db.get_connection(DB)

# ── Counts ──
ticker_count = conn.execute("SELECT count(*) FROM tickers").fetchone()[0]
active_count = conn.execute("SELECT count(*) FROM tickers WHERE is_active = 1").fetchone()[0]
price_count = conn.execute("SELECT count(*) FROM prices").fetchone()[0]
pair_count = conn.execute("SELECT count(*) FROM candidate_pairs").fetchone()[0]
eval_count = conn.execute("SELECT count(*) FROM trade_evaluations").fetchone()[0]
buy_count = conn.execute("SELECT count(*) FROM trade_evaluations WHERE recommendation='buy'").fetchone()[0]

print(f"Tickers:      {ticker_count:>6} ({active_count} active)")
print(f"Price rows:   {price_count:>6}")
print(f"Pairs:        {pair_count:>6}")
print(f"Evaluations:  {eval_count:>6} ({buy_count} BUY)")

# ── Pair breakdown ──
print("\nPairs by status / confidence:")
for row in conn.execute("""
    SELECT COALESCE(human_review, 'unreviewed') as status, confidence, count(*)
    FROM candidate_pairs GROUP BY 1, 2 ORDER BY 1, 2
""").fetchall():
    print(f"  {row[0]:12} {row[1]:8} {row[2]:>5}")

# ── Reasonable BUYs ──
print(f"\nBUY signals (sorted by excess yield):")
for row in conn.execute("""
    SELECT te.pair_id, te.n_contracts, te.annualized_yield, te.excess_yield,
           te.total_cost, te.days_to_maturity,
           cp.antecedent_ticker, cp.consequent_ticker, cp.confidence
    FROM trade_evaluations te
    JOIN candidate_pairs cp ON te.pair_id = cp.id
    WHERE te.recommendation = 'buy'
    ORDER BY te.excess_yield DESC
""").fetchall():
    y = f"{row[2]*100:.1f}%" if row[2] is not None and abs(row[2]) < 10 else ">999%"
    ex = f"{row[3]*100:+.1f}%" if row[3] is not None and abs(row[3]) < 10 else ">999%"
    print(f"  #{row[0]:>5}  n={row[1]:>4}  yield={y:>8}  excess={ex:>8}"
          f"  cost=${row[4]:>7.2f}  days={row[5]:>3}  [{row[8]}]")
    print(f"         {row[6]} -> {row[7]}")

# ── Treasury yields ──
print("\nRecent Treasury yields:")
for row in conn.execute(
    "SELECT date, y1, y2, y5, y10, y30 FROM treasury_yields ORDER BY date DESC LIMIT 5"
).fetchall():
    print(f"  {row[0]}  1Y={row[1]:.2f}%  2Y={row[2]:.2f}%  5Y={row[3]:.2f}%  10Y={row[4]:.2f}%  30Y={row[5]:.2f}%")

conn.close()
