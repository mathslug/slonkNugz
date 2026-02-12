#!/usr/bin/env python3
"""Kalshi cross-market arbitrage checker.

Exploits the logical relationship: winning the French Open => winning a Grand Slam.
Therefore: P(NOT win FO) + P(win a GS) >= 1 must hold.
If the market prices violate this, there's an arbitrage.

Strategy:
  Buy NO on "Musetti wins FO"   (pays $1 if he doesn't win FO)
  Buy YES on "Musetti wins GS"  (pays $1 if he wins any GS)

  Scenario A: Musetti wins FO  -> NO=$0, YES=$1 -> $1
  Scenario B: Loses FO, wins other GS -> NO=$1, YES=$1 -> $2 (bonus)
  Scenario C: Wins no GS -> NO=$1, YES=$0 -> $1

  Guaranteed minimum payout: $1 per contract pair.
  If total cost < $1 per pair, that's risk-free profit.
"""

import argparse
import logging
import sys
from dataclasses import dataclass, field
from datetime import date

import requests
from dotenv import load_dotenv

log = logging.getLogger(__name__)

from kalshi import (
    Side,
    Fill,
    LegResult,
    fetch_market,
    fetch_orderbook,
    taker_fee,
    walk_book,
)

load_dotenv()


# ── Data types ───────────────────────────────────────────────────────────────


@dataclass
class ArbResult:
    leg_a: LegResult
    leg_b: LegResult
    n_filled: int
    n_requested: int
    total_cost: float       # leg costs + all fees
    payoff: float           # guaranteed payout at settlement
    pv_payoff: float        # payoff discounted to today
    npv: float              # pv_payoff - total_cost
    days_to_settlement: int
    discount_rate: float
    tob_a_ask: float        # top-of-book ask for leg A
    tob_b_ask: float        # top-of-book ask for leg B
    market_a: dict = field(repr=False)
    market_b: dict = field(repr=False)

    @property
    def tob_cost(self) -> float:
        return self.tob_a_ask + self.tob_b_ask

    @property
    def has_tob_arb(self) -> bool:
        return self.tob_cost < 1.0

    @property
    def liquidity_constrained(self) -> bool:
        return self.n_filled < self.n_requested


# ── Core reusable evaluator ─────────────────────────────────────────────────


def evaluate_arb(
    ticker_a: str,
    side_a: Side,
    ticker_b: str,
    side_b: Side,
    n: int,
    settlement_date: date,
    discount_rate: float,
) -> ArbResult:
    """Evaluate NPV of a cross-market binary-contract arbitrage.

    Buys `side_a` on `ticker_a` and `side_b` on `ticker_b`, assuming the pair
    guarantees a minimum $1 payout at `settlement_date`.

    Walks the full orderbook for each leg to account for depth / slippage.
    Fees are computed per fill level using Kalshi's taker fee formula.

    Returns an ArbResult; the key field is `npv` (positive = profitable after
    discounting at `discount_rate`).
    """
    today = date.today()
    days = (settlement_date - today).days

    # Fetch market + orderbook data
    market_a = fetch_market(ticker_a)
    market_b = fetch_market(ticker_b)
    book_a = fetch_orderbook(ticker_a)
    book_b = fetch_orderbook(ticker_b)

    # Top-of-book asks
    tob_a = float(market_a[f"{side_a}_ask_dollars"])
    tob_b = float(market_b[f"{side_b}_ask_dollars"])

    # To buy YES, walk NO bids; to buy NO, walk YES bids.
    opposite = {"yes": "no", "no": "yes"}
    bids_a = list(reversed(book_a[opposite[side_a]]))
    bids_b = list(reversed(book_b[opposite[side_b]]))

    leg_a = walk_book(bids_a, n)
    leg_b = walk_book(bids_b, n)

    # Cap at available liquidity (min of both legs)
    effective_n = min(leg_a.filled, leg_b.filled)
    if effective_n < n and effective_n > 0:
        leg_a = walk_book(bids_a, effective_n)
        leg_b = walk_book(bids_b, effective_n)

    total_cost = leg_a.cost + leg_a.fees + leg_b.cost + leg_b.fees
    payoff = 1.0 * effective_n

    if days > 0:
        pv_payoff = payoff / (1.0 + discount_rate) ** (days / 365.0)
    else:
        pv_payoff = payoff

    npv = pv_payoff - total_cost

    return ArbResult(
        leg_a=leg_a,
        leg_b=leg_b,
        n_filled=effective_n,
        n_requested=n,
        total_cost=total_cost,
        payoff=payoff,
        pv_payoff=pv_payoff,
        npv=npv,
        days_to_settlement=days,
        discount_rate=discount_rate,
        tob_a_ask=tob_a,
        tob_b_ask=tob_b,
        market_a=market_a,
        market_b=market_b,
    )


# ── Pair evaluation (for confirmed arb pairs from DB) ──────────────────────


def fetch_pair_books(antecedent_ticker: str, consequent_ticker: str) -> dict:
    """Fetch orderbooks for both legs of an arb pair.

    Returns dict with bids reversed (best-first) for walking, plus
    top-of-book asks.

    Strategy: buy NO on antecedent (walk YES bids), buy YES on consequent (walk NO bids).
    """
    log.debug("fetch_pair_books: ant=%s con=%s", antecedent_ticker, consequent_ticker)
    ant_market = fetch_market(antecedent_ticker)
    con_market = fetch_market(consequent_ticker)
    ant_book = fetch_orderbook(antecedent_ticker)
    con_book = fetch_orderbook(consequent_ticker)

    # Buy NO on antecedent: walk YES bids (highest first)
    ant_bids = list(reversed(ant_book["yes"]))
    # Buy YES on consequent: walk NO bids (highest first)
    con_bids = list(reversed(con_book["no"]))

    return {
        "ant_bids": ant_bids,
        "con_bids": con_bids,
        "ant_tob_no_ask": float(ant_market["no_ask_dollars"]),
        "con_tob_yes_ask": float(con_market["yes_ask_dollars"]),
    }


def evaluate_pair(pair: dict, hurdle_yield: float, max_n: int = 500) -> dict:
    """Evaluate a confirmed arb pair against live orderbooks.

    Takes a pair dict (from get_pairs_for_review(conn, "confirmed")), fetches
    orderbooks, finds the optimal number of contracts via binary search where
    yield still exceeds the hurdle.

    Returns a dict with recommendation, optimal n, costs, yields, and fills.
    """
    from datetime import datetime

    pair_id = pair["id"]
    ant_ticker = pair["antecedent_ticker"]
    con_ticker = pair["consequent_ticker"]

    # Compute days to maturity (antecedent date = payoff date; by then the arb
    # is guaranteed to resolve regardless of outcome direction)
    ant_exp = pair.get("antecedent_expiration")
    if not ant_exp:
        return {"pair_id": pair_id, "recommendation": "pass", "n_contracts": 0,
                "annualized_yield": None, "hurdle_yield": hurdle_yield,
                "excess_yield": None, "days_to_maturity": None}

    try:
        settlement = datetime.fromisoformat(ant_exp.replace("Z", "+00:00")).date()
    except (ValueError, AttributeError):
        return {"pair_id": pair_id, "recommendation": "pass", "n_contracts": 0,
                "annualized_yield": None, "hurdle_yield": hurdle_yield,
                "excess_yield": None, "days_to_maturity": None}

    days = (settlement - date.today()).days
    if days <= 0:
        return {"pair_id": pair_id, "recommendation": "pass", "n_contracts": 0,
                "annualized_yield": None, "hurdle_yield": hurdle_yield,
                "excess_yield": None, "days_to_maturity": days}

    # Fetch orderbooks
    books = fetch_pair_books(ant_ticker, con_ticker)

    if not books["ant_bids"]:
        log.warning("Pair %s: empty antecedent orderbook (%s)", pair_id, ant_ticker)
    if not books["con_bids"]:
        log.warning("Pair %s: empty consequent orderbook (%s)", pair_id, con_ticker)

    def yield_at_n(n: int) -> float | None:
        leg_a = walk_book(books["ant_bids"], n)
        leg_b = walk_book(books["con_bids"], n)
        effective_n = min(leg_a.filled, leg_b.filled)
        if effective_n == 0:
            return None
        # Re-walk at effective_n if constrained
        if effective_n < n:
            leg_a = walk_book(books["ant_bids"], effective_n)
            leg_b = walk_book(books["con_bids"], effective_n)
        cost_per = (leg_a.cost + leg_a.fees + leg_b.cost + leg_b.fees) / effective_n
        if cost_per >= 1.0:
            return None
        return (1.0 / cost_per) ** (365.0 / days) - 1.0

    # Check yield at n=1
    y1 = yield_at_n(1)
    log.debug("Pair %s: yield_at_n(1) = %s, hurdle = %.4f", pair_id, y1, hurdle_yield)
    if y1 is None or y1 < hurdle_yield:
        # Build a pass result with top-of-book info
        tob_cost = books["ant_tob_no_ask"] + books["con_tob_yes_ask"]
        return {
            "pair_id": pair_id, "recommendation": "pass", "n_contracts": 0,
            "cost_per_pair": tob_cost, "total_cost": 0.0,
            "ant_leg_cost": 0.0, "ant_leg_fees": 0.0,
            "con_leg_cost": 0.0, "con_leg_fees": 0.0,
            "annualized_yield": y1, "hurdle_yield": hurdle_yield,
            "excess_yield": (y1 - hurdle_yield) if y1 is not None else None,
            "days_to_maturity": days, "max_fillable": 0,
            "tob_ant_no_ask": books["ant_tob_no_ask"],
            "tob_con_yes_ask": books["con_tob_yes_ask"],
            "tob_cost": tob_cost,
            "ant_fills": [], "con_fills": [],
        }

    # Find max fillable
    leg_a_max = walk_book(books["ant_bids"], max_n)
    leg_b_max = walk_book(books["con_bids"], max_n)
    max_fillable = min(leg_a_max.filled, leg_b_max.filled)

    # Binary search for largest n where yield >= hurdle
    lo, hi = 1, max_fillable
    best_n = 1
    while lo <= hi:
        mid = (lo + hi) // 2
        y = yield_at_n(mid)
        if y is not None and y >= hurdle_yield:
            best_n = mid
            lo = mid + 1
        else:
            hi = mid - 1

    log.debug("Pair %s: binary search -> best_n=%d, max_fillable=%d", pair_id, best_n, max_fillable)

    # Final evaluation at optimal n
    leg_a = walk_book(books["ant_bids"], best_n)
    leg_b = walk_book(books["con_bids"], best_n)
    effective_n = min(leg_a.filled, leg_b.filled)
    if effective_n < best_n and effective_n > 0:
        leg_a = walk_book(books["ant_bids"], effective_n)
        leg_b = walk_book(books["con_bids"], effective_n)
        best_n = effective_n

    total = leg_a.cost + leg_a.fees + leg_b.cost + leg_b.fees
    cost_per = total / best_n if best_n > 0 else None
    ann_yield = (1.0 / cost_per) ** (365.0 / days) - 1.0 if cost_per and cost_per < 1.0 else None
    tob_cost = books["ant_tob_no_ask"] + books["con_tob_yes_ask"]

    return {
        "pair_id": pair_id,
        "recommendation": "buy",
        "n_contracts": best_n,
        "cost_per_pair": round(cost_per, 6) if cost_per else None,
        "total_cost": round(total, 2),
        "ant_leg_cost": round(leg_a.cost, 4),
        "ant_leg_fees": round(leg_a.fees, 4),
        "con_leg_cost": round(leg_b.cost, 4),
        "con_leg_fees": round(leg_b.fees, 4),
        "annualized_yield": round(ann_yield, 6) if ann_yield is not None else None,
        "hurdle_yield": hurdle_yield,
        "excess_yield": round(ann_yield - hurdle_yield, 6) if ann_yield is not None else None,
        "days_to_maturity": days,
        "max_fillable": max_fillable,
        "tob_ant_no_ask": books["ant_tob_no_ask"],
        "tob_con_yes_ask": books["con_tob_yes_ask"],
        "tob_cost": round(tob_cost, 4),
        "ant_fills": [{"price": f.price, "qty": f.qty, "fee": f.fee} for f in leg_a.fills],
        "con_fills": [{"price": f.price, "qty": f.qty, "fee": f.fee} for f in leg_b.fills],
    }


# ── CLI: Musetti FO / GS tennis arb ─────────────────────────────────────────

FO_TICKER = "KXFOMEN-26-MUS"
GS_TICKER = "KXATPGRANDSLAM-26-LMUS"
FO_SETTLEMENT_DATE = date(2026, 6, 8)
DEFAULT_RFR = 0.035
DEFAULT_BUFFER = 0.01


def main() -> None:
    parser = argparse.ArgumentParser(description="Kalshi FO/GS arbitrage checker")
    parser.add_argument(
        "-n", "--contracts", type=int, default=100,
        help="number of contract pairs to evaluate (default: 100)",
    )
    parser.add_argument(
        "--rfr", type=float, default=DEFAULT_RFR,
        help=f"risk-free rate (default: {DEFAULT_RFR})",
    )
    parser.add_argument(
        "--buffer", type=float, default=DEFAULT_BUFFER,
        help=f"buffer above RFR (default: {DEFAULT_BUFFER})",
    )
    args = parser.parse_args()
    n = args.contracts
    hurdle = args.rfr + args.buffer

    if (FO_SETTLEMENT_DATE - date.today()).days <= 0:
        print("French Open 2026 has already settled.")
        sys.exit(1)

    print("Fetching Kalshi market data...\n")
    try:
        result = evaluate_arb(
            FO_TICKER, "no",
            GS_TICKER, "yes",
            n,
            FO_SETTLEMENT_DATE,
            hurdle,
        )
    except requests.HTTPError as e:
        print(f"API error: {e}")
        sys.exit(1)

    # ── Display ──────────────────────────────────────────────────────────────
    ma, mb = result.market_a, result.market_b
    fo_last = float(ma["last_price_dollars"])
    gs_last = float(mb["last_price_dollars"])

    print(f"{'Market':<32} {'YES ask':>8} {'NO ask':>8} {'Last':>8}")
    print("-" * 60)
    print(
        f"{'FO winner (Musetti)':<32}"
        f" ${float(ma['yes_ask_dollars']):>6.2f}"
        f"  ${result.tob_a_ask:>6.2f}"
        f"  ${fo_last:>6.2f}"
    )
    print(
        f"{'GS winner (Musetti)':<32}"
        f" ${result.tob_b_ask:>6.2f}"
        f"  ${float(mb['no_ask_dollars']):>6.2f}"
        f"  ${gs_last:>6.2f}"
    )
    print()

    arb_cost_last = (1.0 - fo_last) + gs_last
    print(f"Top-of-book arb cost:      ${result.tob_cost:.4f}  (need < $1.00)")
    print(f"Last-traded arb cost:      ${arb_cost_last:.4f}  (need < $1.00)")
    print()

    if not result.has_tob_arb:
        print(
            "No actionable arbitrage at current ask prices"
            f" (${result.tob_cost - 1:.4f} above parity)."
        )
        if arb_cost_last < 1.0:
            print(
                f"\n  Note: last-traded prices imply ${1.0 - arb_cost_last:.4f} spread,"
                "\n  but that requires limit orders, not immediately executable."
            )
        sys.exit(0)

    # ── Liquidity warning ────────────────────────────────────────────────────
    if result.liquidity_constrained:
        print(f"Insufficient liquidity for {result.n_requested} contracts.")
        if not result.leg_a.sufficient:
            print(f"  FO NO side: only {result.leg_a.filled} available")
        if not result.leg_b.sufficient:
            print(f"  GS YES side: only {result.leg_b.filled} available")
        if result.n_filled == 0:
            sys.exit(0)
        print(f"  Showing analysis for {result.n_filled} fillable pairs.\n")

    n = result.n_filled
    la, lb = result.leg_a, result.leg_b

    print(f"Trade simulation ({n} contract pairs, walking the book)")
    print("=" * 56)

    print("\n  Buy NO on FO (via FO YES bids):")
    for f in la.fills:
        print(f"    {f.qty:>6} contracts @ ${f.price:.2f}  (fee ${f.fee:.2f})")
    print(f"    Subtotal: ${la.cost:>8.2f}  fees: ${la.fees:>6.2f}")

    print("\n  Buy YES on GS (via GS NO bids):")
    for f in lb.fills:
        print(f"    {f.qty:>6} contracts @ ${f.price:.2f}  (fee ${f.fee:.2f})")
    print(f"    Subtotal: ${lb.cost:>8.2f}  fees: ${lb.fees:>6.2f}")

    print(f"\n  Avg fill NO FO:  ${la.cost / n:.4f}")
    print(f"  Avg fill YES GS: ${lb.cost / n:.4f}")
    print(f"  Avg pair cost:   ${(la.cost + lb.cost) / n:.4f}  (excl. fees)")

    print(f"\n  {'':─<50}")
    print(f"  Total outlay (today):       ${result.total_cost:>10.2f}")
    print(f"  Guaranteed payoff (nominal): ${result.payoff:>9.2f}")
    print(f"  PV of payoff:               ${result.pv_payoff:>10.2f}")
    print(f"  NPV:                        ${result.npv:>10.2f}")
    print()
    print(f"  Days to settlement:  {result.days_to_settlement}")
    print(
        f"  Discount rate:       {hurdle:>8.2%}"
        f"  (RFR {args.rfr:.1%} + buffer {args.buffer:.1%})"
    )
    print()

    if result.npv > 0:
        print(f"BUY: NPV is ${result.npv:.2f} (positive at {hurdle:.2%} discount rate).")
    else:
        print(f"PASS: NPV is ${result.npv:.2f} (not profitable at {hurdle:.2%} discount rate).")


if __name__ == "__main__":
    main()
