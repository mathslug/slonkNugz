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
import sys
from dataclasses import dataclass, field
from datetime import date

import requests
from dotenv import load_dotenv

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
