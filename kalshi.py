"""Shared Kalshi API helpers, fee model, and orderbook utilities."""

import math
from dataclasses import dataclass
from typing import Literal

import requests

# ── Kalshi constants ─────────────────────────────────────────────────────────

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"

# Kalshi taker fee: ceil(0.07 * C * P * (1 - P) * 100) / 100
# Source: https://kalshi.com/fee-schedule
TAKER_FEE_COEFF = 0.07


# ── Data types ───────────────────────────────────────────────────────────────

Side = Literal["yes", "no"]


@dataclass
class Fill:
    price: float
    qty: int
    fee: float


@dataclass
class LegResult:
    fills: list[Fill]
    cost: float  # sum of price * qty across fills
    fees: float  # sum of per-fill taker fees
    filled: int
    requested: int

    @property
    def sufficient(self) -> bool:
        return self.filled >= self.requested


# ── API helpers ──────────────────────────────────────────────────────────────


def fetch_market(ticker: str) -> dict:
    """Fetch a single market from the Kalshi public API (no auth required)."""
    resp = requests.get(f"{KALSHI_BASE}/markets/{ticker}", timeout=10)
    resp.raise_for_status()
    return resp.json()["market"]


def fetch_orderbook(ticker: str) -> dict:
    """Fetch the orderbook for a market.

    Returns dict with 'yes' and 'no' bid arrays.
    Each entry is (price_dollars, quantity).
    Arrays arrive sorted ascending from API; caller should reverse to walk
    best-first.

    Kalshi returns bids only:
      - YES bid at $P  =  NO ask at $(1-P)
      - NO bid at $P   =  YES ask at $(1-P)
    """
    resp = requests.get(f"{KALSHI_BASE}/markets/{ticker}/orderbook", timeout=10)
    resp.raise_for_status()
    data = resp.json()["orderbook"]

    def parse_levels(raw: list) -> list[tuple[float, int]]:
        return [(float(price), int(qty)) for price, qty in raw]

    return {
        "yes": parse_levels(data.get("yes_dollars", [])),
        "no": parse_levels(data.get("no_dollars", [])),
    }


# ── Fee & book-walking ──────────────────────────────────────────────────────


def taker_fee(num_contracts: int, price: float) -> float:
    """Kalshi taker fee in dollars for a fill of num_contracts at price.

    fee = ceil(0.07 * C * P * (1 - P) * 100) / 100
    P is contract price in dollars [0, 1]. Result rounded up to nearest cent.
    """
    raw = TAKER_FEE_COEFF * num_contracts * price * (1.0 - price)
    return math.ceil(raw * 100) / 100


def walk_book(opposite_bids: list[tuple[float, int]], n: int) -> LegResult:
    """Simulate filling n contracts by walking the opposite side's bids.

    Pass YES bids (highest first) to buy NO, or NO bids (highest first) to
    buy YES.  Fill price = 1 - bid_price.  Fees computed per fill level.
    """
    remaining = n
    fills: list[Fill] = []
    total_cost = 0.0
    total_fees = 0.0

    for bid_price, qty_available in opposite_bids:
        if remaining <= 0:
            break
        fill_qty = min(remaining, qty_available)
        fill_price = round(1.0 - bid_price, 4)
        fee = taker_fee(fill_qty, fill_price)
        fills.append(Fill(fill_price, fill_qty, fee))
        total_cost += fill_price * fill_qty
        total_fees += fee
        remaining -= fill_qty

    filled = n - remaining
    return LegResult(fills, total_cost, total_fees, filled, n)
