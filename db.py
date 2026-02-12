"""SQLite persistence for Kalshi arbitrage scanner.

All functions take a sqlite3.Connection as first arg — no global state.
Designed for REPL use: import db; conn = db.get_connection("kalshi_arb.db")
"""

import json
import sqlite3
from datetime import datetime, timezone


SCHEMA_SQL = """\
CREATE TABLE IF NOT EXISTS tickers (
    ticker                   TEXT PRIMARY KEY,
    series_ticker            TEXT NOT NULL,
    event_ticker             TEXT NOT NULL,
    title                    TEXT NOT NULL DEFAULT '',
    yes_sub_title            TEXT NOT NULL DEFAULT '',
    rules_primary            TEXT NOT NULL DEFAULT '',
    expected_expiration_time TEXT,
    close_time               TEXT,
    last_price_dollars       TEXT,
    yes_ask_dollars          TEXT,
    no_ask_dollars           TEXT,
    volume                   INTEGER NOT NULL DEFAULT 0,
    first_seen               TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    last_scanned             TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    is_active                INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS prices (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker      TEXT NOT NULL REFERENCES tickers(ticker),
    recorded_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    last_price  TEXT,
    yes_ask     TEXT,
    no_ask      TEXT
);

CREATE TABLE IF NOT EXISTS candidate_pairs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker_a        TEXT NOT NULL REFERENCES tickers(ticker),
    ticker_b        TEXT NOT NULL REFERENCES tickers(ticker),
    subset_ticker   TEXT,
    superset_ticker TEXT,
    confidence      TEXT CHECK(confidence IN ('high','medium','low','none')),
    reasoning       TEXT,
    llm_model       TEXT,
    screened_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    human_review    TEXT CHECK(human_review IN ('confirmed','rejected') OR human_review IS NULL),
    reviewed_at     TEXT,
    UNIQUE(ticker_a, ticker_b)
);
"""


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _sorted_pair(ticker_a: str, ticker_b: str) -> tuple[str, str]:
    """Return tickers in sorted order so UNIQUE constraint works."""
    return (ticker_a, ticker_b) if ticker_a <= ticker_b else (ticker_b, ticker_a)


def init_db(db_path: str) -> None:
    """Create tables. Idempotent."""
    conn = sqlite3.connect(db_path) if db_path != ":memory:" else sqlite3.connect(":memory:")
    conn.executescript(SCHEMA_SQL)
    conn.close()


def get_connection(db_path: str = "kalshi_arb.db") -> sqlite3.Connection:
    """REPL-friendly connection helper. Sets WAL mode, foreign keys, Row factory."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA_SQL)
    return conn


def upsert_tickers(conn: sqlite3.Connection, markets: list[dict]) -> tuple[int, int]:
    """Insert or update tickers from fetched market dicts. Returns (new, updated)."""
    new = 0
    updated = 0
    now = _now_utc()
    for m in markets:
        existing = conn.execute(
            "SELECT ticker FROM tickers WHERE ticker = ?", (m["ticker"],)
        ).fetchone()
        if existing:
            conn.execute(
                """UPDATE tickers SET
                    series_ticker = ?, event_ticker = ?, title = ?,
                    yes_sub_title = ?, rules_primary = ?,
                    expected_expiration_time = ?, close_time = ?,
                    last_price_dollars = ?, yes_ask_dollars = ?, no_ask_dollars = ?,
                    volume = ?, last_scanned = ?, is_active = 1
                WHERE ticker = ?""",
                (
                    m["series_ticker"], m["event_ticker"], m.get("title", ""),
                    m.get("yes_sub_title", ""), m.get("rules_primary", ""),
                    m.get("expected_expiration_time"), m.get("close_time"),
                    m.get("last_price_dollars"), m.get("yes_ask_dollars"),
                    m.get("no_ask_dollars"), int(m.get("volume", 0) or 0),
                    now, m["ticker"],
                ),
            )
            updated += 1
        else:
            conn.execute(
                """INSERT INTO tickers
                    (ticker, series_ticker, event_ticker, title, yes_sub_title,
                     rules_primary, expected_expiration_time, close_time,
                     last_price_dollars, yes_ask_dollars, no_ask_dollars,
                     volume, first_seen, last_scanned, is_active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)""",
                (
                    m["ticker"], m["series_ticker"], m["event_ticker"],
                    m.get("title", ""), m.get("yes_sub_title", ""),
                    m.get("rules_primary", ""),
                    m.get("expected_expiration_time"), m.get("close_time"),
                    m.get("last_price_dollars"), m.get("yes_ask_dollars"),
                    m.get("no_ask_dollars"), int(m.get("volume", 0) or 0),
                    now, now,
                ),
            )
            new += 1
    conn.commit()
    return new, updated


def record_prices(conn: sqlite3.Connection, markets: list[dict]) -> int:
    """Append a price snapshot per market into the prices history table.

    Returns count of rows inserted.
    """
    now = _now_utc()
    rows = [
        (
            m["ticker"],
            now,
            m.get("last_price_dollars"),
            m.get("yes_ask_dollars"),
            m.get("no_ask_dollars"),
        )
        for m in markets
    ]
    conn.executemany(
        """INSERT INTO prices (ticker, recorded_at, last_price, yes_ask, no_ask)
        VALUES (?, ?, ?, ?, ?)""",
        rows,
    )
    conn.commit()
    return len(rows)


def get_tickers_by_entity(conn: sqlite3.Connection) -> dict[str, list[dict]]:
    """Group active tickers by yes_sub_title where entity spans 2+ series.

    Returns dict mapping entity -> list of market dicts (matching the format
    used by generate_candidate_pairs).
    """
    rows = conn.execute(
        """SELECT ticker, series_ticker, event_ticker, title, yes_sub_title,
                  rules_primary, expected_expiration_time, close_time,
                  last_price_dollars, yes_ask_dollars, no_ask_dollars, volume
           FROM tickers WHERE is_active = 1 AND yes_sub_title != ''"""
    ).fetchall()

    groups: dict[str, list[dict]] = {}
    for r in rows:
        m = dict(r)
        groups.setdefault(m["yes_sub_title"], []).append(m)

    # Only keep entities appearing in 2+ series
    return {
        entity: markets
        for entity, markets in groups.items()
        if len({m["series_ticker"] for m in markets}) >= 2
    }


def get_screened_pair_keys(conn: sqlite3.Connection) -> set[tuple[str, str]]:
    """Return set of (ticker_a, ticker_b) already evaluated."""
    rows = conn.execute("SELECT ticker_a, ticker_b FROM candidate_pairs").fetchall()
    return {(r["ticker_a"], r["ticker_b"]) for r in rows}


def sorted_key(pair: tuple[dict, dict]) -> tuple[str, str]:
    """Canonical sorted key for a market-dict pair."""
    return _sorted_pair(pair[0]["ticker"], pair[1]["ticker"])


def bulk_upsert_pair_results(
    conn: sqlite3.Connection,
    results: list[dict],
    model: str,
) -> int:
    """Store LLM screening results (including 'none' confidence).

    Each result dict must have ticker_a, ticker_b, and may have subset_ticker,
    superset_ticker, confidence, reasoning. Returns count of rows upserted.
    """
    count = 0
    now = _now_utc()
    for r in results:
        ta, tb = _sorted_pair(r["ticker_a"], r["ticker_b"])
        conn.execute(
            """INSERT INTO candidate_pairs
                (ticker_a, ticker_b, subset_ticker, superset_ticker,
                 confidence, reasoning, llm_model, screened_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(ticker_a, ticker_b) DO UPDATE SET
                subset_ticker = excluded.subset_ticker,
                superset_ticker = excluded.superset_ticker,
                confidence = excluded.confidence,
                reasoning = excluded.reasoning,
                llm_model = excluded.llm_model,
                screened_at = excluded.screened_at""",
            (
                ta, tb,
                r.get("subset_ticker"), r.get("superset_ticker"),
                r.get("confidence"), r.get("reasoning"),
                model, now,
            ),
        )
        count += 1
    conn.commit()
    return count


def get_pairs_for_review(conn: sqlite3.Connection, status: str) -> list[dict]:
    """Fetch pairs for review UI.

    status: "unreviewed" | "confirmed" | "rejected"
    Returns list of dicts with pair + joined ticker info + computed arb_cost.
    """
    if status == "unreviewed":
        where = "cp.human_review IS NULL AND cp.confidence != 'none'"
    elif status == "confirmed":
        where = "cp.human_review = 'confirmed'"
    elif status == "rejected":
        where = "cp.human_review = 'rejected'"
    else:
        raise ValueError(f"Invalid status: {status}")

    rows = conn.execute(
        f"""SELECT
            cp.id, cp.ticker_a, cp.ticker_b,
            cp.subset_ticker, cp.superset_ticker,
            cp.confidence, cp.reasoning, cp.llm_model,
            cp.screened_at, cp.human_review, cp.reviewed_at,
            sub.title AS subset_title,
            sub.yes_ask_dollars AS subset_yes_ask,
            sub.no_ask_dollars AS subset_no_ask,
            sub.event_ticker AS subset_event_ticker,
            sup.title AS superset_title,
            sup.yes_ask_dollars AS superset_yes_ask,
            sup.no_ask_dollars AS superset_no_ask,
            sup.event_ticker AS superset_event_ticker
        FROM candidate_pairs cp
        LEFT JOIN tickers sub ON sub.ticker = cp.subset_ticker
        LEFT JOIN tickers sup ON sup.ticker = cp.superset_ticker
        WHERE {where}
        ORDER BY cp.confidence DESC, cp.screened_at DESC""",
    ).fetchall()

    result = []
    for r in rows:
        d = dict(r)
        # Compute arb cost: buy NO on subset + YES on superset
        sub_no = d.get("subset_no_ask")
        sup_yes = d.get("superset_yes_ask")
        if sub_no is not None and sup_yes is not None:
            try:
                d["arb_cost"] = round(float(sub_no) + float(sup_yes), 4)
            except (ValueError, TypeError):
                d["arb_cost"] = None
        else:
            d["arb_cost"] = None
        result.append(d)

    return result


def get_pair_detail(conn: sqlite3.Connection, pair_id: int) -> dict | None:
    """Full info for a single pair, including both markets."""
    row = conn.execute(
        """SELECT
            cp.*,
            sub.title AS subset_title, sub.yes_sub_title AS subset_entity,
            sub.series_ticker AS subset_series, sub.event_ticker AS subset_event_ticker,
            sub.rules_primary AS subset_rules,
            sub.expected_expiration_time AS subset_expiration,
            sub.last_price_dollars AS subset_last_price,
            sub.yes_ask_dollars AS subset_yes_ask,
            sub.no_ask_dollars AS subset_no_ask,
            sub.volume AS subset_volume,
            sup.title AS superset_title, sup.yes_sub_title AS superset_entity,
            sup.series_ticker AS superset_series, sup.event_ticker AS superset_event_ticker,
            sup.rules_primary AS superset_rules,
            sup.expected_expiration_time AS superset_expiration,
            sup.last_price_dollars AS superset_last_price,
            sup.yes_ask_dollars AS superset_yes_ask,
            sup.no_ask_dollars AS superset_no_ask,
            sup.volume AS superset_volume
        FROM candidate_pairs cp
        LEFT JOIN tickers sub ON sub.ticker = cp.subset_ticker
        LEFT JOIN tickers sup ON sup.ticker = cp.superset_ticker
        WHERE cp.id = ?""",
        (pair_id,),
    ).fetchone()

    if not row:
        return None

    d = dict(row)
    sub_no = d.get("subset_no_ask")
    sup_yes = d.get("superset_yes_ask")
    if sub_no is not None and sup_yes is not None:
        try:
            d["arb_cost"] = round(float(sub_no) + float(sup_yes), 4)
        except (ValueError, TypeError):
            d["arb_cost"] = None
    else:
        d["arb_cost"] = None
    return d


def set_review(conn: sqlite3.Connection, pair_id: int, decision: str) -> None:
    """Set human_review on a pair. decision must be 'confirmed' or 'rejected'."""
    if decision not in ("confirmed", "rejected"):
        raise ValueError(f"Invalid decision: {decision}")
    conn.execute(
        "UPDATE candidate_pairs SET human_review = ?, reviewed_at = ? WHERE id = ?",
        (decision, _now_utc(), pair_id),
    )
    conn.commit()


def get_pair_stats(conn: sqlite3.Connection) -> dict:
    """Return counts for dashboard."""
    row = conn.execute(
        """SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN confidence != 'none' AND human_review IS NULL THEN 1 ELSE 0 END) AS unreviewed,
            SUM(CASE WHEN human_review = 'confirmed' THEN 1 ELSE 0 END) AS confirmed,
            SUM(CASE WHEN human_review = 'rejected' THEN 1 ELSE 0 END) AS rejected,
            SUM(CASE WHEN confidence = 'none' THEN 1 ELSE 0 END) AS no_relationship
        FROM candidate_pairs"""
    ).fetchone()
    return dict(row)


def import_from_cache(
    conn: sqlite3.Connection,
    cache_path: str,
    results_path: str | None = None,
) -> tuple[int, int]:
    """Bootstrap DB from existing JSON cache and optional scan results.

    Returns (tickers_imported, pairs_imported).
    """
    with open(cache_path) as f:
        cache = json.load(f)

    markets = cache.get("markets", [])
    new, updated = upsert_tickers(conn, markets)
    ticker_count = new + updated

    pair_count = 0
    if results_path:
        with open(results_path) as f:
            results = json.load(f)
        for r in results:
            sub = r.get("subset_ticker")
            sup = r.get("superset_ticker")
            if not sub or not sup:
                continue
            r["ticker_a"] = sub
            r["ticker_b"] = sup
        pair_count = bulk_upsert_pair_results(conn, results, model="imported")

    return ticker_count, pair_count
