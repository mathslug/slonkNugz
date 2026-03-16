"""SQLite persistence for Kalshi arbitrage scanner.

All functions take a sqlite3.Connection as first arg — no global state.
Designed for REPL use: import db; conn = db.get_connection("slonk_arb.db")
"""

import json
import sqlite3
from datetime import date, datetime, timezone


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
    antecedent_ticker  TEXT,
    consequent_ticker TEXT,
    confidence      TEXT CHECK(confidence IN ('high','medium','low','none','need_more_info')),
    reasoning       TEXT,
    llm_model       TEXT,
    screened_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    human_review    TEXT CHECK(human_review IN ('confirmed','rejected') OR human_review IS NULL),
    reviewed_at     TEXT,
    UNIQUE(ticker_a, ticker_b)
);

CREATE TABLE IF NOT EXISTS trade_evaluations (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    pair_id           INTEGER NOT NULL REFERENCES candidate_pairs(id),
    evaluated_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    recommendation    TEXT NOT NULL CHECK(recommendation IN ('buy','pass')),
    n_contracts       INTEGER NOT NULL DEFAULT 0,
    cost_per_pair     REAL,
    total_cost        REAL,
    ant_leg_cost      REAL,
    ant_leg_fees      REAL,
    con_leg_cost      REAL,
    con_leg_fees      REAL,
    annualized_yield  REAL,
    hurdle_yield      REAL,
    excess_yield      REAL,
    days_to_maturity  INTEGER,
    max_fillable      INTEGER,
    tob_ant_no_ask    REAL,
    tob_con_yes_ask   REAL,
    tob_cost          REAL,
    ant_fills_json    TEXT,
    con_fills_json    TEXT
);

CREATE TABLE IF NOT EXISTS treasury_yields (
    date        TEXT PRIMARY KEY,
    m1 REAL, m1h REAL, m2 REAL, m3 REAL, m4 REAL, m6 REAL,
    y1 REAL, y2 REAL, y3 REAL, y5 REAL, y7 REAL,
    y10 REAL, y20 REAL, y30 REAL,
    fetched_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
);

CREATE TABLE IF NOT EXISTS settings (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
);
"""


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _sorted_pair(ticker_a: str, ticker_b: str) -> tuple[str, str]:
    """Return tickers in sorted order so UNIQUE constraint works."""
    return (ticker_a, ticker_b) if ticker_a <= ticker_b else (ticker_b, ticker_a)


_MIGRATIONS = [
    "ALTER TABLE candidate_pairs RENAME COLUMN subset_ticker TO antecedent_ticker",
    "ALTER TABLE candidate_pairs RENAME COLUMN superset_ticker TO consequent_ticker",
    "ALTER TABLE candidate_pairs RENAME COLUMN necessary_ticker TO antecedent_ticker",
    "ALTER TABLE candidate_pairs RENAME COLUMN sufficient_ticker TO consequent_ticker",
    "ALTER TABLE treasury_yields ADD COLUMN m1h REAL",
]

_TABLE_REBUILDS = [
    # Add 'need_more_info' to confidence CHECK constraint
    (
        "candidate_pairs",
        "need_more_info_confidence",
        """\
CREATE TABLE candidate_pairs_new (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker_a        TEXT NOT NULL REFERENCES tickers(ticker),
    ticker_b        TEXT NOT NULL REFERENCES tickers(ticker),
    antecedent_ticker  TEXT,
    consequent_ticker TEXT,
    confidence      TEXT CHECK(confidence IN ('high','medium','low','none','need_more_info')),
    reasoning       TEXT,
    llm_model       TEXT,
    screened_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    human_review    TEXT CHECK(human_review IN ('confirmed','rejected') OR human_review IS NULL),
    reviewed_at     TEXT,
    UNIQUE(ticker_a, ticker_b)
)""",
    ),
]


def _run_migrations(conn: sqlite3.Connection) -> None:
    """Apply column renames and table rebuilds to existing DBs. Safe to re-run."""
    for sql in _MIGRATIONS:
        try:
            conn.execute(sql)
        except sqlite3.OperationalError:
            pass  # already renamed or table doesn't exist yet

    # Table rebuilds for CHECK constraint changes
    # Temporarily disable foreign keys so DROP TABLE succeeds when other tables reference it
    fk_state = conn.execute("PRAGMA foreign_keys").fetchone()
    conn.execute("PRAGMA foreign_keys=OFF")
    for table, migration_name, create_sql in _TABLE_REBUILDS:
        # Check if table exists at all
        exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone()
        if not exists:
            continue  # will be created by SCHEMA_SQL

        # Check if already migrated by looking for 'need_more_info' in the schema
        schema = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone()
        if schema and "need_more_info" in schema[0]:
            continue  # already rebuilt

        new_table = f"{table}_new"
        conn.execute(create_sql)
        conn.execute(f"INSERT INTO {new_table} SELECT * FROM {table}")
        conn.execute(f"DROP TABLE {table}")
        conn.execute(f"ALTER TABLE {new_table} RENAME TO {table}")
        conn.commit()
    if fk_state and fk_state[0]:
        conn.execute("PRAGMA foreign_keys=ON")


def init_db(db_path: str) -> None:
    """Create tables. Idempotent."""
    conn = sqlite3.connect(db_path) if db_path != ":memory:" else sqlite3.connect(":memory:")
    _run_migrations(conn)
    conn.executescript(SCHEMA_SQL)
    conn.close()


def get_connection(db_path: str = "slonk_arb.db") -> sqlite3.Connection:
    """REPL-friendly connection helper. Sets WAL mode, foreign keys, Row factory."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    _run_migrations(conn)
    conn.executescript(SCHEMA_SQL)
    conn.executemany(
        "INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)",
        [("buffer_bps", "50"), ("borrow_rate_bps", "400")],
    )
    conn.commit()
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
                    m.get("no_ask_dollars"), int(float(m.get("volume") or m.get("volume_fp") or 0)),
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
                    m.get("no_ask_dollars"), int(float(m.get("volume") or m.get("volume_fp") or 0)),
                    now, now,
                ),
            )
            new += 1
    conn.commit()
    return new, updated


def deactivate_missing_tickers(conn: sqlite3.Connection, active_tickers: set[str]) -> int:
    """Mark tickers NOT in the active set as is_active=0. Returns count deactivated."""
    if not active_tickers:
        return 0
    placeholders = ",".join("?" for _ in active_tickers)
    cur = conn.execute(
        f"UPDATE tickers SET is_active = 0 WHERE is_active = 1 AND ticker NOT IN ({placeholders})",
        list(active_tickers),
    )
    conn.commit()
    return cur.rowcount


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


def get_tickers_by_entity(conn: sqlite3.Connection, min_volume: int = 0) -> dict[str, list[dict]]:
    """Group active tickers by yes_sub_title where entity spans 2+ series.

    Returns dict mapping entity -> list of market dicts (matching the format
    used by generate_candidate_pairs).
    """
    rows = conn.execute(
        """SELECT ticker, series_ticker, event_ticker, title, yes_sub_title,
                  rules_primary, expected_expiration_time, close_time,
                  last_price_dollars, yes_ask_dollars, no_ask_dollars, volume
           FROM tickers WHERE is_active = 1 AND yes_sub_title != ''
                  AND volume >= ?""",
        (min_volume,),
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

    Each result dict must have ticker_a, ticker_b, and may have antecedent_ticker,
    consequent_ticker, confidence, reasoning. Returns count of rows upserted.
    """
    count = 0
    now = _now_utc()
    for r in results:
        ta, tb = _sorted_pair(r["ticker_a"], r["ticker_b"])
        conn.execute(
            """INSERT INTO candidate_pairs
                (ticker_a, ticker_b, antecedent_ticker, consequent_ticker,
                 confidence, reasoning, llm_model, screened_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(ticker_a, ticker_b) DO UPDATE SET
                antecedent_ticker = excluded.antecedent_ticker,
                consequent_ticker = excluded.consequent_ticker,
                confidence = excluded.confidence,
                reasoning = excluded.reasoning,
                llm_model = excluded.llm_model,
                screened_at = excluded.screened_at""",
            (
                ta, tb,
                r.get("antecedent_ticker"), r.get("consequent_ticker"),
                r.get("confidence"), r.get("reasoning"),
                model, now,
            ),
        )
        count += 1
    conn.commit()
    return count


def _compute_yield(
    cost: float | None,
    antecedent_expiration: str | None,
) -> tuple[float | None, int | None]:
    """Effective annual yield and days to maturity for an arb pair.

    Settlement date is the antecedent expiration. By that date, the arb is
    guaranteed to resolve: either the antecedent is NO (NO leg pays $1) or
    YES (implication fires, YES leg is determined and settles).
    Returns (yield, days) or (None, None).
    """
    if cost is None or cost <= 0:
        return None, None
    if not antecedent_expiration:
        return None, None
    try:
        settlement = datetime.fromisoformat(antecedent_expiration.replace("Z", "+00:00")).date()
    except (ValueError, AttributeError):
        return None, None
    days = (settlement - date.today()).days
    if days <= 0:
        return None, None
    ann_yield = (1.0 / cost) ** (365.0 / days) - 1.0
    return ann_yield, days


_CONF_ORDER = {"high": 0, "medium": 1, "low": 2, "need_more_info": 3, "none": 4}

# Treasury tenor name -> approximate days
_TENORS = [
    ("m1", 30), ("m1h", 45), ("m2", 60), ("m3", 91), ("m4", 122), ("m6", 182),
    ("y1", 365), ("y2", 730), ("y3", 1095), ("y5", 1825), ("y7", 2555),
    ("y10", 3650), ("y20", 7300), ("y30", 10950),
]


# ---------------------------------------------------------------------------
# Settings helpers
# ---------------------------------------------------------------------------

def get_setting(conn: sqlite3.Connection, key: str, default: str | None = None) -> str | None:
    """Single setting lookup."""
    row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else default


def set_setting(conn: sqlite3.Connection, key: str, value: str) -> None:
    """Upsert a setting with updated_at timestamp."""
    conn.execute(
        """INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at""",
        (key, value, _now_utc()),
    )
    conn.commit()


def get_all_settings(conn: sqlite3.Connection) -> dict[str, str]:
    """All settings as dict."""
    rows = conn.execute("SELECT key, value FROM settings").fetchall()
    return {r["key"]: r["value"] for r in rows}


# ---------------------------------------------------------------------------
# Treasury yield helpers
# ---------------------------------------------------------------------------

def upsert_treasury_yields(conn: sqlite3.Connection, rows: list[dict]) -> int:
    """Upsert parsed Treasury CMT yield rows. Returns count."""
    count = 0
    for r in rows:
        conn.execute(
            """INSERT INTO treasury_yields (date, m1, m1h, m2, m3, m4, m6, y1, y2, y3, y5, y7, y10, y20, y30)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(date) DO UPDATE SET
                m1=excluded.m1, m1h=excluded.m1h, m2=excluded.m2, m3=excluded.m3, m4=excluded.m4, m6=excluded.m6,
                y1=excluded.y1, y2=excluded.y2, y3=excluded.y3, y5=excluded.y5, y7=excluded.y7,
                y10=excluded.y10, y20=excluded.y20, y30=excluded.y30,
                fetched_at=strftime('%Y-%m-%dT%H:%M:%SZ','now')""",
            (
                r["date"], r.get("m1"), r.get("m1h"), r.get("m2"), r.get("m3"), r.get("m4"), r.get("m6"),
                r.get("y1"), r.get("y2"), r.get("y3"), r.get("y5"), r.get("y7"),
                r.get("y10"), r.get("y20"), r.get("y30"),
            ),
        )
        count += 1
    conn.commit()
    return count


def get_latest_yields(conn: sqlite3.Connection) -> dict | None:
    """Most recent row from treasury_yields (by date DESC)."""
    row = conn.execute(
        "SELECT * FROM treasury_yields ORDER BY date DESC LIMIT 1"
    ).fetchone()
    return dict(row) if row else None


def interpolate_treasury_rate(yields: dict, days: int) -> float | None:
    """Linear interpolation between tenor brackets for a given days-to-maturity.

    Returns rate as a percentage (e.g., 4.53 for 4.53%), or None if no data.
    Clamps to nearest tenor if outside range.
    """
    if yields is None or days <= 0:
        return None

    # Build list of (days, rate) pairs, skipping None values
    points = []
    for name, tenor_days in _TENORS:
        rate = yields.get(name)
        if rate is not None:
            points.append((tenor_days, rate))

    if not points:
        return None

    # Clamp to nearest if outside range
    if days <= points[0][0]:
        return points[0][1]
    if days >= points[-1][0]:
        return points[-1][1]

    # Find bracketing tenors and interpolate
    for i in range(len(points) - 1):
        d0, r0 = points[i]
        d1, r1 = points[i + 1]
        if d0 <= days <= d1:
            frac = (days - d0) / (d1 - d0)
            return r0 + frac * (r1 - r0)

    return points[-1][1]


# ---------------------------------------------------------------------------
# Hurdle yield
# ---------------------------------------------------------------------------

def compute_hurdle_yield(conn: sqlite3.Connection, days: int | None) -> float | None:
    """Hurdle yield = max(treasury_rate + buffer, borrow_rate).

    Returns as a decimal (e.g., 0.06 for 6%), or None if days is None/invalid.
    """
    if days is None or days <= 0:
        return None
    buffer = int(get_setting(conn, "buffer_bps", "50")) / 10000
    borrow_rate = int(get_setting(conn, "borrow_rate_bps", "400")) / 10000
    latest = get_latest_yields(conn)
    treasury_rate = interpolate_treasury_rate(latest, days) if latest else None
    if treasury_rate is None:
        return borrow_rate
    return max(treasury_rate / 100 + buffer, borrow_rate)


def get_pairs_for_review(conn: sqlite3.Connection, status: str) -> list[dict]:
    """Fetch pairs for review UI.

    status: "unreviewed" | "confirmed" | "rejected" | "need_more_info" | "high_unreviewed"
    Returns list of dicts with pair + joined ticker info + computed arb_cost,
    sorted by cost ascending then confidence descending.
    """
    if status == "unreviewed":
        where = "cp.human_review IS NULL AND cp.confidence NOT IN ('none','need_more_info') AND cp.antecedent_ticker IS NOT NULL AND cp.consequent_ticker IS NOT NULL"
    elif status == "need_more_info":
        where = "cp.human_review IS NULL AND cp.confidence = 'need_more_info'"
    elif status == "confirmed":
        where = "cp.human_review = 'confirmed'"
    elif status == "rejected":
        where = "cp.human_review = 'rejected'"
    elif status == "high_unreviewed":
        where = "cp.human_review IS NULL AND cp.confidence = 'high' AND cp.antecedent_ticker IS NOT NULL AND cp.consequent_ticker IS NOT NULL"
    else:
        raise ValueError(f"Invalid status: {status}")

    rows = conn.execute(
        f"""SELECT
            cp.id, cp.ticker_a, cp.ticker_b,
            cp.antecedent_ticker, cp.consequent_ticker,
            cp.confidence, cp.reasoning, cp.llm_model,
            cp.screened_at, cp.human_review, cp.reviewed_at,
            ant.title AS antecedent_title,
            ant.yes_ask_dollars AS antecedent_yes_ask,
            ant.no_ask_dollars AS antecedent_no_ask,
            ant.event_ticker AS antecedent_event_ticker,
            ant.series_ticker AS antecedent_series,
            con.title AS consequent_title,
            con.yes_ask_dollars AS consequent_yes_ask,
            con.no_ask_dollars AS consequent_no_ask,
            con.event_ticker AS consequent_event_ticker,
            con.series_ticker AS consequent_series,
            ant.expected_expiration_time AS antecedent_expiration,
            con.expected_expiration_time AS consequent_expiration
        FROM candidate_pairs cp
        LEFT JOIN tickers ant ON ant.ticker = cp.antecedent_ticker
        LEFT JOIN tickers con ON con.ticker = cp.consequent_ticker
        WHERE {where}""",
    ).fetchall()

    result = []
    for r in rows:
        d = dict(r)
        # Compute arb cost: buy NO on antecedent + YES on consequent
        ant_no = d.get("antecedent_no_ask")
        con_yes = d.get("consequent_yes_ask")
        if ant_no is not None and con_yes is not None:
            try:
                d["arb_cost"] = round(float(ant_no) + float(con_yes), 4)
            except (ValueError, TypeError):
                d["arb_cost"] = None
        else:
            d["arb_cost"] = None
        d["annualized_yield"], d["days_to_maturity"] = _compute_yield(
            d["arb_cost"], d.get("antecedent_expiration"),
        )
        d["hurdle_yield"] = compute_hurdle_yield(conn, d["days_to_maturity"])
        if d["annualized_yield"] is not None and d["hurdle_yield"] is not None:
            d["excess_yield"] = d["annualized_yield"] - d["hurdle_yield"]
        else:
            d["excess_yield"] = None
        result.append(d)

    result.sort(key=lambda d: (
        d["excess_yield"] is None,
        -(d["excess_yield"] if d["excess_yield"] is not None else 0),
        _CONF_ORDER.get(d.get("confidence", ""), 4),
    ))
    return result


def get_pair_detail(conn: sqlite3.Connection, pair_id: int) -> dict | None:
    """Full info for a single pair, including both markets."""
    row = conn.execute(
        """SELECT
            cp.*,
            ant.title AS antecedent_title, ant.yes_sub_title AS antecedent_entity,
            ant.series_ticker AS antecedent_series, ant.event_ticker AS antecedent_event_ticker,
            ant.rules_primary AS antecedent_rules,
            ant.expected_expiration_time AS antecedent_expiration,
            ant.last_price_dollars AS antecedent_last_price,
            ant.yes_ask_dollars AS antecedent_yes_ask,
            ant.no_ask_dollars AS antecedent_no_ask,
            ant.volume AS antecedent_volume,
            con.title AS consequent_title, con.yes_sub_title AS consequent_entity,
            con.series_ticker AS consequent_series, con.event_ticker AS consequent_event_ticker,
            con.rules_primary AS consequent_rules,
            con.expected_expiration_time AS consequent_expiration,
            con.last_price_dollars AS consequent_last_price,
            con.yes_ask_dollars AS consequent_yes_ask,
            con.no_ask_dollars AS consequent_no_ask,
            con.volume AS consequent_volume
        FROM candidate_pairs cp
        LEFT JOIN tickers ant ON ant.ticker = cp.antecedent_ticker
        LEFT JOIN tickers con ON con.ticker = cp.consequent_ticker
        WHERE cp.id = ?""",
        (pair_id,),
    ).fetchone()

    if not row:
        return None

    d = dict(row)
    ant_no = d.get("antecedent_no_ask")
    con_yes = d.get("consequent_yes_ask")
    if ant_no is not None and con_yes is not None:
        try:
            d["arb_cost"] = round(float(ant_no) + float(con_yes), 4)
        except (ValueError, TypeError):
            d["arb_cost"] = None
    else:
        d["arb_cost"] = None
    d["annualized_yield"], d["days_to_maturity"] = _compute_yield(
        d["arb_cost"], d.get("antecedent_expiration"),
    )
    d["hurdle_yield"] = compute_hurdle_yield(conn, d["days_to_maturity"])
    if d["annualized_yield"] is not None and d["hurdle_yield"] is not None:
        d["excess_yield"] = d["annualized_yield"] - d["hurdle_yield"]
    else:
        d["excess_yield"] = None
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


def reverse_and_confirm(conn: sqlite3.Connection, pair_id: int) -> None:
    """Swap antecedent/consequent and mark pair as confirmed."""
    conn.execute(
        """UPDATE candidate_pairs
           SET antecedent_ticker = consequent_ticker,
               consequent_ticker = antecedent_ticker,
               human_review = 'confirmed',
               reviewed_at = ?
           WHERE id = ?""",
        (_now_utc(), pair_id),
    )
    conn.commit()


def get_pair_stats(conn: sqlite3.Connection) -> dict:
    """Return counts for dashboard."""
    row = conn.execute(
        """SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN confidence NOT IN ('none','need_more_info') AND human_review IS NULL THEN 1 ELSE 0 END) AS unreviewed,
            SUM(CASE WHEN human_review = 'confirmed' THEN 1 ELSE 0 END) AS confirmed,
            SUM(CASE WHEN human_review = 'rejected' THEN 1 ELSE 0 END) AS rejected,
            SUM(CASE WHEN confidence = 'none' THEN 1 ELSE 0 END) AS no_relationship,
            SUM(CASE WHEN confidence = 'need_more_info' AND human_review IS NULL THEN 1 ELSE 0 END) AS need_more_info
        FROM candidate_pairs"""
    ).fetchone()
    return dict(row)


# ---------------------------------------------------------------------------
# Trade evaluation helpers
# ---------------------------------------------------------------------------

def insert_trade_evaluation(conn: sqlite3.Connection, eval_dict: dict) -> int:
    """Insert a single trade evaluation row. Returns the new row ID."""
    now = _now_utc()
    cur = conn.execute(
        """INSERT INTO trade_evaluations
            (pair_id, evaluated_at, recommendation, n_contracts,
             cost_per_pair, total_cost, ant_leg_cost, ant_leg_fees,
             con_leg_cost, con_leg_fees, annualized_yield, hurdle_yield,
             excess_yield, days_to_maturity, max_fillable,
             tob_ant_no_ask, tob_con_yes_ask, tob_cost,
             ant_fills_json, con_fills_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            eval_dict["pair_id"], now, eval_dict["recommendation"],
            eval_dict.get("n_contracts", 0),
            eval_dict.get("cost_per_pair"), eval_dict.get("total_cost"),
            eval_dict.get("ant_leg_cost"), eval_dict.get("ant_leg_fees"),
            eval_dict.get("con_leg_cost"), eval_dict.get("con_leg_fees"),
            eval_dict.get("annualized_yield"), eval_dict.get("hurdle_yield"),
            eval_dict.get("excess_yield"), eval_dict.get("days_to_maturity"),
            eval_dict.get("max_fillable"),
            eval_dict.get("tob_ant_no_ask"), eval_dict.get("tob_con_yes_ask"),
            eval_dict.get("tob_cost"),
            json.dumps(eval_dict.get("ant_fills")) if eval_dict.get("ant_fills") else None,
            json.dumps(eval_dict.get("con_fills")) if eval_dict.get("con_fills") else None,
        ),
    )
    conn.commit()
    return cur.lastrowid


def get_recent_evaluations(conn: sqlite3.Connection, days: int = 2) -> list[dict]:
    """All evaluations from the last N days, joined with pair/ticker info."""
    rows = conn.execute(
        """SELECT te.*, cp.antecedent_ticker, cp.consequent_ticker,
                  cp.confidence, cp.reasoning,
                  ant.title AS antecedent_title,
                  ant.event_ticker AS antecedent_event_ticker,
                  ant.series_ticker AS antecedent_series,
                  con.title AS consequent_title,
                  con.event_ticker AS consequent_event_ticker,
                  con.series_ticker AS consequent_series
           FROM trade_evaluations te
           INNER JOIN candidate_pairs cp ON cp.id = te.pair_id
           LEFT JOIN tickers ant ON ant.ticker = cp.antecedent_ticker
           LEFT JOIN tickers con ON con.ticker = cp.consequent_ticker
           WHERE te.evaluated_at >= strftime('%Y-%m-%dT%H:%M:%SZ', 'now', ?)
           ORDER BY te.evaluated_at DESC""",
        (f"-{days} days",),
    ).fetchall()
    return [dict(r) for r in rows]


def get_latest_evaluations(conn: sqlite3.Connection) -> list[dict]:
    """Latest evaluation per pair (only 'buy' recommendations), joined with pair/ticker info."""
    rows = conn.execute(
        """SELECT te.*, cp.antecedent_ticker, cp.consequent_ticker,
                  cp.confidence, cp.reasoning,
                  ant.title AS antecedent_title,
                  ant.event_ticker AS antecedent_event_ticker,
                  ant.series_ticker AS antecedent_series,
                  con.title AS consequent_title,
                  con.event_ticker AS consequent_event_ticker,
                  con.series_ticker AS consequent_series
           FROM trade_evaluations te
           INNER JOIN (
               SELECT pair_id, MAX(evaluated_at) AS max_eval
               FROM trade_evaluations
               GROUP BY pair_id
           ) latest ON te.pair_id = latest.pair_id AND te.evaluated_at = latest.max_eval
           INNER JOIN candidate_pairs cp ON cp.id = te.pair_id
           LEFT JOIN tickers ant ON ant.ticker = cp.antecedent_ticker
           LEFT JOIN tickers con ON con.ticker = cp.consequent_ticker
           WHERE te.recommendation = 'buy'
           ORDER BY te.excess_yield DESC"""
    ).fetchall()
    return [dict(r) for r in rows]


