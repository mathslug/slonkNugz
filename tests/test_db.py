"""Tests for db.py — SQLite persistence with in-memory database."""

import pytest

import db


@pytest.fixture
def conn():
    c = db.get_connection(":memory:")
    yield c
    c.close()


def _make_market(**overrides):
    """Helper to create a minimal market dict."""
    defaults = {
        "ticker": "TICK-A",
        "series_ticker": "SERIES-1",
        "event_ticker": "EVENT-1",
        "title": "Test Market",
        "yes_sub_title": "Entity A",
        "rules_primary": "Some rules",
        "expected_expiration_time": "2026-06-01T00:00:00Z",
        "close_time": "2026-06-01T00:00:00Z",
        "last_price_dollars": "0.50",
        "yes_ask_dollars": "0.55",
        "no_ask_dollars": "0.48",
        "volume": 500,
        "sport_tag": "Tennis",
        "sub_sport": "Tennis",
    }
    defaults.update(overrides)
    return defaults


# ── upsert_tickers ───────────────────────────────────────────────────────────


def test_upsert_tickers_insert(conn):
    markets = [_make_market(ticker="T1"), _make_market(ticker="T2")]
    new, updated = db.upsert_tickers(conn, markets)
    assert new == 2
    assert updated == 0


def test_upsert_tickers_update(conn):
    db.upsert_tickers(conn, [_make_market(ticker="T1", volume=100)])
    new, updated = db.upsert_tickers(conn, [_make_market(ticker="T1", volume=999)])
    assert new == 0
    assert updated == 1
    row = conn.execute("SELECT volume FROM tickers WHERE ticker = 'T1'").fetchone()
    assert row["volume"] == 999


# ── record_prices ────────────────────────────────────────────────────────────


def test_record_prices(conn):
    db.upsert_tickers(conn, [_make_market(ticker="T1")])
    count = db.record_prices(conn, [_make_market(ticker="T1")])
    assert count == 1
    rows = conn.execute("SELECT * FROM prices WHERE ticker = 'T1'").fetchall()
    assert len(rows) == 1

    # Record again — should append
    db.record_prices(conn, [_make_market(ticker="T1", last_price_dollars="0.60")])
    rows = conn.execute("SELECT * FROM prices WHERE ticker = 'T1'").fetchall()
    assert len(rows) == 2


# ── deactivate_missing_tickers ───────────────────────────────────────────────


def test_deactivate_missing_tickers(conn):
    db.upsert_tickers(conn, [
        _make_market(ticker="T1"),
        _make_market(ticker="T2"),
        _make_market(ticker="T3"),
    ])
    deactivated = db.deactivate_missing_tickers(conn, {"T1", "T3"})
    assert deactivated == 1
    row = conn.execute("SELECT is_active FROM tickers WHERE ticker = 'T2'").fetchone()
    assert row["is_active"] == 0


def test_deactivate_empty_set(conn):
    db.upsert_tickers(conn, [_make_market(ticker="T1")])
    assert db.deactivate_missing_tickers(conn, set()) == 0


# ── get_tickers_by_entity ────────────────────────────────────────────────────


def test_get_tickers_by_entity_groups(conn):
    # Two tickers with same entity in different series -> grouped
    db.upsert_tickers(conn, [
        _make_market(ticker="T1", series_ticker="S1", event_ticker="E1", yes_sub_title="Alcaraz"),
        _make_market(ticker="T2", series_ticker="S2", event_ticker="E2", yes_sub_title="Alcaraz"),
    ])
    groups = db.get_tickers_by_entity(conn)
    assert "Alcaraz" in groups
    assert len(groups["Alcaraz"]) == 2


def test_get_tickers_by_entity_requires_two_series(conn):
    # Same series -> not grouped
    db.upsert_tickers(conn, [
        _make_market(ticker="T1", series_ticker="S1", yes_sub_title="Alcaraz"),
        _make_market(ticker="T2", series_ticker="S1", yes_sub_title="Alcaraz"),
    ])
    groups = db.get_tickers_by_entity(conn)
    assert "Alcaraz" not in groups


def test_get_tickers_by_entity_min_volume(conn):
    db.upsert_tickers(conn, [
        _make_market(ticker="T1", series_ticker="S1", yes_sub_title="Alcaraz", volume=50),
        _make_market(ticker="T2", series_ticker="S2", yes_sub_title="Alcaraz", volume=50),
    ])
    groups = db.get_tickers_by_entity(conn, min_volume=100)
    assert "Alcaraz" not in groups


# ── bulk_upsert_pair_results + get_screened_pair_keys ────────────────────────


def test_bulk_upsert_and_screened_keys(conn):
    db.upsert_tickers(conn, [_make_market(ticker="A"), _make_market(ticker="B")])
    results = [{
        "ticker_a": "A",
        "ticker_b": "B",
        "antecedent_ticker": "A",
        "consequent_ticker": "B",
        "confidence": "high",
        "reasoning": "A implies B",
    }]
    count = db.bulk_upsert_pair_results(conn, results, "test-model")
    assert count == 1

    screened = db.get_screened_pair_keys(conn)
    assert ("A", "B") in screened


def test_bulk_upsert_sorted_order(conn):
    db.upsert_tickers(conn, [_make_market(ticker="A"), _make_market(ticker="Z")])
    results = [{"ticker_a": "Z", "ticker_b": "A", "confidence": "none"}]
    db.bulk_upsert_pair_results(conn, results, "test-model")
    screened = db.get_screened_pair_keys(conn)
    assert ("A", "Z") in screened  # stored in sorted order


# ── get_pairs_for_review ─────────────────────────────────────────────────────


def _seed_pair(conn, ticker_a="A", ticker_b="B", confidence="high", human_review=None):
    """Insert tickers and a candidate pair, return pair id."""
    db.upsert_tickers(conn, [
        _make_market(ticker=ticker_a, series_ticker="S1"),
        _make_market(ticker=ticker_b, series_ticker="S2"),
    ])
    db.bulk_upsert_pair_results(conn, [{
        "ticker_a": ticker_a,
        "ticker_b": ticker_b,
        "antecedent_ticker": ticker_a,
        "consequent_ticker": ticker_b,
        "confidence": confidence,
        "reasoning": "test",
    }], "test-model")
    if human_review:
        pair_id = conn.execute(
            "SELECT id FROM candidate_pairs ORDER BY id DESC LIMIT 1"
        ).fetchone()["id"]
        db.set_review(conn, pair_id, human_review)
    return conn.execute(
        "SELECT id FROM candidate_pairs ORDER BY id DESC LIMIT 1"
    ).fetchone()["id"]


def test_get_pairs_unreviewed(conn):
    _seed_pair(conn, "A", "B", "high")
    pairs = db.get_pairs_for_review(conn, "unreviewed")
    assert len(pairs) == 1


def test_get_pairs_confirmed(conn):
    _seed_pair(conn, "A", "B", "high", "confirmed")
    assert len(db.get_pairs_for_review(conn, "confirmed")) == 1
    assert len(db.get_pairs_for_review(conn, "unreviewed")) == 0


def test_get_pairs_rejected(conn):
    _seed_pair(conn, "A", "B", "high", "rejected")
    assert len(db.get_pairs_for_review(conn, "rejected")) == 1


# ── set_review + reverse_and_confirm ─────────────────────────────────────────


def test_set_review(conn):
    pair_id = _seed_pair(conn, "A", "B", "high")
    db.set_review(conn, pair_id, "confirmed")
    row = conn.execute("SELECT human_review FROM candidate_pairs WHERE id = ?", (pair_id,)).fetchone()
    assert row["human_review"] == "confirmed"


def test_set_review_invalid(conn):
    pair_id = _seed_pair(conn, "A", "B", "high")
    with pytest.raises(ValueError):
        db.set_review(conn, pair_id, "maybe")


def test_reverse_and_confirm(conn):
    pair_id = _seed_pair(conn, "A", "B", "high")
    db.reverse_and_confirm(conn, pair_id)
    row = conn.execute(
        "SELECT antecedent_ticker, consequent_ticker, human_review FROM candidate_pairs WHERE id = ?",
        (pair_id,),
    ).fetchone()
    assert row["antecedent_ticker"] == "B"
    assert row["consequent_ticker"] == "A"
    assert row["human_review"] == "confirmed"


# ── get_pair_stats ───────────────────────────────────────────────────────────


def test_get_pair_stats(conn):
    _seed_pair(conn, "A", "B", "high")
    _seed_pair(conn, "C", "D", "none")
    stats = db.get_pair_stats(conn)
    assert stats["total"] == 2
    assert stats["unreviewed"] == 1  # 'none' excluded
    assert stats["no_relationship"] == 1


# ── settings ─────────────────────────────────────────────────────────────────


def test_get_set_setting(conn):
    db.set_setting(conn, "foo", "bar")
    assert db.get_setting(conn, "foo") == "bar"


def test_get_setting_default(conn):
    assert db.get_setting(conn, "nonexistent", "fallback") == "fallback"


def test_settings_default_values(conn):
    # get_connection seeds buffer_bps and borrow_rate_bps
    assert db.get_setting(conn, "buffer_bps") == "50"
    assert db.get_setting(conn, "borrow_rate_bps") == "400"


# ── treasury yields ──────────────────────────────────────────────────────────


def test_upsert_treasury_yields(conn):
    rows = [{"date": "2026-03-15", "m1": 4.0, "m3": 4.2, "y1": 4.5}]
    count = db.upsert_treasury_yields(conn, rows)
    assert count == 1
    latest = db.get_latest_yields(conn)
    assert latest["date"] == "2026-03-15"
    assert latest["m1"] == 4.0


def test_upsert_treasury_yields_update(conn):
    db.upsert_treasury_yields(conn, [{"date": "2026-03-15", "m1": 4.0}])
    db.upsert_treasury_yields(conn, [{"date": "2026-03-15", "m1": 4.5}])
    latest = db.get_latest_yields(conn)
    assert latest["m1"] == 4.5


# ── interpolate_treasury_rate ────────────────────────────────────────────────


def test_interpolate_exact_tenor():
    yields = {"m3": 4.5}
    assert db.interpolate_treasury_rate(yields, 91) == 4.5


def test_interpolate_between_tenors():
    yields = {"m3": 4.0, "m6": 5.0}
    # 91 days (m3) to 182 days (m6), midpoint at ~136 days
    rate = db.interpolate_treasury_rate(yields, 136)
    assert rate is not None
    assert 4.0 < rate < 5.0


def test_interpolate_clamp_below():
    yields = {"m3": 4.5, "y1": 5.0}
    assert db.interpolate_treasury_rate(yields, 10) == 4.5


def test_interpolate_clamp_above():
    yields = {"m3": 4.5, "y1": 5.0}
    assert db.interpolate_treasury_rate(yields, 9999) == 5.0


def test_interpolate_none_yields():
    assert db.interpolate_treasury_rate(None, 90) is None


def test_interpolate_zero_days():
    assert db.interpolate_treasury_rate({"m3": 4.5}, 0) is None


def test_interpolate_no_data():
    assert db.interpolate_treasury_rate({}, 90) is None


# ── compute_hurdle_yield ─────────────────────────────────────────────────────


def test_compute_hurdle_yield(conn):
    # With default settings: buffer=50bps, borrow=400bps (4%)
    # No treasury data -> falls back to borrow rate
    hurdle = db.compute_hurdle_yield(conn, 90)
    assert hurdle == 0.04  # borrow_rate_bps=400 -> 4%


def test_compute_hurdle_yield_with_treasury(conn):
    db.upsert_treasury_yields(conn, [{"date": "2026-03-15", "m3": 4.0}])
    # treasury_rate=4.0% -> 0.04 + buffer(0.005) = 0.045
    # max(0.045, 0.04) = 0.045
    hurdle = db.compute_hurdle_yield(conn, 91)
    assert hurdle == 0.045


def test_compute_hurdle_yield_none_days(conn):
    assert db.compute_hurdle_yield(conn, None) is None


def test_compute_hurdle_yield_zero_days(conn):
    assert db.compute_hurdle_yield(conn, 0) is None
