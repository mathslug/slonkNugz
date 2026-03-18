"""Tests for scan.py — pair generation, JSON extraction, filter mapping."""

from scan import (
    ENTITY_BLOCKLIST,
    _extract_json,
    _FILTER_TO_API_TAG,
    filter_groups_by_sport,
    format_pair_for_llm,
    generate_candidate_pairs,
)


def _market(ticker, series, event="E1", entity="Player A", sport="Tennis", sub_sport="Tennis"):
    return {
        "ticker": ticker,
        "series_ticker": series,
        "event_ticker": event,
        "title": f"Title {ticker}",
        "rules_primary": "Rules",
        "yes_sub_title": entity,
        "sport_tag": sport,
        "sub_sport": sub_sport,
        "volume": 500,
    }


# ── generate_candidate_pairs ─────────────────────────────────────────────────


def test_cross_series_pairing():
    groups = {
        "Alcaraz": [
            _market("T1", "FO", event="E1"),
            _market("T2", "GS", event="E2"),
        ]
    }
    pairs = generate_candidate_pairs(groups)
    assert len(pairs) == 1
    tickers = {pairs[0][0]["ticker"], pairs[0][1]["ticker"]}
    assert tickers == {"T1", "T2"}


def test_same_series_rejected():
    groups = {
        "Alcaraz": [
            _market("T1", "FO", event="E1"),
            _market("T2", "FO", event="E2"),
        ]
    }
    # Same series, different events — still rejected (same series_ticker)
    pairs = generate_candidate_pairs(groups)
    assert len(pairs) == 0


def test_same_event_rejected():
    groups = {
        "Alcaraz": [
            _market("T1", "S1", event="E1"),
            _market("T2", "S2", event="E1"),
        ]
    }
    # Different series but same event — rejected
    pairs = generate_candidate_pairs(groups)
    assert len(pairs) == 0


def test_cross_sport_rejected():
    groups = {
        "Player": [
            _market("T1", "S1", event="E1", sub_sport="Tennis"),
            _market("T2", "S2", event="E2", sub_sport="Golf"),
        ]
    }
    pairs = generate_candidate_pairs(groups)
    assert len(pairs) == 0


def test_blocklisted_entity():
    for entity in ("Tie", "Yes"):
        assert entity in ENTITY_BLOCKLIST
    groups = {
        "Tie": [
            _market("T1", "S1", event="E1", entity="Tie"),
            _market("T2", "S2", event="E2", entity="Tie"),
        ]
    }
    pairs = generate_candidate_pairs(groups)
    assert len(pairs) == 0


def test_empty_groups():
    assert generate_candidate_pairs({}) == []


def test_multiple_entities():
    groups = {
        "Alcaraz": [
            _market("T1", "S1", event="E1"),
            _market("T2", "S2", event="E2"),
        ],
        "Sinner": [
            _market("T3", "S1", event="E3", entity="Sinner"),
            _market("T4", "S3", event="E4", entity="Sinner"),
        ],
    }
    pairs = generate_candidate_pairs(groups)
    assert len(pairs) == 2


# ── format_pair_for_llm ──────────────────────────────────────────────────────


def test_format_pair_for_llm():
    a = _market("TICK-A", "S1")
    b = _market("TICK-B", "S2")
    text = format_pair_for_llm(1, a, b)
    assert "Pair 1" in text
    assert "TICK-A" in text
    assert "TICK-B" in text
    assert "Event A:" in text
    assert "Event B:" in text


def test_format_pair_truncates_rules():
    a = _market("T1", "S1")
    a["rules_primary"] = "x" * 1000
    b = _market("T2", "S2")
    text = format_pair_for_llm(1, a, b)
    # rules truncated to 500 chars
    assert "x" * 501 not in text


# ── _extract_json ────────────────────────────────────────────────────────────


def test_extract_json_plain():
    text = '{"results": [{"ticker_a": "A", "confidence": "high"}]}'
    result = _extract_json(text)
    assert len(result) == 1
    assert result[0]["ticker_a"] == "A"


def test_extract_json_markdown_fenced():
    text = '```json\n{"results": [{"ticker_a": "A"}]}\n```'
    result = _extract_json(text)
    assert len(result) == 1


def test_extract_json_pairs_key():
    text = '{"pairs": [{"ticker_a": "X"}]}'
    result = _extract_json(text)
    assert result[0]["ticker_a"] == "X"


def test_extract_json_data_key():
    text = '{"data": [{"ticker_a": "X"}]}'
    result = _extract_json(text)
    assert result[0]["ticker_a"] == "X"


def test_extract_json_single_object():
    text = '{"antecedent_ticker": "A", "consequent_ticker": "B"}'
    result = _extract_json(text)
    assert len(result) == 1
    assert result[0]["antecedent_ticker"] == "A"


def test_extract_json_bare_array():
    text = '[{"ticker_a": "A"}, {"ticker_a": "B"}]'
    result = _extract_json(text)
    assert len(result) == 2


def test_extract_json_malformed():
    import pytest
    import json
    with pytest.raises(json.JSONDecodeError):
        _extract_json("not json at all")


# ── _FILTER_TO_API_TAG ───────────────────────────────────────────────────────


def test_filter_tag_pro_football():
    assert _FILTER_TO_API_TAG["pro football"] == "Football"


def test_filter_tag_college_football():
    assert _FILTER_TO_API_TAG["college football"] == "Football"


# ── filter_groups_by_sport ───────────────────────────────────────────────────


def test_filter_drops_non_matching_markets():
    """Filtering 'hockey' on a mixed entity keeps only hockey markets."""
    groups = {
        "Denver": [
            _market("NHL-DEN", "KNHL", event="E1", entity="Denver", sport="Hockey", sub_sport="NHL"),
            _market("NFL-DEN", "KNFL", event="E2", entity="Denver", sport="Football", sub_sport="Pro Football"),
            _market("NFL2-DEN", "KNFLPLAY", event="E3", entity="Denver", sport="Football", sub_sport="Pro Football"),
        ]
    }
    filtered = filter_groups_by_sport(groups, ["hockey"])
    assert "Denver" in filtered
    assert len(filtered["Denver"]) == 1
    assert filtered["Denver"][0]["ticker"] == "NHL-DEN"


def test_filter_drops_entity_with_no_matches():
    groups = {
        "Denver": [
            _market("NFL-DEN", "KNFL", event="E1", entity="Denver", sport="Football", sub_sport="Pro Football"),
        ]
    }
    filtered = filter_groups_by_sport(groups, ["hockey"])
    assert "Denver" not in filtered


def test_filter_mixed_entity_no_cross_sport_pairs():
    """End-to-end: filtering + pair generation produces no cross-sport pairs."""
    groups = {
        "Denver": [
            _market("NHL1-DEN", "KNHL", event="E1", entity="Denver", sport="Hockey", sub_sport="NHL"),
            _market("NHL2-DEN", "KNHLPLAY", event="E2", entity="Denver", sport="Hockey", sub_sport="NHL"),
            _market("NFL1-DEN", "KNFL", event="E3", entity="Denver", sport="Football", sub_sport="Pro Football"),
            _market("NFL2-DEN", "KNFLPLAY", event="E4", entity="Denver", sport="Football", sub_sport="Pro Football"),
        ]
    }
    filtered = filter_groups_by_sport(groups, ["hockey"])
    pairs = generate_candidate_pairs(filtered)
    assert len(pairs) == 1
    tickers = {pairs[0][0]["ticker"], pairs[0][1]["ticker"]}
    assert tickers == {"NHL1-DEN", "NHL2-DEN"}
