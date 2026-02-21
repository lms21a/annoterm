from __future__ import annotations

from annoterm.filters.eval import row_matches_filter
from annoterm.filters.parser import parse_filter_expression


def test_parse_filter_expression_supports_multiple_and_conditions() -> None:
    query = parse_filter_expression("score >= 0.5 and label == 'good'")
    assert query is not None
    assert len(query.conditions) == 2
    assert query.conditions[0].column == "score"
    assert query.conditions[0].operator == ">="
    assert query.conditions[0].value == 0.5
    assert query.conditions[1].column == "label"
    assert query.conditions[1].operator == "=="
    assert query.conditions[1].value == "good"


def test_row_matches_filter_handles_numeric_and_string_ops() -> None:
    query = parse_filter_expression("score > 0.5 and text contains 'hello'")
    assert query is not None

    assert row_matches_filter({"score": 0.9, "text": "hello world"}, query) is True
    assert row_matches_filter({"score": 0.1, "text": "hello world"}, query) is False
    assert row_matches_filter({"score": 0.9, "text": "world"}, query) is False


def test_parse_filter_expression_supports_symbolic_logical_ops() -> None:
    query = parse_filter_expression("score >= 0.7 && category == 'x' || text contains 'alpha'")
    assert query is not None
    assert len(query.conditions) == 3

    assert row_matches_filter({"score": 0.9, "category": "x", "text": "beta"}, query) is True
    assert row_matches_filter({"score": 0.6, "category": "x", "text": "alpha"}, query) is True
    assert row_matches_filter({"score": 0.6, "category": "y", "text": "alpha"}, query) is True
    assert row_matches_filter({"score": 0.6, "category": "y", "text": "beta"}, query) is False


def test_parse_filter_expression_supports_parentheses_precedence() -> None:
    query = parse_filter_expression(
        "(score > 0.7 and category == 'x') || text contains 'alpha'"
    )
    assert query is not None
    assert len(query.conditions) == 3

    assert (
        row_matches_filter({"score": 0.6, "category": "x", "text": "beta"}, query)
        is False
    )
    assert (
        row_matches_filter({"score": 0.6, "category": "y", "text": "alpha"}, query)
        is True
    )


def test_parse_filter_invalid_clause_raises() -> None:
    try:
        parse_filter_expression("badclause")
    except ValueError as exc:
        assert "Invalid filter clause" in str(exc)
    else:
        raise AssertionError("Expected ValueError for invalid clause")
