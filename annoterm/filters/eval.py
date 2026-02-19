"""Evaluate parsed filter queries in Python rows and Polars."""

from __future__ import annotations

from typing import Any

import polars as pl

from annoterm.filters.parser import FilterCondition, FilterQuery


def row_matches_filter(row_data: dict[str, Any], query: FilterQuery | None) -> bool:
    if query is None:
        return True
    for condition in query.conditions:
        if not _match_condition(row_data.get(condition.column), condition):
            return False
    return True


def to_polars_expression(query: FilterQuery | None) -> pl.Expr | None:
    if query is None:
        return None
    expressions = [_condition_to_expr(condition) for condition in query.conditions]
    if not expressions:
        return None
    expr = expressions[0]
    for next_expr in expressions[1:]:
        expr = expr & next_expr
    return expr


def _condition_to_expr(condition: FilterCondition) -> pl.Expr:
    column_expr = pl.col(condition.column)
    value = condition.value
    op = condition.operator

    if op == "==":
        return column_expr == pl.lit(value)
    if op == "!=":
        return column_expr != pl.lit(value)
    if op == ">":
        return column_expr > pl.lit(value)
    if op == ">=":
        return column_expr >= pl.lit(value)
    if op == "<":
        return column_expr < pl.lit(value)
    if op == "<=":
        return column_expr <= pl.lit(value)
    if op == "contains":
        return (
            column_expr.cast(pl.Utf8)
            .fill_null("")
            .str.contains(str(value), literal=True, strict=False)
        )
    if op == "startswith":
        return column_expr.cast(pl.Utf8).fill_null("").str.starts_with(str(value))
    if op == "endswith":
        return column_expr.cast(pl.Utf8).fill_null("").str.ends_with(str(value))

    raise ValueError(f"Unsupported operator: {op}")


def _match_condition(current_value: Any, condition: FilterCondition) -> bool:
    target = condition.value
    op = condition.operator

    if op == "==":
        return current_value == target
    if op == "!=":
        return current_value != target
    if op in {">", ">=", "<", "<="}:
        left, right = _coerce_comparable(current_value, target)
        if left is None or right is None:
            return False
        if op == ">":
            return left > right
        if op == ">=":
            return left >= right
        if op == "<":
            return left < right
        return left <= right

    text = "" if current_value is None else str(current_value)
    needle = str(target)
    if op == "contains":
        return needle in text
    if op == "startswith":
        return text.startswith(needle)
    if op == "endswith":
        return text.endswith(needle)

    raise ValueError(f"Unsupported operator: {op}")


def _coerce_comparable(left: Any, right: Any) -> tuple[Any | None, Any | None]:
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        return left, right
    try:
        return float(left), float(right)
    except (TypeError, ValueError):
        pass
    if isinstance(left, str) and isinstance(right, str):
        return left, right
    return None, None
