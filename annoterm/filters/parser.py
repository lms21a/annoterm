"""Filter expression parser for the TUI command bar."""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass
from typing import Any


SUPPORTED_OPERATORS = {
    "=": "==",
    "==": "==",
    "!=": "!=",
    ">": ">",
    ">=": ">=",
    "<": "<",
    "<=": "<=",
    "contains": "contains",
    "startswith": "startswith",
    "endswith": "endswith",
}

_CONDITION_PATTERN = re.compile(
    r"""^\s*
    (?P<column>[A-Za-z_][A-Za-z0-9_\.]*)
    \s*
    (?P<operator>==|=|!=|>=|<=|>|<|contains|startswith|endswith)
    \s*
    (?P<value>.+?)
    \s*$
    """,
    re.IGNORECASE | re.VERBOSE,
)


@dataclass(frozen=True)
class FilterCondition:
    """Represents a single `<column> <op> <value>` clause."""

    column: str
    operator: str
    value: Any


@dataclass(frozen=True)
class FilterQuery:
    """Conjunction (AND) of filter conditions."""

    conditions: tuple[FilterCondition, ...]
    raw: str

    def cache_key(self) -> str:
        return self.raw.strip()


def parse_filter_expression(expression: str | None) -> FilterQuery | None:
    if expression is None:
        return None
    text = expression.strip()
    if not text:
        return None

    clauses = _split_top_level_and(text)
    conditions: list[FilterCondition] = []
    for clause in clauses:
        match = _CONDITION_PATTERN.match(clause)
        if not match:
            raise ValueError(
                "Invalid filter clause. Expected format like `column == value` or "
                "`column contains value`."
            )

        column = match.group("column")
        operator_raw = match.group("operator").lower()
        operator = SUPPORTED_OPERATORS[operator_raw]
        value_raw = match.group("value")
        value = _parse_literal(value_raw)
        conditions.append(FilterCondition(column=column, operator=operator, value=value))

    return FilterQuery(conditions=tuple(conditions), raw=text)


def _split_top_level_and(text: str) -> list[str]:
    clauses: list[str] = []
    start = 0
    quote: str | None = None
    index = 0
    while index < len(text):
        char = text[index]
        if char in {"'", '"'}:
            if quote is None:
                quote = char
            elif quote == char:
                quote = None
            index += 1
            continue

        if quote is None and text[index : index + 5].lower() == " and ":
            clause = text[start:index].strip()
            if clause:
                clauses.append(clause)
            start = index + 5
            index += 5
            continue
        index += 1

    last_clause = text[start:].strip()
    if last_clause:
        clauses.append(last_clause)
    return clauses


def _parse_literal(token: str) -> Any:
    value = token.strip()
    if not value:
        return ""

    lowered = value.lower()
    if lowered in {"none", "null"}:
        return None
    if lowered == "true":
        return True
    if lowered == "false":
        return False

    if _is_quoted(value):
        try:
            return ast.literal_eval(value)
        except (SyntaxError, ValueError):
            return value[1:-1]

    if _looks_like_int(value):
        try:
            return int(value)
        except ValueError:
            pass

    if _looks_like_float(value):
        try:
            return float(value)
        except ValueError:
            pass

    return value


def _is_quoted(value: str) -> bool:
    return len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}


def _looks_like_int(value: str) -> bool:
    return bool(re.match(r"^[+-]?\d+$", value))


def _looks_like_float(value: str) -> bool:
    return bool(re.match(r"^[+-]?\d+\.\d+$", value))
