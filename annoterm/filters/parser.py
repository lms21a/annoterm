"""Filter expression parser for the TUI command bar."""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass
from typing import Any, Literal


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

_CONDITION_COLUMN_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_\.]*$")
@dataclass(frozen=True)
class FilterCondition:
    """Represents a single `<column> <op> <value>` clause."""

    column: str
    operator: str
    value: Any


@dataclass(frozen=True)
class FilterLogical:
    """Logical expression between two filter expressions."""

    operator: Literal["and", "or"]
    left: FilterExpression
    right: FilterExpression


FilterExpression = FilterCondition | FilterLogical


@dataclass(frozen=True)
class FilterQuery:
    expression: FilterExpression
    raw: str

    @property
    def conditions(self) -> tuple[FilterCondition, ...]:
        return tuple(_collect_conditions(self.expression))

    def cache_key(self) -> str:
        return self.raw.strip()


def parse_filter_expression(expression: str | None) -> FilterQuery | None:
    if expression is None:
        return None
    text = expression.strip()
    if not text:
        return None

    tokens = _tokenize(text)
    if not tokens:
        return None
    query = _FilterParser(tokens).parse()
    return FilterQuery(expression=query, raw=text)


def _collect_conditions(expression: FilterExpression) -> list[FilterCondition]:
    if isinstance(expression, FilterCondition):
        return [expression]
    return [
        *_collect_conditions(expression.left),
        *_collect_conditions(expression.right),
    ]


def _tokenize(text: str) -> list[str]:
    tokens: list[str] = []
    buffer: list[str] = []
    quote: str | None = None
    index = 0

    def flush_buffer() -> None:
        if buffer:
            tokens.append("".join(buffer))
            buffer.clear()

    while index < len(text):
        char = text[index]

        if quote is not None:
            buffer.append(char)
            if char == "\\" and index + 1 < len(text):
                index += 1
                buffer.append(text[index])
            elif char == quote:
                quote = None
            index += 1
            continue

        if char in {"'", '"'}:
            quote = char
            buffer.append(char)
            index += 1
            continue

        if char.isspace():
            flush_buffer()
            index += 1
            continue

        if text[index : index + 2] in {"&&", "||", "==", ">=", "<=", "!="}:
            flush_buffer()
            tokens.append(text[index : index + 2])
            index += 2
            continue

        if char in {"(", ")", "<", ">", "="}:
            flush_buffer()
            tokens.append(char)
            index += 1
            continue

        buffer.append(char)
        index += 1

    flush_buffer()
    return tokens


class _FilterParser:
    def __init__(self, tokens: list[str]) -> None:
        self._tokens = tokens
        self._index = 0

    def parse(self) -> FilterExpression:
        expression = self._parse_or()
        if not self._is_end():
            raise ValueError(self._invalid_error())
        return expression

    def _parse_or(self) -> FilterExpression:
        expression = self._parse_and()
        while self._match("or", "||"):
            expression = FilterLogical(
                operator="or",
                left=expression,
                right=self._parse_and(),
            )
        return expression

    def _parse_and(self) -> FilterExpression:
        expression = self._parse_atom()
        while self._match("and", "&&"):
            expression = FilterLogical(
                operator="and",
                left=expression,
                right=self._parse_atom(),
            )
        return expression

    def _parse_atom(self) -> FilterExpression:
        if self._match("("):
            expression = self._parse_or()
            if not self._match(")"):
                raise ValueError(self._invalid_error())
            return expression
        return self._parse_condition()

    def _parse_condition(self) -> FilterCondition:
        column = self._consume_identifier()
        operator_token = self._consume_token()
        operator = SUPPORTED_OPERATORS.get(operator_token.lower())
        if operator is None:
            raise ValueError(self._invalid_error())

        value_token = self._consume_token(allow_none=False)
        value = _parse_literal(value_token)

        return FilterCondition(column=column, operator=operator, value=value)

    def _consume_identifier(self) -> str:
        column = self._consume_token()
        if not _CONDITION_COLUMN_PATTERN.match(column):
            raise ValueError(self._invalid_error())
        return column

    def _consume_token(self, *, allow_none: bool = True) -> str:
        if self._is_end():
            if allow_none:
                return ""
            raise ValueError(self._invalid_error())
        token = self._tokens[self._index]
        self._index += 1
        return token

    def _match(self, *token_values: str) -> bool:
        if self._is_end():
            return False
        current = self._tokens[self._index]
        if current.lower() in {token.lower() for token in token_values}:
            self._index += 1
            return True
        return False

    def _is_end(self) -> bool:
        return self._index >= len(self._tokens)

    def _invalid_error(self) -> str:
        return (
            "Invalid filter clause. Expected format like `column == value` or "
            "`column contains value`."
        )


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
