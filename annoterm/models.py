"""Shared data models used across adapters and UI."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ColumnInfo:
    """Column metadata for display and schema introspection."""

    name: str
    dtype: str


@dataclass(frozen=True)
class RowRecord:
    """Normalized row representation across all source types."""

    row_index: int
    row_data: dict[str, Any]
    row_id: str | None
    key_fields: dict[str, Any]
    row_hash: str


@dataclass(frozen=True)
class SortSpec:
    """Single-column sort spec used by adapter queries."""

    column: str
    descending: bool = False


@dataclass(frozen=True)
class DatasetMeta:
    """Metadata describing the loaded data source."""

    source_type: str
    source_uri: str
    split: str | None
    fingerprint: str
    row_count: int | None
    row_id_field: str | None
    key_fields: tuple[str, ...]
