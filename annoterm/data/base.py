"""Base interfaces for data adapters."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Sequence

from annoterm.filters.parser import FilterQuery
from annoterm.models import ColumnInfo, DatasetMeta, RowRecord
from annoterm.models import SortSpec


class DataAdapter(ABC):
    """Abstract adapter that normalizes row access for each source type."""

    source_type: str = "unknown"

    def __init__(
        self,
        source_uri: str,
        split: str | None = None,
        row_id_field: str | None = None,
        key_fields: Sequence[str] = (),
    ) -> None:
        self.source_uri = source_uri
        self.split = split
        self.row_id_field = row_id_field
        self.key_fields = tuple(key_fields)

    @abstractmethod
    def schema(self) -> list[ColumnInfo]:
        """Return the full schema."""

    @abstractmethod
    def row_count(self, filter_query: FilterQuery | None = None) -> int | None:
        """Return total row count when known."""

    @abstractmethod
    def rows(
        self,
        offset: int,
        limit: int,
        visible_columns: Sequence[str] | None = None,
        filter_query: FilterQuery | None = None,
        sort: SortSpec | None = None,
    ) -> list[RowRecord]:
        """Return rows in the half-open range [offset, offset + limit)."""

    @abstractmethod
    def fingerprint(self) -> str:
        """Return a stable fingerprint for this dataset snapshot."""

    def meta(self) -> DatasetMeta:
        """Assemble common metadata from adapter methods."""

        return DatasetMeta(
            source_type=self.source_type,
            source_uri=self.source_uri,
            split=self.split,
            fingerprint=self.fingerprint(),
            row_count=self.row_count(),
            row_id_field=self.row_id_field,
            key_fields=self.key_fields,
        )
