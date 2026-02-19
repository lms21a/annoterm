"""CSV adapter backed by Polars lazy scanning."""

from __future__ import annotations

from pathlib import Path
from typing import Sequence

import polars as pl

from annoterm.data.base import DataAdapter
from annoterm.data.identity import build_row_record, fingerprint_from_path
from annoterm.filters.eval import to_polars_expression
from annoterm.filters.parser import FilterQuery
from annoterm.models import ColumnInfo, RowRecord
from annoterm.models import SortSpec


class CSVAdapter(DataAdapter):
    source_type = "csv"
    _INDEX_COL = "__annoterm_row_index"

    def __init__(
        self,
        path: str,
        row_id_field: str | None = None,
        key_fields: tuple[str, ...] = (),
    ) -> None:
        self.path = Path(path).expanduser().resolve()
        super().__init__(
            source_uri=str(self.path),
            split=None,
            row_id_field=row_id_field,
            key_fields=key_fields,
        )
        self._lazy = pl.scan_csv(str(self.path)).with_row_index(self._INDEX_COL)
        self._row_count_cache: int | None = None
        self._fingerprint_cache: str | None = None
        self._schema_cache: list[ColumnInfo] | None = None

    def schema(self) -> list[ColumnInfo]:
        if self._schema_cache is None:
            schema = self._lazy.collect_schema()
            self._schema_cache = [
                ColumnInfo(name=name, dtype=str(dtype))
                for name, dtype in schema.items()
                if name != self._INDEX_COL
            ]
        return list(self._schema_cache)

    def row_count(self, filter_query: FilterQuery | None = None) -> int:
        if filter_query is None and self._row_count_cache is not None:
            return self._row_count_cache
        lazy = self._apply_query(self._lazy, filter_query=filter_query, sort=None)
        row_count_df = lazy.select(pl.len().alias("row_count")).collect()
        row_count = int(row_count_df.item(0, "row_count"))
        if filter_query is None:
            self._row_count_cache = row_count
        return row_count

    def rows(
        self,
        offset: int,
        limit: int,
        visible_columns: Sequence[str] | None = None,
        filter_query: FilterQuery | None = None,
        sort: SortSpec | None = None,
    ) -> list[RowRecord]:
        if limit <= 0:
            return []
        lazy = self._apply_query(self._lazy, filter_query=filter_query, sort=sort)
        selected_cols = self._select_columns(visible_columns)
        frame = lazy.select(selected_cols).slice(offset, limit).collect()
        raw_rows = frame.to_dicts()
        records: list[RowRecord] = []
        for row_data in raw_rows:
            row_index = int(row_data.pop(self._INDEX_COL))
            records.append(
                build_row_record(
                    row_index=row_index,
                    row_data=row_data,
                    row_id_field=self.row_id_field,
                    key_fields=self.key_fields,
                )
            )
        return records

    def _apply_query(
        self,
        lazy: pl.LazyFrame,
        filter_query: FilterQuery | None,
        sort: SortSpec | None,
    ) -> pl.LazyFrame:
        filtered = lazy
        filter_expr = to_polars_expression(filter_query)
        if filter_expr is not None:
            filtered = filtered.filter(filter_expr)
        if sort and sort.column in {column.name for column in self.schema()}:
            filtered = filtered.sort(
                by=sort.column,
                descending=sort.descending,
                nulls_last=True,
            )
        return filtered

    def _select_columns(self, visible_columns: Sequence[str] | None) -> list[str]:
        schema_columns = [column.name for column in self.schema()]
        if visible_columns is None:
            return [self._INDEX_COL, *schema_columns]
        filtered_columns = [column for column in visible_columns if column in schema_columns]
        if not filtered_columns:
            filtered_columns = schema_columns
        return [self._INDEX_COL, *filtered_columns]

    def fingerprint(self) -> str:
        if self._fingerprint_cache is None:
            self._fingerprint_cache = fingerprint_from_path(self.path)
        return self._fingerprint_cache
