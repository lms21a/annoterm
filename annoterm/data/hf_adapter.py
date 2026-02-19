"""Hugging Face datasets adapter."""

from __future__ import annotations

import hashlib
from functools import cmp_to_key
from typing import Any, Sequence

import orjson
from datasets import Dataset, load_dataset

from annoterm.data.base import DataAdapter
from annoterm.data.identity import build_row_record
from annoterm.filters.eval import row_matches_filter
from annoterm.filters.parser import FilterQuery
from annoterm.models import ColumnInfo, RowRecord
from annoterm.models import SortSpec


class HFAdapter(DataAdapter):
    source_type = "hf"

    def __init__(
        self,
        dataset_name: str,
        split: str = "train",
        config_name: str | None = None,
        row_id_field: str | None = None,
        key_fields: tuple[str, ...] = (),
    ) -> None:
        self.dataset_name = dataset_name
        self.config_name = config_name
        super().__init__(
            source_uri=dataset_name,
            split=split,
            row_id_field=row_id_field,
            key_fields=key_fields,
        )

        load_kwargs: dict[str, Any] = {}
        if config_name:
            load_kwargs["name"] = config_name
        loaded = load_dataset(path=dataset_name, split=split, **load_kwargs)
        if not isinstance(loaded, Dataset):
            raise TypeError(
                "HF adapter expects a map-style datasets.Dataset. "
                "Streaming/iterable datasets are not supported in this baseline."
            )
        self._dataset = loaded
        self._fingerprint_cache: str | None = None
        self._query_cache: dict[tuple[str, str], list[tuple[int, dict[str, Any]]]] = {}

    def schema(self) -> list[ColumnInfo]:
        return [
            ColumnInfo(name=field_name, dtype=str(feature))
            for field_name, feature in self._dataset.features.items()
        ]

    def row_count(self, filter_query: FilterQuery | None = None) -> int:
        if filter_query is None:
            return len(self._dataset)
        rows = self._materialize_rows(filter_query=filter_query, sort=None)
        return len(rows)

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
        rows = self._materialize_rows(filter_query=filter_query, sort=sort)
        if offset >= len(rows):
            return []
        end = min(offset + limit, len(rows))
        selected_columns = self._select_columns(visible_columns)
        subset = rows[offset:end]
        return [
            build_row_record(
                row_index=row_index,
                row_data=self._subset_row_columns(row_data, selected_columns),
                row_id_field=self.row_id_field,
                key_fields=self.key_fields,
            )
            for row_index, row_data in subset
        ]

    def fingerprint(self) -> str:
        if self._fingerprint_cache is None:
            payload = orjson.dumps(
                {
                    "dataset_name": self.dataset_name,
                    "config_name": self.config_name,
                    "split": self.split,
                    "dataset_fingerprint": getattr(self._dataset, "_fingerprint", None),
                    "row_count": len(self._dataset),
                },
                option=orjson.OPT_SORT_KEYS,
            )
            digest = hashlib.sha256(payload).hexdigest()
            self._fingerprint_cache = f"sha256:{digest}"
        return self._fingerprint_cache

    def _materialize_rows(
        self,
        filter_query: FilterQuery | None,
        sort: SortSpec | None,
    ) -> list[tuple[int, dict[str, Any]]]:
        filter_key = "" if filter_query is None else filter_query.cache_key()
        sort_key = "" if sort is None else f"{sort.column}:{'desc' if sort.descending else 'asc'}"
        cache_key = (filter_key, sort_key)
        if cache_key in self._query_cache:
            return self._query_cache[cache_key]

        records: list[tuple[int, dict[str, Any]]] = []
        for row_index, row_data in enumerate(self._dataset):
            if row_matches_filter(row_data, filter_query):
                records.append((row_index, row_data))

        if sort and sort.column in self._dataset.features:
            records.sort(
                key=cmp_to_key(
                    lambda left, right: _compare_for_sort(
                        left[1].get(sort.column),
                        right[1].get(sort.column),
                        descending=sort.descending,
                    )
                )
            )

        self._query_cache[cache_key] = records
        return records

    def _select_columns(self, visible_columns: Sequence[str] | None) -> set[str] | None:
        if visible_columns is None:
            return None
        schema_columns = set(self._dataset.features.keys())
        selected = {column for column in visible_columns if column in schema_columns}
        return selected if selected else None

    @staticmethod
    def _subset_row_columns(row_data: dict[str, Any], columns: set[str] | None) -> dict[str, Any]:
        if columns is None:
            return dict(row_data)
        return {column: value for column, value in row_data.items() if column in columns}


def _compare_for_sort(left: Any, right: Any, descending: bool) -> int:
    if left is None and right is None:
        return 0
    if left is None:
        return 1
    if right is None:
        return -1

    try:
        if left < right:
            result = -1
        elif left > right:
            result = 1
        else:
            result = 0
    except TypeError:
        left_text = str(left)
        right_text = str(right)
        if left_text < right_text:
            result = -1
        elif left_text > right_text:
            result = 1
        else:
            result = 0

    return -result if descending else result
