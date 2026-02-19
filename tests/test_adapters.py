from __future__ import annotations

from pathlib import Path

import orjson

from annoterm.data.factory import create_adapter
from annoterm.filters.parser import parse_filter_expression
from annoterm.models import SortSpec


def _write_csv(path: Path) -> None:
    path.write_text(
        "id,name,score\n"
        "1,alpha,0.10\n"
        "2,beta,0.55\n"
        "3,gamma,0.95\n",
        encoding="utf-8",
    )


def _write_jsonl(path: Path) -> None:
    rows = [
        {"id": "a", "name": "alpha", "score": 0.10},
        {"id": "b", "name": "beta", "score": 0.55},
        {"id": "c", "name": "gamma", "score": 0.95},
    ]
    serialized = "\n".join(orjson.dumps(row).decode("utf-8") for row in rows)
    path.write_text(f"{serialized}\n", encoding="utf-8")


def test_csv_adapter_roundtrip(tmp_path: Path) -> None:
    csv_path = tmp_path / "sample.csv"
    _write_csv(csv_path)

    adapter = create_adapter(str(csv_path), row_id_field="id", key_fields=("id",))
    schema = adapter.schema()
    rows = adapter.rows(offset=0, limit=2)

    assert adapter.source_type == "csv"
    assert adapter.row_count() == 3
    assert [column.name for column in schema] == ["id", "name", "score"]
    assert rows[0].row_index == 0
    assert rows[0].row_id == "1"
    assert rows[0].key_fields == {"id": 1}
    assert rows[0].row_hash.startswith("sha256:")
    assert adapter.fingerprint().startswith("sha256:")


def test_jsonl_adapter_roundtrip(tmp_path: Path) -> None:
    jsonl_path = tmp_path / "sample.jsonl"
    _write_jsonl(jsonl_path)

    adapter = create_adapter(str(jsonl_path), key_fields=("id",))
    rows = adapter.rows(offset=1, limit=2)

    assert adapter.source_type == "jsonl"
    assert adapter.row_count() == 3
    assert [row.row_index for row in rows] == [1, 2]
    assert rows[0].row_id == "b"
    assert rows[1].key_fields == {"id": "c"}


def test_factory_supports_hf_prefix_without_loading(tmp_path: Path) -> None:
    csv_path = tmp_path / "small.csv"
    _write_csv(csv_path)
    adapter = create_adapter(str(csv_path))
    assert adapter.source_type == "csv"


def test_csv_adapter_filter_and_sort(tmp_path: Path) -> None:
    csv_path = tmp_path / "sample.csv"
    _write_csv(csv_path)

    adapter = create_adapter(str(csv_path), row_id_field="id")
    filter_query = parse_filter_expression("score >= 0.55")
    rows = adapter.rows(
        offset=0,
        limit=5,
        filter_query=filter_query,
        sort=SortSpec(column="score", descending=True),
    )

    assert adapter.row_count(filter_query=filter_query) == 2
    assert [row.row_id for row in rows] == ["3", "2"]
    assert [row.row_index for row in rows] == [2, 1]


def test_jsonl_adapter_visible_columns_subset(tmp_path: Path) -> None:
    jsonl_path = tmp_path / "sample.jsonl"
    _write_jsonl(jsonl_path)

    adapter = create_adapter(str(jsonl_path))
    rows = adapter.rows(offset=0, limit=1, visible_columns=["id"])

    assert list(rows[0].row_data.keys()) == ["id"]
