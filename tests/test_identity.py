from __future__ import annotations

from annoterm.data.identity import build_row_record, compute_row_hash


def test_compute_row_hash_is_stable_across_key_order() -> None:
    row_a = {
        "b": 2,
        "a": 1,
        "nested": {
            "k2": [1, 2, 3],
            "k1": {"x": "y"},
        },
    }
    row_b = {
        "nested": {
            "k1": {"x": "y"},
            "k2": [1, 2, 3],
        },
        "a": 1,
        "b": 2,
    }
    assert compute_row_hash(row_a) == compute_row_hash(row_b)


def test_compute_row_hash_changes_when_values_change() -> None:
    assert compute_row_hash({"id": 1, "name": "a"}) != compute_row_hash(
        {"id": 1, "name": "b"}
    )


def test_build_row_record_uses_id_field_and_key_fields() -> None:
    row = build_row_record(
        row_index=7,
        row_data={"sample_id": "abc", "group": "train", "text": "hello"},
        row_id_field="sample_id",
        key_fields=("sample_id", "group"),
    )
    assert row.row_index == 7
    assert row.row_id == "abc"
    assert row.key_fields == {"sample_id": "abc", "group": "train"}
    assert row.row_hash.startswith("sha256:")
