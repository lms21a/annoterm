from __future__ import annotations

from pathlib import Path

import orjson
import pytest

from annoterm.annotations.io import AnnotationBundleStore
from annoterm.data.identity import build_row_record
from annoterm.models import DatasetMeta


def _dataset_meta(source_uri: str, fingerprint: str) -> DatasetMeta:
    return DatasetMeta(
        source_type="csv",
        source_uri=source_uri,
        split=None,
        fingerprint=fingerprint,
        row_count=10,
        row_id_field="id",
        key_fields=("id",),
    )


def test_bundle_initialization_creates_manifest_labelset_and_jsonl(tmp_path: Path) -> None:
    bundle_dir = tmp_path / "bundle"
    store = AnnotationBundleStore(
        bundle_dir=bundle_dir,
        dataset_meta=_dataset_meta("sample.csv", "sha256:dataset1"),
        annotator="alice",
        task_type="preference",
        quick_labels=("high-quality", "low-quality"),
    )

    store.ensure_initialized()

    assert (bundle_dir / "manifest.json").exists()
    assert (bundle_dir / "label_set.json").exists()
    assert (bundle_dir / "annotations.jsonl").exists()
    assert store.quick_label_for_key("1") == "high-quality"
    assert store.quick_label_for_key("2") == "low-quality"

    manifest = orjson.loads((bundle_dir / "manifest.json").read_bytes())
    assert manifest["dataset"]["fingerprint"] == "sha256:dataset1"


def test_append_annotation_writes_one_jsonl_record(tmp_path: Path) -> None:
    bundle_dir = tmp_path / "bundle"
    store = AnnotationBundleStore(
        bundle_dir=bundle_dir,
        dataset_meta=_dataset_meta("sample.csv", "sha256:dataset1"),
        annotator="alice",
        quick_labels=("high-quality",),
    )
    store.ensure_initialized()

    row = build_row_record(
        row_index=4,
        row_data={"id": "r4", "text": "hello"},
        row_id_field="id",
        key_fields=("id",),
    )
    record = store.append_annotation(row=row, label="high-quality", note="strong reasoning")

    lines = (bundle_dir / "annotations.jsonl").read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    payload = orjson.loads(lines[0].encode("utf-8"))

    assert payload["annotation_id"] == record.annotation_id
    assert payload["dataset_fingerprint"] == "sha256:dataset1"
    assert payload["row_ref"]["row_index"] == 4
    assert payload["row_ref"]["row_id"] == "r4"
    assert payload["row_data"] == {"id": "r4", "text": "hello"}
    assert payload["label"] == "high-quality"
    assert store.annotation_count() == 1


def test_bundle_init_rejects_mismatched_dataset_fingerprint(tmp_path: Path) -> None:
    bundle_dir = tmp_path / "bundle"
    first = AnnotationBundleStore(
        bundle_dir=bundle_dir,
        dataset_meta=_dataset_meta("sample.csv", "sha256:dataset1"),
    )
    first.ensure_initialized()

    second = AnnotationBundleStore(
        bundle_dir=bundle_dir,
        dataset_meta=_dataset_meta("sample.csv", "sha256:dataset2"),
    )
    with pytest.raises(ValueError, match="fingerprint"):
        second.ensure_initialized()


def test_set_task_type_persists_label_set(tmp_path: Path) -> None:
    bundle_dir = tmp_path / "bundle"
    store = AnnotationBundleStore(
        bundle_dir=bundle_dir,
        dataset_meta=_dataset_meta("sample.csv", "sha256:dataset1"),
    )
    store.ensure_initialized()

    active = store.set_task_type("classification")
    assert active == "classification"
    assert store.active_task_type() == "classification"
    assert store.labels() == []

    label_set = orjson.loads((bundle_dir / "label_set.json").read_bytes())
    assert label_set["active_task_type"] == "classification"
    assert label_set["task_type"] == "classification"
    assert "classification" in label_set["task_profiles"]


def test_ensure_label_supports_more_than_nine_labels(tmp_path: Path) -> None:
    bundle_dir = tmp_path / "bundle"
    quick_labels = tuple(f"l{index}" for index in range(1, 10))
    store = AnnotationBundleStore(
        bundle_dir=bundle_dir,
        dataset_meta=_dataset_meta("sample.csv", "sha256:dataset1"),
        quick_labels=quick_labels,
    )
    store.ensure_initialized()

    label, key, created = store.ensure_label("extra-label")
    assert label == "extra-label"
    assert created is True
    assert key is None
    assert store.quick_label_map == {str(index): f"l{index}" for index in range(1, 10)}

    label_set = orjson.loads((bundle_dir / "label_set.json").read_bytes())
    assert "extra-label" in label_set["labels"]


def test_labels_are_scoped_per_task(tmp_path: Path) -> None:
    bundle_dir = tmp_path / "bundle"
    store = AnnotationBundleStore(
        bundle_dir=bundle_dir,
        dataset_meta=_dataset_meta("sample.csv", "sha256:dataset1"),
        quick_labels=("good", "bad"),
    )
    store.ensure_initialized()
    assert store.labels() == ["good", "bad"]
    assert store.active_task_type() == "preference"

    store.ensure_label("needs-review")
    assert store.labels() == ["good", "bad", "needs-review"]

    store.set_task_type("classification")
    assert store.labels() == []
    added_label, added_key, created = store.ensure_label("entity-person")
    assert created is True
    assert added_label == "entity-person"
    assert added_key == "1"
    assert store.labels() == ["entity-person"]

    store.set_task_type("preference")
    assert store.labels() == ["good", "bad", "needs-review"]

    label_set = orjson.loads((bundle_dir / "label_set.json").read_bytes())
    assert sorted(label_set["task_profiles"].keys()) == ["classification", "preference"]
    assert label_set["task_profiles"]["classification"]["labels"] == ["entity-person"]
    assert label_set["task_profiles"]["preference"]["labels"] == ["good", "bad", "needs-review"]
