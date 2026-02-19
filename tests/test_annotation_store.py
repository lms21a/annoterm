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
