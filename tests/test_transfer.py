from __future__ import annotations

import dataclasses
import tarfile
from pathlib import Path

import orjson
import pytest

from annoterm.annotations.io import AnnotationBundleStore
from annoterm.annotations.transfer import export_bundle, import_bundle, summarize_bundle
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


def _make_store(
    bundle_dir: Path,
    fingerprint: str = "sha256:dataset1",
    quick_labels: tuple[str, ...] = ("high-quality", "low-quality"),
) -> AnnotationBundleStore:
    store = AnnotationBundleStore(
        bundle_dir=bundle_dir,
        dataset_meta=_dataset_meta("sample.csv", fingerprint),
        annotator="alice",
        task_type="preference",
        quick_labels=quick_labels,
    )
    store.ensure_initialized()
    return store


def _append(store: AnnotationBundleStore, row_id: str, label: str):
    row = build_row_record(
        row_index=int(row_id.strip("r")),
        row_data={"id": row_id, "text": f"text-{row_id}"},
        row_id_field="id",
        key_fields=("id",),
    )
    return store.append_annotation(row=row, label=label)


def test_export_bundle_directory_and_tar(tmp_path: Path) -> None:
    source_store = _make_store(tmp_path / "source")
    _append(source_store, "r1", "high-quality")

    out_dir = tmp_path / "export_dir"
    dir_result = export_bundle(source_store.bundle_dir, out_dir, fmt="dir")
    assert dir_result["format"] == "dir"
    assert (out_dir / "manifest.json").exists()
    assert (out_dir / "label_set.json").exists()
    assert (out_dir / "annotations.jsonl").exists()

    tar_result = export_bundle(source_store.bundle_dir, tmp_path / "share", fmt="tar")
    tar_path = Path(tar_result["output"])
    assert tar_path.exists()
    assert tar_path.name.endswith(".tar.gz")

    with tarfile.open(tar_path, "r:gz") as archive:
        names = archive.getnames()
    assert any(name.endswith("/manifest.json") for name in names)
    assert any(name.endswith("/label_set.json") for name in names)
    assert any(name.endswith("/annotations.jsonl") for name in names)


def test_import_bundle_deduplicates_and_merges_labels(tmp_path: Path) -> None:
    target_store = _make_store(tmp_path / "target", quick_labels=("high-quality", "low-quality"))
    source_store = _make_store(tmp_path / "source", quick_labels=("needs-review",))

    target_record = _append(target_store, "r1", "high-quality")
    source_record = _append(source_store, "r2", "needs-review")

    duplicate_payload = dataclasses.asdict(target_record)
    source_payload = dataclasses.asdict(source_record)
    source_annotations = source_store.bundle_dir / "annotations.jsonl"
    source_annotations.write_bytes(
        b"\n".join(
            [
                orjson.dumps(duplicate_payload),
                orjson.dumps(source_payload),
            ]
        )
        + b"\n"
    )

    result = import_bundle(target_store.bundle_dir, source_store.bundle_dir)

    assert result["imported_count"] == 1
    assert result["duplicate_count"] == 1
    assert result["conflict_count"] == 0

    merged_label_set = orjson.loads((target_store.bundle_dir / "label_set.json").read_bytes())
    assert merged_label_set["labels"] == ["high-quality", "low-quality", "needs-review"]

    merged_lines = (target_store.bundle_dir / "annotations.jsonl").read_text(encoding="utf-8")
    assert len([line for line in merged_lines.splitlines() if line.strip()]) == 2


def test_import_bundle_counts_row_label_conflicts(tmp_path: Path) -> None:
    target_store = _make_store(tmp_path / "target")
    source_store = _make_store(tmp_path / "source")

    _append(target_store, "r3", "high-quality")
    _append(source_store, "r3", "low-quality")

    result = import_bundle(target_store.bundle_dir, source_store.bundle_dir)
    assert result["imported_count"] == 1
    assert result["conflict_count"] == 1


def test_import_bundle_rejects_fingerprint_mismatch_without_override(tmp_path: Path) -> None:
    target_store = _make_store(tmp_path / "target", fingerprint="sha256:f1")
    source_store = _make_store(tmp_path / "source", fingerprint="sha256:f2")

    with pytest.raises(ValueError, match="fingerprint"):
        import_bundle(target_store.bundle_dir, source_store.bundle_dir)

    result = import_bundle(
        target_store.bundle_dir,
        source_store.bundle_dir,
        allow_fingerprint_mismatch=True,
    )
    assert result["source_dataset_fingerprint"] == "sha256:f2"


def test_import_bundle_dry_run_does_not_modify_target_files(tmp_path: Path) -> None:
    target_store = _make_store(tmp_path / "target", quick_labels=("high-quality",))
    source_store = _make_store(tmp_path / "source", quick_labels=("needs-review",))

    _append(source_store, "r7", "needs-review")

    target_annotations_path = target_store.bundle_dir / "annotations.jsonl"
    target_label_set_path = target_store.bundle_dir / "label_set.json"
    before_annotations = target_annotations_path.read_text(encoding="utf-8")
    before_label_set = target_label_set_path.read_text(encoding="utf-8")

    result = import_bundle(
        target_store.bundle_dir,
        source_store.bundle_dir,
        dry_run=True,
    )

    after_annotations = target_annotations_path.read_text(encoding="utf-8")
    after_label_set = target_label_set_path.read_text(encoding="utf-8")

    assert result["dry_run"] is True
    assert result["applied"] is False
    assert result["imported_count"] == 1
    assert result["label_set_changed"] is True
    assert before_annotations == after_annotations
    assert before_label_set == after_label_set


def test_summarize_bundle_reports_stats_duplicates_and_conflicts(tmp_path: Path) -> None:
    store = _make_store(tmp_path / "bundle")
    first = _append(store, "r1", "high-quality")
    _append(store, "r2", "low-quality")
    _append(store, "r2", "high-quality")

    duplicate_payload = dataclasses.asdict(first)
    with (store.bundle_dir / "annotations.jsonl").open("ab") as handle:
        handle.write(orjson.dumps(duplicate_payload))
        handle.write(b"\n")

    summary = summarize_bundle(store.bundle_dir, limit=2)

    assert summary["counts"]["total_records"] == 4
    assert summary["counts"]["filtered_records"] == 4
    assert summary["counts"]["duplicate_annotation_ids"] == 1
    assert summary["counts"]["row_label_conflicts"] == 1
    assert summary["stats"]["by_label"] == {"high-quality": 3, "low-quality": 1}
    assert summary["stats"]["by_annotator"] == {"alice": 4}
    assert len(summary["sample_records"]) == 2


def test_summarize_bundle_applies_filters_case_insensitively(tmp_path: Path) -> None:
    store = _make_store(tmp_path / "bundle")
    _append(store, "r1", "high-quality")
    bob_record = dataclasses.asdict(_append(store, "r2", "needs-review"))
    bob_record["annotator"] = "bob"
    bob_record["task_type"] = "classification"
    bob_record["label"] = "entity-person"

    with (store.bundle_dir / "annotations.jsonl").open("ab") as handle:
        handle.write(orjson.dumps(bob_record))
        handle.write(b"\n")

    summary = summarize_bundle(
        store.bundle_dir,
        label="ENTITY-PERSON",
        annotator="BoB",
        task_type="classification",
    )

    assert summary["counts"]["total_records"] == 3
    assert summary["counts"]["filtered_records"] == 1
    assert summary["stats"]["by_label"] == {"entity-person": 1}
    assert summary["stats"]["by_annotator"] == {"bob": 1}
