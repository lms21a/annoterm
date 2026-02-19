"""Bundle export/import and merge helpers."""

from __future__ import annotations

import shutil
import tarfile
from collections import Counter
from pathlib import Path
from typing import Any

import orjson


REQUIRED_BUNDLE_FILES = ("manifest.json", "label_set.json", "annotations.jsonl")


def _read_json(path: Path) -> dict[str, Any]:
    return orjson.loads(path.read_bytes())


def _write_json(path: Path, payload: object) -> None:
    path.write_bytes(orjson.dumps(payload, option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS))


def validate_bundle_dir(bundle_dir: str | Path) -> Path:
    bundle_path = Path(bundle_dir).expanduser().resolve()
    if not bundle_path.exists() or not bundle_path.is_dir():
        raise ValueError(f"Bundle directory does not exist: {bundle_path}")

    missing = [name for name in REQUIRED_BUNDLE_FILES if not (bundle_path / name).exists()]
    if missing:
        joined = ", ".join(missing)
        raise ValueError(f"Bundle is missing required files: {joined}")
    return bundle_path


def load_manifest(bundle_dir: str | Path) -> dict[str, Any]:
    bundle_path = validate_bundle_dir(bundle_dir)
    return _read_json(bundle_path / "manifest.json")


def load_label_set(bundle_dir: str | Path) -> dict[str, Any]:
    bundle_path = validate_bundle_dir(bundle_dir)
    return _read_json(bundle_path / "label_set.json")


def load_annotations(bundle_dir: str | Path) -> list[dict[str, Any]]:
    bundle_path = validate_bundle_dir(bundle_dir)
    records: list[dict[str, Any]] = []
    with (bundle_path / "annotations.jsonl").open("rb") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            records.append(orjson.loads(line))
    return records


def export_bundle(
    source_bundle_dir: str | Path,
    output_path: str | Path,
    fmt: str = "dir",
    overwrite: bool = False,
) -> dict[str, Any]:
    source = validate_bundle_dir(source_bundle_dir)
    output = Path(output_path).expanduser().resolve()
    export_format = fmt.strip().lower()
    if export_format not in {"dir", "tar"}:
        raise ValueError("Export format must be 'dir' or 'tar'.")

    if export_format == "dir":
        if output.exists():
            if not overwrite:
                raise ValueError(f"Export path already exists: {output}")
            if output.is_file():
                output.unlink()
            else:
                shutil.rmtree(output)
        output.mkdir(parents=True, exist_ok=True)
        for file_name in REQUIRED_BUNDLE_FILES:
            shutil.copy2(source / file_name, output / file_name)
        return {
            "format": "dir",
            "source": str(source),
            "output": str(output),
            "files": list(REQUIRED_BUNDLE_FILES),
            "annotation_count": _count_annotations(source / "annotations.jsonl"),
        }

    # format == "tar"
    tar_path = output
    if tar_path.suffixes[-2:] != [".tar", ".gz"]:
        tar_path = Path(f"{tar_path}.tar.gz")
    if tar_path.exists():
        if not overwrite:
            raise ValueError(f"Export path already exists: {tar_path}")
        tar_path.unlink()
    tar_path.parent.mkdir(parents=True, exist_ok=True)

    bundle_name = source.name
    with tarfile.open(tar_path, "w:gz") as archive:
        for file_name in REQUIRED_BUNDLE_FILES:
            archive.add(source / file_name, arcname=f"{bundle_name}/{file_name}")
    return {
        "format": "tar",
        "source": str(source),
        "output": str(tar_path),
        "files": list(REQUIRED_BUNDLE_FILES),
        "annotation_count": _count_annotations(source / "annotations.jsonl"),
    }


def import_bundle(
    target_bundle_dir: str | Path,
    source_bundle_dir: str | Path,
    allow_fingerprint_mismatch: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    target = validate_bundle_dir(target_bundle_dir)
    source = validate_bundle_dir(source_bundle_dir)

    target_manifest = _read_json(target / "manifest.json")
    source_manifest = _read_json(source / "manifest.json")
    target_fp = (target_manifest.get("dataset") or {}).get("fingerprint")
    source_fp = (source_manifest.get("dataset") or {}).get("fingerprint")
    if not allow_fingerprint_mismatch and target_fp != source_fp:
        raise ValueError(
            "Dataset fingerprint mismatch. "
            f"target={target_fp} source={source_fp}. "
            "Use --allow-fingerprint-mismatch to override."
        )

    target_labels = _read_json(target / "label_set.json")
    source_labels = _read_json(source / "label_set.json")
    merged_label_set = _merge_label_set(target_labels, source_labels)
    label_set_changed = (
        merged_label_set.get("labels") != target_labels.get("labels")
        or merged_label_set.get("hotkeys") != target_labels.get("hotkeys")
        or merged_label_set.get("task_type") != target_labels.get("task_type")
    )
    if not dry_run:
        _write_json(target / "label_set.json", merged_label_set)

    target_records = load_annotations(target)
    source_records = load_annotations(source)

    existing_ids = {str(record.get("annotation_id")) for record in target_records}
    to_add: list[dict[str, Any]] = []
    duplicate_count = 0
    for record in source_records:
        annotation_id = str(record.get("annotation_id"))
        if annotation_id and annotation_id in existing_ids:
            duplicate_count += 1
            continue
        to_add.append(record)
        if annotation_id:
            existing_ids.add(annotation_id)

    sorted_to_add = sorted(
        to_add,
        key=lambda record: (
            str(record.get("created_at") or ""),
            str(record.get("annotation_id") or ""),
        ),
    )
    if sorted_to_add and not dry_run:
        with (target / "annotations.jsonl").open("ab") as handle:
            for record in sorted_to_add:
                handle.write(orjson.dumps(record))
                handle.write(b"\n")

    merged_records = target_records + sorted_to_add
    conflict_count = _count_row_label_conflicts(merged_records)

    return {
        "target_bundle": str(target),
        "source_bundle": str(source),
        "dry_run": bool(dry_run),
        "applied": not dry_run,
        "target_dataset_fingerprint": target_fp,
        "source_dataset_fingerprint": source_fp,
        "source_count": len(source_records),
        "existing_count": len(target_records),
        "label_set_changed": label_set_changed,
        "imported_count": len(sorted_to_add),
        "duplicate_count": duplicate_count,
        "conflict_count": conflict_count,
    }


def summarize_bundle(
    bundle_dir: str | Path,
    limit: int = 5,
    label: str | None = None,
    annotator: str | None = None,
    task_type: str | None = None,
) -> dict[str, Any]:
    bundle = validate_bundle_dir(bundle_dir)
    manifest = _read_json(bundle / "manifest.json")
    label_set = _read_json(bundle / "label_set.json")
    all_records = load_annotations(bundle)

    filtered_records = [
        record
        for record in all_records
        if _record_matches_filter(
            record,
            label=label,
            annotator=annotator,
            task_type=task_type,
        )
    ]

    by_label = Counter(str(record.get("label") or "") for record in filtered_records)
    by_annotator = Counter(str(record.get("annotator") or "") for record in filtered_records)
    by_task_type = Counter(str(record.get("task_type") or "") for record in filtered_records)
    created_at_values = sorted(
        str(record.get("created_at"))
        for record in filtered_records
        if record.get("created_at")
    )
    sample_size = max(limit, 0)
    sample = filtered_records[:sample_size]

    return {
        "bundle_dir": str(bundle),
        "manifest": manifest,
        "label_set": label_set,
        "filters": {
            "label": label,
            "annotator": annotator,
            "task_type": task_type,
        },
        "counts": {
            "total_records": len(all_records),
            "filtered_records": len(filtered_records),
            "duplicate_annotation_ids": _count_duplicate_annotation_ids(filtered_records),
            "row_label_conflicts": _count_row_label_conflicts(filtered_records),
        },
        "stats": {
            "by_label": dict(sorted((k, v) for k, v in by_label.items() if k)),
            "by_annotator": dict(sorted((k, v) for k, v in by_annotator.items() if k)),
            "by_task_type": dict(sorted((k, v) for k, v in by_task_type.items() if k)),
            "first_created_at": created_at_values[0] if created_at_values else None,
            "last_created_at": created_at_values[-1] if created_at_values else None,
        },
        "sample_records": sample,
    }


def _record_matches_filter(
    record: dict[str, Any],
    *,
    label: str | None = None,
    annotator: str | None = None,
    task_type: str | None = None,
) -> bool:
    if label is not None:
        record_label = str(record.get("label") or "").strip().lower()
        if record_label != label.strip().lower():
            return False

    if annotator is not None:
        record_annotator = str(record.get("annotator") or "").strip().lower()
        if record_annotator != annotator.strip().lower():
            return False

    if task_type is not None:
        record_task_type = str(record.get("task_type") or "").strip().lower()
        if record_task_type != task_type.strip().lower():
            return False

    return True


def _count_duplicate_annotation_ids(records: list[dict[str, Any]]) -> int:
    counts: Counter[str] = Counter()
    for record in records:
        annotation_id = str(record.get("annotation_id") or "").strip()
        if annotation_id:
            counts[annotation_id] += 1
    return sum(1 for count in counts.values() if count > 1)


def _count_annotations(path: Path) -> int:
    with path.open("rb") as handle:
        return sum(1 for line in handle if line.strip())


def _merge_label_set(target: dict[str, Any], source: dict[str, Any]) -> dict[str, Any]:
    target_labels = [str(label) for label in target.get("labels", []) if str(label).strip()]
    source_labels = [str(label) for label in source.get("labels", []) if str(label).strip()]

    merged_labels: list[str] = []
    for label in target_labels + source_labels:
        if label not in merged_labels:
            merged_labels.append(label)

    target_hotkeys = {
        str(key): str(value)
        for key, value in (target.get("hotkeys") or {}).items()
        if str(value).strip()
    }
    source_hotkeys = {
        str(key): str(value)
        for key, value in (source.get("hotkeys") or {}).items()
        if str(value).strip()
    }

    merged_hotkeys = dict(target_hotkeys)
    used_labels = set(merged_hotkeys.values())

    # Add source mappings when the key is free or identical.
    for key in sorted(source_hotkeys):
        label = source_hotkeys[key]
        if key not in merged_hotkeys:
            merged_hotkeys[key] = label
            used_labels.add(label)
        elif merged_hotkeys[key] == label:
            used_labels.add(label)

    # Ensure all labels are mapped, filling remaining numeric slots 1..9.
    free_keys = [str(index) for index in range(1, 10) if str(index) not in merged_hotkeys]
    for label in merged_labels:
        if label in used_labels:
            continue
        if not free_keys:
            break
        merged_hotkeys[free_keys.pop(0)] = label
        used_labels.add(label)

    return {
        "schema_version": target.get("schema_version", source.get("schema_version", "1.0")),
        "task_type": target.get("task_type", source.get("task_type", "preference")),
        "labels": merged_labels,
        "hotkeys": dict(sorted(merged_hotkeys.items(), key=lambda item: item[0])),
    }


def _count_row_label_conflicts(records: list[dict[str, Any]]) -> int:
    labels_by_key: dict[tuple[str, str, str], set[str]] = {}
    for record in records:
        task_type = str(record.get("task_type") or "")
        label = str(record.get("label") or "")
        if not task_type or not label:
            continue

        row_ref = record.get("row_ref") or {}
        row_id = str(row_ref.get("row_id") or "")
        if not row_id:
            key_fields = row_ref.get("key_fields") or {}
            row_id = orjson.dumps(key_fields, option=orjson.OPT_SORT_KEYS).decode("utf-8")
        row_hash = str(record.get("row_hash") or "")
        key = (task_type, row_id, row_hash)

        labels_by_key.setdefault(key, set()).add(label)

    return sum(1 for labels in labels_by_key.values() if len(labels) > 1)
