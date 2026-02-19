"""JSONL-first annotation bundle persistence."""

from __future__ import annotations

import os
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Sequence
from uuid import uuid4

import orjson

from annoterm import __version__
from annoterm.annotations.model import AnnotationRecord, LabelSet, Manifest, SCHEMA_VERSION
from annoterm.models import DatasetMeta, RowRecord


DEFAULT_QUICK_LABELS: tuple[str, ...] = (
    "high-quality",
    "low-quality",
    "needs-review",
    "skip",
)


def _now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _write_json(path: Path, payload: object) -> None:
    path.write_bytes(orjson.dumps(payload, option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS))


def _read_json(path: Path) -> dict[str, Any]:
    return orjson.loads(path.read_bytes())


class AnnotationBundleStore:
    """Manages one portable annotation bundle directory."""

    def __init__(
        self,
        bundle_dir: str | Path,
        dataset_meta: DatasetMeta,
        annotator: str | None = None,
        task_type: str = "preference",
        quick_labels: Sequence[str] = DEFAULT_QUICK_LABELS,
    ) -> None:
        self.bundle_dir = Path(bundle_dir).expanduser().resolve()
        self.dataset_meta = dataset_meta
        self.annotator = (annotator or os.environ.get("USER") or "unknown").strip() or "unknown"
        self.task_type = task_type

        cleaned_labels = [label.strip() for label in quick_labels if label.strip()]
        self.quick_labels = cleaned_labels[:9] if cleaned_labels else list(DEFAULT_QUICK_LABELS)

        self.manifest_path = self.bundle_dir / "manifest.json"
        self.label_set_path = self.bundle_dir / "label_set.json"
        self.annotations_path = self.bundle_dir / "annotations.jsonl"

        self.bundle_id: str | None = None
        self.quick_label_map: dict[str, str] = {}
        self._annotation_count = 0

    def ensure_initialized(self) -> None:
        self.bundle_dir.mkdir(parents=True, exist_ok=True)

        if self.manifest_path.exists():
            self._load_and_validate_manifest()
        else:
            self._initialize_manifest()

        if self.label_set_path.exists():
            self._load_label_set()
        else:
            self._initialize_label_set()

        if not self.annotations_path.exists():
            self.annotations_path.write_text("", encoding="utf-8")
            self._annotation_count = 0
        else:
            self._annotation_count = self._count_lines(self.annotations_path)

    def quick_label_for_key(self, key: str) -> str | None:
        return self.quick_label_map.get(str(key))

    def annotation_count(self) -> int:
        return self._annotation_count

    def append_annotation(
        self,
        row: RowRecord,
        label: str,
        note: str | None = None,
        payload: dict[str, Any] | None = None,
        event_type: str = "create",
    ) -> AnnotationRecord:
        if not self.bundle_id:
            raise RuntimeError("Bundle not initialized. Call ensure_initialized() before writing.")

        record = AnnotationRecord(
            annotation_id=str(uuid4()),
            bundle_id=self.bundle_id,
            dataset_fingerprint=self.dataset_meta.fingerprint,
            task_type=self.task_type,
            label=label,
            payload=payload or {},
            row_ref={
                "row_index": row.row_index,
                "row_id": row.row_id,
                "key_fields": row.key_fields,
            },
            row_hash=row.row_hash,
            annotator=self.annotator,
            note=note,
            created_at=_now_iso(),
            updated_at=None,
            event_type=event_type,
        )

        with self.annotations_path.open("ab") as handle:
            handle.write(orjson.dumps(asdict(record)))
            handle.write(b"\n")

        self._annotation_count += 1
        return record

    def _initialize_manifest(self) -> None:
        self.bundle_id = str(uuid4())
        manifest = Manifest(
            schema_version=SCHEMA_VERSION,
            bundle_id=self.bundle_id,
            created_at=_now_iso(),
            dataset={
                "source_type": self.dataset_meta.source_type,
                "source_uri": self.dataset_meta.source_uri,
                "split": self.dataset_meta.split,
                "fingerprint": self.dataset_meta.fingerprint,
                "row_id_field": self.dataset_meta.row_id_field,
                "key_fields": list(self.dataset_meta.key_fields),
            },
            tool={"name": "annoterm", "version": __version__},
        )
        _write_json(self.manifest_path, asdict(manifest))

    def _load_and_validate_manifest(self) -> None:
        manifest = _read_json(self.manifest_path)
        dataset = manifest.get("dataset", {})
        expected_fp = dataset.get("fingerprint")
        if expected_fp != self.dataset_meta.fingerprint:
            raise ValueError(
                "Bundle dataset fingerprint does not match current source. "
                f"bundle={expected_fp} current={self.dataset_meta.fingerprint}"
            )
        self.bundle_id = str(manifest.get("bundle_id", "")).strip() or str(uuid4())

    def _initialize_label_set(self) -> None:
        hotkeys = {str(index): label for index, label in enumerate(self.quick_labels, start=1)}
        label_set = LabelSet(
            schema_version=SCHEMA_VERSION,
            task_type=self.task_type,
            labels=list(self.quick_labels),
            hotkeys=hotkeys,
        )
        _write_json(self.label_set_path, asdict(label_set))
        self.quick_label_map = hotkeys

    def _load_label_set(self) -> None:
        label_set = _read_json(self.label_set_path)
        hotkeys = label_set.get("hotkeys") or {}
        normalized_hotkeys: dict[str, str] = {}
        for key, value in hotkeys.items():
            if value:
                normalized_hotkeys[str(key)] = str(value)
        self.quick_label_map = normalized_hotkeys

    @staticmethod
    def _count_lines(path: Path) -> int:
        with path.open("rb") as handle:
            return sum(1 for _ in handle)
