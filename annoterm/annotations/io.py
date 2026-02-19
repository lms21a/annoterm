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
        self.task_profiles: dict[str, dict[str, Any]] = {}
        self.quick_label_map: dict[str, str] = {}
        self.label_values: list[str] = []
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

    def active_task_type(self) -> str:
        return self.task_type

    def task_types(self) -> list[str]:
        if self.task_type not in self.task_profiles:
            return list(self.task_profiles.keys())
        ordered = [self.task_type]
        for task in self.task_profiles:
            if task != self.task_type:
                ordered.append(task)
        return ordered

    def labels(self) -> list[str]:
        return list(self.label_values)

    def active_hotkeys(self) -> dict[str, str]:
        return dict(self.quick_label_map)

    def annotation_count(self) -> int:
        return self._annotation_count

    def set_task_type(self, task_type: str) -> str:
        cleaned = task_type.strip()
        if not cleaned:
            raise ValueError("Task type cannot be empty.")
        self._ensure_task_profile(cleaned)
        self.task_type = cleaned
        self._load_active_task_state()
        self._persist_label_set()
        return self.task_type

    def ensure_label(self, label: str) -> tuple[str, str | None, bool]:
        cleaned = label.strip()
        if not cleaned:
            raise ValueError("Label cannot be empty.")

        for existing in self.label_values:
            if existing.casefold() == cleaned.casefold():
                return existing, self._hotkey_for_label(existing), False

        self.label_values.append(cleaned)
        assigned_key = self._assign_hotkey_if_available(cleaned)
        self._store_active_task_state()
        self._persist_label_set()
        return cleaned, assigned_key, True

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
        resolved_label, _, _ = self.ensure_label(label)

        record = AnnotationRecord(
            annotation_id=str(uuid4()),
            bundle_id=self.bundle_id,
            dataset_fingerprint=self.dataset_meta.fingerprint,
            task_type=self.task_type,
            label=resolved_label,
            payload=payload or {},
            row_ref={
                "row_index": row.row_index,
                "row_id": row.row_id,
                "key_fields": row.key_fields,
            },
            row_hash=row.row_hash,
            row_data=dict(row.row_data),
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
        self.task_profiles = {
            self.task_type: {
                "labels": list(self.quick_labels),
                "hotkeys": dict(hotkeys),
            }
        }
        label_set = LabelSet(
            schema_version=SCHEMA_VERSION,
            task_type=self.task_type,
            labels=list(self.quick_labels),
            hotkeys=hotkeys,
        )
        _write_json(self.label_set_path, asdict(label_set))
        self._load_active_task_state()
        self._persist_label_set()

    def _load_label_set(self) -> None:
        label_set = _read_json(self.label_set_path)
        active_task = (
            str(label_set.get("active_task_type") or "").strip()
            or str(label_set.get("task_type") or "").strip()
            or self.task_type
        )

        task_profiles_payload = label_set.get("task_profiles")
        loaded_profiles: dict[str, dict[str, Any]] = {}
        if isinstance(task_profiles_payload, dict):
            for raw_task, profile in task_profiles_payload.items():
                task_name = str(raw_task).strip()
                if not task_name:
                    continue
                if not isinstance(profile, dict):
                    profile = {}
                loaded_profiles[task_name] = self._normalize_profile(
                    labels=profile.get("labels") or [],
                    hotkeys=profile.get("hotkeys") or {},
                )

        if not loaded_profiles:
            loaded_profiles[active_task] = self._normalize_profile(
                labels=label_set.get("labels") or [],
                hotkeys=label_set.get("hotkeys") or {},
            )

        self.task_profiles = loaded_profiles
        self._ensure_task_profile(active_task)
        self.task_type = active_task
        self._load_active_task_state()
        self._persist_label_set()

    def _persist_label_set(self) -> None:
        self._store_active_task_state()
        ordered_hotkeys = dict(sorted(self.quick_label_map.items(), key=lambda item: item[0]))
        ordered_profiles = {
            task_name: {
                "labels": list(profile["labels"]),
                "hotkeys": dict(sorted(profile["hotkeys"].items(), key=lambda item: item[0])),
            }
            for task_name, profile in sorted(self.task_profiles.items(), key=lambda item: item[0])
        }
        payload = {
            "schema_version": SCHEMA_VERSION,
            "active_task_type": self.task_type,
            "task_type": self.task_type,
            "labels": list(self.label_values),
            "hotkeys": ordered_hotkeys,
            "task_profiles": ordered_profiles,
        }
        if self.label_set_path.exists():
            try:
                if _read_json(self.label_set_path) == payload:
                    return
            except Exception:
                pass
        _write_json(self.label_set_path, payload)

    def _assign_hotkey_if_available(self, label: str) -> str | None:
        for index in range(1, 10):
            key = str(index)
            if key not in self.quick_label_map:
                self.quick_label_map[key] = label
                return key
        return None

    def _hotkey_for_label(self, label: str) -> str | None:
        for key, value in self.quick_label_map.items():
            if value.casefold() == label.casefold():
                return key
        return None

    def _ensure_task_profile(self, task_type: str) -> None:
        if task_type in self.task_profiles:
            return
        self.task_profiles[task_type] = self._normalize_profile(labels=[], hotkeys={})

    def _load_active_task_state(self) -> None:
        self._ensure_task_profile(self.task_type)
        profile = self.task_profiles[self.task_type]
        self.label_values = list(profile["labels"])
        self.quick_label_map = dict(profile["hotkeys"])

    def _store_active_task_state(self) -> None:
        self._ensure_task_profile(self.task_type)
        normalized = self._normalize_profile(labels=self.label_values, hotkeys=self.quick_label_map)
        self.task_profiles[self.task_type] = normalized
        self.label_values = list(normalized["labels"])
        self.quick_label_map = dict(normalized["hotkeys"])

    def _normalize_profile(
        self,
        labels: Sequence[Any],
        hotkeys: dict[str, Any],
    ) -> dict[str, Any]:
        normalized_labels: list[str] = []
        label_index: set[str] = set()
        for label in labels:
            text = str(label).strip()
            if not text:
                continue
            key = text.casefold()
            if key in label_index:
                continue
            normalized_labels.append(text)
            label_index.add(key)

        normalized_hotkeys: dict[str, str] = {}
        for key_raw, label_raw in hotkeys.items():
            key = str(key_raw).strip()
            label = str(label_raw).strip()
            if not key or not label:
                continue
            normalized_hotkeys[key] = label
            label_key = label.casefold()
            if label_key not in label_index:
                normalized_labels.append(label)
                label_index.add(label_key)

        return {
            "labels": normalized_labels,
            "hotkeys": normalized_hotkeys,
        }

    @staticmethod
    def _count_lines(path: Path) -> int:
        with path.open("rb") as handle:
            return sum(1 for _ in handle)
