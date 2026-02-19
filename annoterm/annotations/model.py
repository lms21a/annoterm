"""Annotation model dataclasses for JSONL bundle persistence."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


SCHEMA_VERSION = "1.0"


@dataclass(frozen=True)
class Manifest:
    schema_version: str
    bundle_id: str
    created_at: str
    dataset: dict[str, Any]
    tool: dict[str, str]


@dataclass(frozen=True)
class LabelSet:
    schema_version: str
    task_type: str
    labels: list[str]
    hotkeys: dict[str, str]


@dataclass(frozen=True)
class AnnotationRecord:
    schema_version: str = SCHEMA_VERSION
    annotation_id: str = ""
    bundle_id: str = ""
    dataset_fingerprint: str = ""
    task_type: str = "preference"
    label: str = ""
    payload: dict[str, Any] = field(default_factory=dict)
    row_ref: dict[str, Any] = field(default_factory=dict)
    row_hash: str = ""
    annotator: str = "unknown"
    note: str | None = None
    created_at: str = ""
    updated_at: str | None = None
    event_type: str = "create"
