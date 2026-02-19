"""Row identity, hashing, and dataset fingerprint helpers."""

from __future__ import annotations

import hashlib
import math
from pathlib import Path
from typing import Any, Iterable

import orjson

from annoterm.models import RowRecord


def _canonicalize(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _canonicalize(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        return [_canonicalize(item) for item in value]
    if isinstance(value, tuple):
        return [_canonicalize(item) for item in value]
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"
    return value


def compute_row_hash(row_data: dict[str, Any]) -> str:
    canonical = _canonicalize(row_data)
    payload = orjson.dumps(canonical, option=orjson.OPT_SORT_KEYS)
    digest = hashlib.sha256(payload).hexdigest()
    return f"sha256:{digest}"


def derive_row_id(row_data: dict[str, Any], row_id_field: str | None = None) -> str | None:
    candidates: list[str] = []
    if row_id_field:
        candidates.append(row_id_field)
    candidates.extend(["id", "row_id", "uuid"])

    for key in candidates:
        if key in row_data and row_data[key] is not None:
            return str(row_data[key])
    return None


def extract_key_fields(row_data: dict[str, Any], key_fields: Iterable[str]) -> dict[str, Any]:
    extracted: dict[str, Any] = {}
    for field in key_fields:
        if field in row_data and row_data[field] is not None:
            extracted[field] = row_data[field]
    return extracted


def build_row_record(
    row_index: int,
    row_data: dict[str, Any],
    row_id_field: str | None = None,
    key_fields: Iterable[str] = (),
) -> RowRecord:
    normalized = {str(key): value for key, value in row_data.items()}
    return RowRecord(
        row_index=row_index,
        row_data=normalized,
        row_id=derive_row_id(normalized, row_id_field=row_id_field),
        key_fields=extract_key_fields(normalized, key_fields),
        row_hash=compute_row_hash(normalized),
    )


def fingerprint_from_path(path: str | Path) -> str:
    file_path = Path(path).expanduser().resolve()
    stat = file_path.stat()
    payload = orjson.dumps(
        {
            "path": str(file_path),
            "size": stat.st_size,
            "mtime_ns": stat.st_mtime_ns,
        },
        option=orjson.OPT_SORT_KEYS,
    )
    digest = hashlib.sha256(payload).hexdigest()
    return f"sha256:{digest}"
