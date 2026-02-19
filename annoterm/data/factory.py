"""Factory for selecting the appropriate data adapter."""

from __future__ import annotations

from pathlib import Path
from typing import Sequence

from annoterm.data.base import DataAdapter
from annoterm.data.csv_adapter import CSVAdapter
from annoterm.data.hf_adapter import HFAdapter
from annoterm.data.jsonl_adapter import JSONLAdapter


def _normalize_source_type(source_type: str | None) -> str | None:
    if source_type is None:
        return None
    lowered = source_type.strip().lower()
    aliases = {
        "csv": "csv",
        "jsonl": "jsonl",
        "ndjson": "jsonl",
        "hf": "hf",
        "huggingface": "hf",
        "datasets": "hf",
    }
    if lowered not in aliases:
        valid = ", ".join(sorted(aliases))
        raise ValueError(f"Unsupported source type '{source_type}'. Valid: {valid}")
    return aliases[lowered]


def _infer_source_type(source: str) -> str:
    source_path = Path(source).expanduser()
    if source_path.exists():
        suffix = source_path.suffix.lower()
        if suffix == ".csv":
            return "csv"
        if suffix in {".jsonl", ".ndjson"}:
            return "jsonl"
        raise ValueError(
            f"Cannot infer adapter from extension '{suffix}'. "
            "Use --type to choose one explicitly."
        )
    return "hf"


def create_adapter(
    source: str,
    source_type: str | None = None,
    split: str | None = None,
    config: str | None = None,
    row_id_field: str | None = None,
    key_fields: Sequence[str] = (),
) -> DataAdapter:
    normalized_type = _normalize_source_type(source_type) or _infer_source_type(source)
    key_fields_tuple = tuple(key_fields)

    if normalized_type == "csv":
        return CSVAdapter(path=source, row_id_field=row_id_field, key_fields=key_fields_tuple)
    if normalized_type == "jsonl":
        return JSONLAdapter(path=source, row_id_field=row_id_field, key_fields=key_fields_tuple)
    if normalized_type == "hf":
        hf_source = source[3:] if source.startswith("hf:") else source
        return HFAdapter(
            dataset_name=hf_source,
            split=split or "train",
            config_name=config,
            row_id_field=row_id_field,
            key_fields=key_fields_tuple,
        )
    raise AssertionError(f"Unhandled source type: {normalized_type}")
