from __future__ import annotations

import orjson

from annoterm.annotations.io import AnnotationBundleStore
from annoterm.cli import main
from annoterm.data.identity import build_row_record
from annoterm.models import DatasetMeta


def test_cli_inspect_bundle_emits_json_summary(tmp_path, capsys) -> None:
    meta = DatasetMeta(
        source_type="csv",
        source_uri="sample.csv",
        split=None,
        fingerprint="sha256:dataset1",
        row_count=3,
        row_id_field="id",
        key_fields=("id",),
    )
    store = AnnotationBundleStore(
        bundle_dir=tmp_path / "bundle",
        dataset_meta=meta,
        annotator="alice",
        task_type="preference",
        quick_labels=("high-quality", "low-quality"),
    )
    store.ensure_initialized()

    row = build_row_record(
        row_index=0,
        row_data={"id": "r1", "text": "hello"},
        row_id_field="id",
        key_fields=("id",),
    )
    store.append_annotation(row=row, label="high-quality")

    exit_code = main(["inspect-bundle", str(store.bundle_dir), "--limit", "1"])
    assert exit_code == 0

    payload = orjson.loads(capsys.readouterr().out)
    assert payload["counts"]["total_records"] == 1
    assert payload["counts"]["filtered_records"] == 1
    assert payload["stats"]["by_label"] == {"high-quality": 1}
    assert len(payload["sample_records"]) == 1
