from __future__ import annotations

from annoterm.cli import _default_bundle_dir_for_meta
from annoterm.models import DatasetMeta


def _meta(source_uri: str, fingerprint: str, source_type: str = "csv") -> DatasetMeta:
    return DatasetMeta(
        source_type=source_type,
        source_uri=source_uri,
        split=None,
        fingerprint=fingerprint,
        row_count=10,
        row_id_field=None,
        key_fields=(),
    )


def test_default_bundle_dir_is_dataset_specific() -> None:
    meta_a = _meta("/data/csv_1.csv", "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    meta_b = _meta("/data/csv_2.csv", "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")

    path_a = _default_bundle_dir_for_meta(meta_a)
    path_b = _default_bundle_dir_for_meta(meta_b)

    assert path_a != path_b
    assert str(path_a).startswith(".annoterm/bundles/")
    assert str(path_b).startswith(".annoterm/bundles/")
    assert "csv_1" in str(path_a)
    assert "csv_2" in str(path_b)
