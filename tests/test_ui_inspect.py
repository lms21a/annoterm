from __future__ import annotations

from annoterm.data.base import DataAdapter
from annoterm.models import ColumnInfo, RowRecord
from annoterm.ui.app import DataViewerApp


class _StubAdapter(DataAdapter):
    source_type = "csv"

    def schema(self) -> list[ColumnInfo]:
        return [ColumnInfo(name="text", dtype="String")]

    def row_count(self, filter_query=None) -> int:
        return 1

    def rows(
        self,
        offset: int,
        limit: int,
        visible_columns=None,
        filter_query=None,
        sort=None,
    ) -> list[RowRecord]:
        return []

    def fingerprint(self) -> str:
        return "sha256:test"


def test_build_row_inspect_text_contains_full_value() -> None:
    adapter = _StubAdapter(source_uri="stub.csv")
    app = DataViewerApp(adapter=adapter)

    long_value = "x" * 500
    row = RowRecord(
        row_index=7,
        row_data={"text": long_value},
        row_id="r7",
        key_fields={},
        row_hash="sha256:abc",
    )

    inspect_text = app._build_row_inspect_text(row, "text", long_value)

    assert "focused_column: text" in inspect_text
    assert long_value in inspect_text
    assert "full_row_json:" in inspect_text
