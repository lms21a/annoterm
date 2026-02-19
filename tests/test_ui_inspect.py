from __future__ import annotations

from annoterm.models import RowRecord
from annoterm.ui.app import RowInspectModal, _format_value_for_inspector


def test_format_value_for_inspector_keeps_long_text() -> None:
    long_text = "x" * 500
    rendered = _format_value_for_inspector(long_text)
    assert rendered == long_text


def test_row_inspect_modal_respects_focused_column() -> None:
    row = RowRecord(
        row_index=3,
        row_data={"id": 3, "text": "hello", "label": "ok"},
        row_id="3",
        key_fields={},
        row_hash="sha256:3",
    )
    modal = RowInspectModal(row=row, columns=["id", "text", "label"], focused_column="text")
    assert modal.current_column_name == "text"
