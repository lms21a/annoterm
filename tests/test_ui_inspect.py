from __future__ import annotations

from unittest.mock import Mock

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


def test_row_inspect_modal_can_copy_focused_entry(monkeypatch) -> None:
    row = RowRecord(
        row_index=1,
        row_data={"id": 1, "text": "hello", "label": "ok"},
        row_id="1",
        key_fields={},
        row_hash="sha256:1",
    )
    copy_mock = Mock(return_value=True)
    monkeypatch.setattr("annoterm.ui.app._copy_text_to_clipboard", copy_mock)

    modal = RowInspectModal(row=row, columns=["id", "text", "label"], focused_column="label")
    modal.notify = lambda *_args, **_kwargs: None
    modal.action_copy_entry()

    assert copy_mock.call_args[0][0] == "ok"
    assert any(
        binding.key == "ctrl+c" and binding.action == "copy_entry"
        for binding in modal.BINDINGS
    )
