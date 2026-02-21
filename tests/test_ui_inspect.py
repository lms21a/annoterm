from __future__ import annotations

from unittest.mock import Mock

import pytest

from annoterm.data.base import DataAdapter
from annoterm.models import ColumnInfo, RowRecord
from annoterm.ui.app import DataViewerApp, RowInspectModal, _format_value_for_inspector
from textual.widgets import TextArea


class _RowsAdapter(DataAdapter):
    source_type = "csv"

    def __init__(self) -> None:
        super().__init__(source_uri="stub.csv")

    def schema(self) -> list[ColumnInfo]:
        return [ColumnInfo(name="id", dtype="Int64"), ColumnInfo(name="text", dtype="String")]

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
        row_data = {"id": 0, "text": "x" * 20}
        selected = visible_columns or ["id", "text"]
        row_data = {name: value for name, value in row_data.items() if name in selected}
        return [
            RowRecord(
                row_index=0,
                row_data=row_data,
                row_id="0",
                key_fields={},
                row_hash="sha256:0",
            )
        ]

    def fingerprint(self) -> str:
        return "sha256:stub"


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


@pytest.mark.anyio
async def test_inspect_modal_text_wraps_entry_view() -> None:
    app = DataViewerApp(adapter=_RowsAdapter(), load_rows=1)
    async with app.run_test() as pilot:
        await pilot.press("enter")
        modal = app.screen_stack[-1]
        text_area = modal.query_one("#row_inspect_text", TextArea)
        assert text_area.soft_wrap is True
