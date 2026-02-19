from __future__ import annotations

import pytest
from textual.widgets import Input

from annoterm.data.base import DataAdapter
from annoterm.models import ColumnInfo, RowRecord
from annoterm.ui.app import DataViewerApp


class _FilterAdapter(DataAdapter):
    source_type = "csv"

    def __init__(self) -> None:
        super().__init__(source_uri="stub.csv")

    def schema(self) -> list[ColumnInfo]:
        return [ColumnInfo(name="id", dtype="Int64"), ColumnInfo(name="text", dtype="String")]

    def row_count(self, filter_query=None) -> int:
        return 3

    def rows(
        self,
        offset: int,
        limit: int,
        visible_columns=None,
        filter_query=None,
        sort=None,
    ) -> list[RowRecord]:
        selected = visible_columns or ["id", "text"]
        records: list[RowRecord] = []
        for index in range(offset, min(offset + limit, 3)):
            row_data = {"id": index, "text": f"value-{index}"}
            row_data = {name: value for name, value in row_data.items() if name in selected}
            records.append(
                RowRecord(
                    row_index=index,
                    row_data=row_data,
                    row_id=str(index),
                    key_fields={},
                    row_hash=f"sha256:{index}",
                )
            )
        return records

    def fingerprint(self) -> str:
        return "sha256:stub"


@pytest.mark.anyio
async def test_filter_input_opens_modal_and_applies_filter() -> None:
    app = DataViewerApp(adapter=_FilterAdapter(), load_rows=2)
    async with app.run_test() as pilot:
        await pilot.press("/")
        assert app.screen_stack[-1].__class__.__name__ == "CommandInputModal"
        assert "| mode /" in app.sub_title

        await pilot.press("i", "d", " ", ">", "=", " ", "1", "enter")

        assert app.screen_stack[-1].__class__.__name__ != "CommandInputModal"
        assert app._filter_query is not None
        assert app._filter_query.raw == "id >= 1"


@pytest.mark.anyio
async def test_f_opens_contains_filter_for_current_column() -> None:
    app = DataViewerApp(adapter=_FilterAdapter(), load_rows=2)
    async with app.run_test() as pilot:
        await pilot.press("f")
        modal = app.screen_stack[-1]
        assert modal.__class__.__name__ == "CommandInputModal"
        input_widget = modal.query_one("#command_modal_input", Input)
        assert input_widget.value == "id contains "
