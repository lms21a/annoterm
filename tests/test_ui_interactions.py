from __future__ import annotations

import pytest

from annoterm.data.base import DataAdapter
from annoterm.models import ColumnInfo, RowRecord
from annoterm.ui.app import DataViewerApp


class _RowsAdapter(DataAdapter):
    source_type = "csv"

    def __init__(self, total_rows: int = 10) -> None:
        super().__init__(source_uri="stub.csv")
        self._total_rows = total_rows

    def schema(self) -> list[ColumnInfo]:
        return [ColumnInfo(name="id", dtype="Int64"), ColumnInfo(name="text", dtype="String")]

    def row_count(self, filter_query=None) -> int:
        return self._total_rows

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
        for index in range(offset, min(offset + limit, self._total_rows)):
            row_data = {"id": index, "text": "x" * 240}
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
async def test_uppercase_g_goes_to_bottom() -> None:
    app = DataViewerApp(adapter=_RowsAdapter(total_rows=7), load_rows=3)
    async with app.run_test() as pilot:
        await pilot.press("G")
        assert app._view_row_position == 6


@pytest.mark.anyio
async def test_enter_opens_row_inspector_modal() -> None:
    app = DataViewerApp(adapter=_RowsAdapter(total_rows=4), load_rows=2)
    async with app.run_test() as pilot:
        await pilot.press("enter")
        assert app.screen_stack[-1].__class__.__name__ == "RowInspectModal"
        await pilot.press("escape")
        assert app.screen_stack[-1].__class__.__name__ != "RowInspectModal"
