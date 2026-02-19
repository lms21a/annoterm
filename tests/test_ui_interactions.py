from __future__ import annotations

from pathlib import Path

import orjson
import pytest

from annoterm.annotations.io import AnnotationBundleStore
from annoterm.data.base import DataAdapter
from annoterm.models import ColumnInfo, RowRecord
from annoterm.ui.app import DataViewerApp


class _RowsAdapter(DataAdapter):
    source_type = "csv"

    def __init__(self, total_rows: int = 10) -> None:
        super().__init__(source_uri="stub.csv")
        self._total_rows = total_rows

    def schema(self) -> list[ColumnInfo]:
        return [
            ColumnInfo(name="id", dtype="Int64"),
            ColumnInfo(name="text", dtype="String"),
            ColumnInfo(name="meta", dtype="String"),
        ]

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
        selected = visible_columns or ["id", "text", "meta"]
        records: list[RowRecord] = []
        for index in range(offset, min(offset + limit, self._total_rows)):
            row_data = {"id": index, "text": "x" * 240, "meta": f"m-{index}"}
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
        modal = app.screen_stack[-1]
        assert modal.__class__.__name__ == "RowInspectModal"
        assert getattr(modal, "current_column_name") == "id"

        await pilot.press("tab")
        assert getattr(modal, "current_column_name") == "text"
        await pilot.press("shift+tab")
        assert getattr(modal, "current_column_name") == "id"

        await pilot.press("escape")
        assert app.screen_stack[-1].__class__.__name__ != "RowInspectModal"


@pytest.mark.anyio
async def test_inspector_modal_can_apply_quick_label(tmp_path: Path) -> None:
    adapter = _RowsAdapter(total_rows=4)
    store = AnnotationBundleStore(
        bundle_dir=tmp_path / "bundle",
        dataset_meta=adapter.meta(),
        annotator="tester",
        quick_labels=("good", "bad"),
    )
    store.ensure_initialized()

    app = DataViewerApp(adapter=adapter, load_rows=2, annotation_store=store)
    async with app.run_test() as pilot:
        await pilot.press("enter")
        await pilot.press("1")
        assert store.annotation_count() == 1
        await pilot.press("escape")

    annotations_path = tmp_path / "bundle" / "annotations.jsonl"
    lines = [line for line in annotations_path.read_text(encoding="utf-8").splitlines() if line]
    assert len(lines) == 1
    payload = orjson.loads(lines[0].encode("utf-8"))
    assert payload["label"] == "good"
