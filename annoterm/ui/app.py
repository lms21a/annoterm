"""Textual app for data navigation, filtering, and quick annotations."""

from __future__ import annotations

import json
import shlex
from typing import Any

import orjson
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container
from textual.screen import ModalScreen
from textual.widgets import DataTable, Footer, Header, Input, Static, TextArea

from annoterm.annotations.io import AnnotationBundleStore
from annoterm.data.base import DataAdapter
from annoterm.filters.parser import FilterQuery, parse_filter_expression
from annoterm.models import ColumnInfo, RowRecord
from annoterm.models import SortSpec


HELP_TEXT = """[b]AnnoTerm Help[/b]

[b]Navigation[/b]
j / k (or arrows): move row
h / l: move column
ctrl+d / ctrl+u: page down / up
g / G: top / bottom
Enter: inspect full row + focused cell value

[b]View Controls[/b]
/: open filter input
: open command input
s: sort current column (asc -> desc -> off)
c: hide current column
Shift+C: show all columns
r: reset filter/sort/columns

[b]Annotations[/b]
1..9: apply quick label to focused row

[b]Commands[/b]
row <index>
filter <expr>
sort <column> [asc|desc|none]
hide <column>
show <column>
show-all
cols
reset
help

Press Esc, q, Enter, or ? to close this help.
"""


class HelpModal(ModalScreen[None]):
    """Simple keyboard shortcut and command reference overlay."""

    BINDINGS = [
        Binding("escape", "close", "Close"),
        Binding("q", "close", "Close"),
        Binding("enter", "close", "Close"),
        Binding("question_mark", "close", "Close"),
    ]

    def compose(self) -> ComposeResult:
        with Container(id="help_modal"):
            yield Static(HELP_TEXT, id="help_text")

    def action_close(self) -> None:
        self.dismiss(None)

    CSS = """
    HelpModal {
        align: center middle;
    }
    #help_modal {
        width: 88%;
        max-width: 112;
        height: auto;
        max-height: 90%;
        border: tall $accent;
        padding: 1 2;
        background: $surface;
    }
    #help_text {
        width: 100%;
    }
    """


class RowInspectModal(ModalScreen[None]):
    """Modal with full row JSON and focused cell value."""

    BINDINGS = [
        Binding("escape", "close", "Close"),
        Binding("q", "close", "Close"),
        Binding("enter", "close", "Close"),
    ]

    def __init__(self, title: str, text: str) -> None:
        super().__init__()
        self._title = title
        self._text = text

    def compose(self) -> ComposeResult:
        with Container(id="row_inspect_modal"):
            yield Static(self._title, id="row_inspect_title")
            yield TextArea(
                self._text,
                language="json",
                read_only=True,
                show_cursor=False,
                show_line_numbers=False,
                soft_wrap=False,
                id="row_inspect_text",
            )
            yield Static("Press Esc, q, or Enter to close.", id="row_inspect_hint")

    def action_close(self) -> None:
        self.dismiss(None)

    CSS = """
    RowInspectModal {
        align: center middle;
    }
    #row_inspect_modal {
        width: 92%;
        max-width: 144;
        height: 92%;
        border: tall $accent;
        padding: 1;
        background: $surface;
        layout: vertical;
    }
    #row_inspect_title {
        width: 100%;
        padding: 0 0 1 0;
        text-style: bold;
    }
    #row_inspect_text {
        width: 100%;
        height: 1fr;
    }
    #row_inspect_hint {
        width: 100%;
        padding: 1 0 0 0;
        color: $text-muted;
    }
    """


class DataViewerApp(App[None]):
    """Viewer with virtual paging, filter/sort commands, and quick labels."""

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("question_mark", "show_help", "Help"),
        Binding("slash", "open_filter_bar", "Filter"),
        Binding("colon", "open_command_bar", "Command"),
        Binding("j,down", "move_down", "Down"),
        Binding("k,up", "move_up", "Up"),
        Binding("h,left", "move_left", "Left"),
        Binding("l,right", "move_right", "Right"),
        Binding("ctrl+d", "page_down", "Page Down"),
        Binding("ctrl+u", "page_up", "Page Up"),
        Binding("g", "go_top", "Top"),
        Binding("G,shift+g,end", "go_bottom", "Bottom"),
        Binding("s", "toggle_sort_current_column", "Sort"),
        Binding("c", "toggle_current_column_visibility", "Hide Column"),
        Binding("shift+c", "show_all_columns", "Show Columns"),
        Binding("r", "reset_view_state", "Reset"),
        Binding("1", "quick_label_1", "Label 1"),
        Binding("2", "quick_label_2", "Label 2"),
        Binding("3", "quick_label_3", "Label 3"),
        Binding("4", "quick_label_4", "Label 4"),
        Binding("5", "quick_label_5", "Label 5"),
        Binding("6", "quick_label_6", "Label 6"),
        Binding("7", "quick_label_7", "Label 7"),
        Binding("8", "quick_label_8", "Label 8"),
        Binding("9", "quick_label_9", "Label 9"),
    ]

    def __init__(
        self,
        adapter: DataAdapter,
        load_rows: int = 200,
        annotation_store: AnnotationBundleStore | None = None,
    ) -> None:
        super().__init__()
        self.adapter = adapter
        self.load_rows = load_rows
        self.annotation_store = annotation_store

        self._schema: list[ColumnInfo] = []
        self._schema_by_name: dict[str, ColumnInfo] = {}
        self._loaded_rows: list[RowRecord] = []
        self._command_mode: str | None = None
        self._filter_query: FilterQuery | None = None
        self._sort_spec: SortSpec | None = None
        self._hidden_columns: set[str] = set()
        self._window_start = 0
        self._view_row_position = 0
        self._filtered_row_count = 0

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield DataTable(id="grid")
        yield Input(placeholder="Type command and press Enter", id="command_input")
        yield Footer()

    def on_mount(self) -> None:
        table = self.query_one(DataTable)
        table.cursor_type = "cell"

        self._schema = self.adapter.schema()
        self._schema_by_name = {column.name: column for column in self._schema}

        command_input = self.query_one("#command_input", Input)
        command_input.display = False

        self._refresh_grid(preserve_column=None)

    def on_data_table_cell_highlighted(self, event: DataTable.CellHighlighted) -> None:
        self._view_row_position = self._window_start + event.coordinate.row
        self._refresh_subtitle()

    def on_data_table_cell_selected(self, _: DataTable.CellSelected) -> None:
        self.action_inspect_current_row()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        text = event.value.strip()
        if self._command_mode == "filter":
            self._handle_filter_submit(text)
        elif self._command_mode == "command":
            self._handle_command_submit(text)
        self._close_command_input()

    def on_input_blurred(self, _: Input.Blurred) -> None:
        if self._command_mode is not None:
            self._close_command_input()

    def action_open_filter_bar(self) -> None:
        current_filter = self._filter_query.raw if self._filter_query else ""
        self._open_command_input(mode="filter", value=current_filter, placeholder="filter: column == value")

    def action_open_command_bar(self) -> None:
        self._open_command_input(mode="command", value="", placeholder="command: row 1200")

    def action_show_help(self) -> None:
        self.push_screen(HelpModal())

    def action_inspect_current_row(self) -> None:
        if self._command_mode is not None:
            return
        row = self._current_row()
        if row is None:
            self.notify("No active row is selected.", severity="warning")
            return
        column = self._current_column_name()
        focused_value = row.row_data.get(column) if column else None
        title = f"Row {row.row_index}"
        if row.row_id is not None:
            title = f"{title} | row_id {row.row_id}"
        if column:
            title = f"{title} | column {column}"
        text = self._build_row_inspect_text(row, column, focused_value)
        self.push_screen(RowInspectModal(title=title, text=text))

    def _refresh_subtitle(self, last_action: str | None = None) -> None:
        total_rows = self.adapter.row_count(filter_query=self._filter_query)
        self._filtered_row_count = 0 if total_rows is None else int(total_rows)
        total_rows_display = "?" if total_rows is None else str(total_rows)
        annotations_count = self.annotation_store.annotation_count() if self.annotation_store else 0
        quick_labels = self.annotation_store.quick_label_map if self.annotation_store else {}
        quick_summary = ", ".join(f"{key}:{value}" for key, value in sorted(quick_labels.items()))
        if not quick_summary:
            quick_summary = "none"

        filter_display = self._filter_query.raw if self._filter_query else "none"
        sort_display = (
            f"{self._sort_spec.column}:{'desc' if self._sort_spec.descending else 'asc'}"
            if self._sort_spec
            else "none"
        )
        visible_columns = self._visible_column_names()

        subtitle = (
            f"{self.adapter.source_type}:{self.adapter.source_uri} "
            f"| row {self._view_row_position}/{total_rows_display} "
            f"| loaded {len(self._loaded_rows)} "
            f"| cols {len(visible_columns)}/{len(self._schema)} "
            f"| filter {filter_display} | sort {sort_display} "
            f"| annotations {annotations_count} | quick {quick_summary}"
        )
        if last_action:
            subtitle = f"{subtitle} | last {last_action}"
        self.sub_title = subtitle

    def _format_cell(self, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, (dict, list, tuple)):
            dumped = orjson.dumps(value).decode("utf-8")
            return dumped if len(dumped) <= 120 else f"{dumped[:117]}..."
        stringified = str(value)
        return stringified if len(stringified) <= 120 else f"{stringified[:117]}..."

    def _build_row_inspect_text(
        self,
        row: RowRecord,
        focused_column: str | None,
        focused_value: Any,
    ) -> str:
        row_payload = {
            "row_index": row.row_index,
            "row_id": row.row_id,
            "key_fields": row.key_fields,
            "row_hash": row.row_hash,
            "row_data": row.row_data,
        }
        try:
            row_json = orjson.dumps(
                row_payload,
                option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS,
            ).decode("utf-8")
        except TypeError:
            row_json = json.dumps(row_payload, indent=2, sort_keys=True, default=str)

        lines: list[str] = [
            f"row_index: {row.row_index}",
            f"row_id: {row.row_id}",
            f"row_hash: {row.row_hash}",
        ]
        if focused_column:
            lines.extend(
                [
                    f"focused_column: {focused_column}",
                    "focused_value:",
                    self._stringify_full_value(focused_value),
                ]
            )
        lines.extend(["", "full_row_json:", row_json])
        return "\n".join(lines)

    def _stringify_full_value(self, value: Any) -> str:
        if value is None:
            return "null"
        if isinstance(value, (dict, list, tuple)):
            try:
                return orjson.dumps(
                    value,
                    option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS,
                ).decode("utf-8")
            except TypeError:
                return json.dumps(value, indent=2, sort_keys=True, default=str)
        return str(value)

    def _current_row(self) -> RowRecord | None:
        table = self.query_one(DataTable)
        cursor_row = table.cursor_row
        if cursor_row < 0 or cursor_row >= len(self._loaded_rows):
            return None
        return self._loaded_rows[cursor_row]

    def _current_column_name(self) -> str | None:
        table = self.query_one(DataTable)
        column_index = table.cursor_column
        visible_columns = self._visible_column_names()
        if not visible_columns:
            return None
        if column_index < 0 or column_index >= len(visible_columns):
            return visible_columns[0]
        return visible_columns[column_index]

    def _visible_column_names(self) -> list[str]:
        return [column.name for column in self._schema if column.name not in self._hidden_columns]

    def _open_command_input(self, mode: str, value: str, placeholder: str) -> None:
        command_input = self.query_one("#command_input", Input)
        command_input.display = True
        command_input.value = value
        command_input.placeholder = placeholder
        self._command_mode = mode
        command_input.focus()

    def _close_command_input(self) -> None:
        command_input = self.query_one("#command_input", Input)
        command_input.display = False
        command_input.value = ""
        self._command_mode = None
        self.query_one(DataTable).focus()

    def _handle_filter_submit(self, text: str) -> None:
        if not text:
            self._filter_query = None
            self._view_row_position = 0
            self._refresh_grid(last_action="filter cleared")
            return
        try:
            self._filter_query = parse_filter_expression(text)
        except ValueError as exc:
            self.notify(f"Invalid filter: {exc}", severity="error")
            return
        self._view_row_position = 0
        self._refresh_grid(last_action=f"filter {text}")

    def _handle_command_submit(self, text: str) -> None:
        if not text:
            return
        try:
            tokens = shlex.split(text)
        except ValueError as exc:
            self.notify(f"Invalid command input: {exc}", severity="error")
            return
        if not tokens:
            return

        command = tokens[0].lower()
        args = tokens[1:]

        if command == "row":
            if len(args) != 1:
                self.notify("Usage: row <index>", severity="warning")
                return
            self._jump_to_row(args[0])
            return
        if command == "filter":
            self._handle_filter_submit(" ".join(args))
            return
        if command in {"clear-filter", "nofilter"}:
            self._filter_query = None
            self._view_row_position = 0
            self._refresh_grid(last_action="filter cleared")
            return
        if command == "sort":
            self._command_sort(args)
            return
        if command == "hide":
            if len(args) != 1:
                self.notify("Usage: hide <column>", severity="warning")
                return
            self._hide_column(args[0])
            return
        if command == "show":
            if len(args) != 1:
                self.notify("Usage: show <column>", severity="warning")
                return
            self._show_column(args[0])
            return
        if command in {"show-all", "columns-reset"}:
            self._hidden_columns.clear()
            self._refresh_grid(last_action="columns reset")
            return
        if command in {"cols", "columns"}:
            self._notify_column_state()
            return
        if command == "inspect":
            self.action_inspect_current_row()
            return
        if command == "reset":
            self.action_reset_view_state()
            return
        if command == "help":
            self.action_show_help()
            return

        self.notify(f"Unknown command: {command}", severity="warning")

    def _command_sort(self, args: list[str]) -> None:
        if not args:
            self.notify("Usage: sort <column> [asc|desc|none]", severity="warning")
            return
        column = args[0]
        if column not in self._schema_by_name:
            self.notify(f"Unknown column: {column}", severity="warning")
            return
        if len(args) == 1:
            self._sort_spec = SortSpec(column=column, descending=False)
        else:
            direction = args[1].lower()
            if direction == "asc":
                self._sort_spec = SortSpec(column=column, descending=False)
            elif direction == "desc":
                self._sort_spec = SortSpec(column=column, descending=True)
            elif direction in {"none", "off"}:
                self._sort_spec = None
            else:
                self.notify("Sort direction must be asc|desc|none.", severity="warning")
                return
        self._view_row_position = 0
        self._refresh_grid(last_action=f"sort {column}")

    def _hide_column(self, name: str) -> None:
        if name not in self._schema_by_name:
            self.notify(f"Unknown column: {name}", severity="warning")
            return
        visible = self._visible_column_names()
        if name in self._hidden_columns:
            return
        if len(visible) <= 1:
            self.notify("Cannot hide the last visible column.", severity="warning")
            return
        self._hidden_columns.add(name)
        self._refresh_grid(last_action=f"hide {name}")

    def _show_column(self, name: str) -> None:
        if name not in self._schema_by_name:
            self.notify(f"Unknown column: {name}", severity="warning")
            return
        if name in self._hidden_columns:
            self._hidden_columns.remove(name)
            self._refresh_grid(last_action=f"show {name}")

    def _notify_column_state(self) -> None:
        visible = self._visible_column_names()
        hidden = sorted(self._hidden_columns)
        hidden_text = ", ".join(hidden) if hidden else "none"
        self.notify(
            f"Visible columns ({len(visible)}): {', '.join(visible)} | Hidden: {hidden_text}",
            timeout=6,
        )

    def _jump_to_row(self, raw_index: str) -> None:
        try:
            index = int(raw_index)
        except ValueError:
            self.notify("Row index must be an integer.", severity="warning")
            return
        max_index = max(self._filtered_row_count - 1, 0)
        index = min(max(index, 0), max_index)
        self._view_row_position = index
        self._refresh_grid(last_action=f"row {index}")

    def _refresh_grid(self, preserve_column: str | None = None, last_action: str | None = None) -> None:
        table = self.query_one(DataTable)
        current_column = preserve_column or self._current_column_name()
        visible_columns = self._visible_column_names()
        if not visible_columns:
            self._hidden_columns.clear()
            visible_columns = self._visible_column_names()

        total_rows = self.adapter.row_count(filter_query=self._filter_query)
        self._filtered_row_count = 0 if total_rows is None else int(total_rows)

        if self._filtered_row_count <= 0:
            self._window_start = 0
            self._view_row_position = 0
            self._loaded_rows = []
        else:
            self._view_row_position = min(max(self._view_row_position, 0), self._filtered_row_count - 1)
            if not (self._window_start <= self._view_row_position < self._window_start + self.load_rows):
                self._window_start = (self._view_row_position // self.load_rows) * self.load_rows
            self._loaded_rows = self.adapter.rows(
                offset=self._window_start,
                limit=self.load_rows,
                visible_columns=visible_columns,
                filter_query=self._filter_query,
                sort=self._sort_spec,
            )

        table.clear(columns=True)
        for column_name in visible_columns:
            table.add_column(column_name)

        for row in self._loaded_rows:
            rendered = [self._format_cell(row.row_data.get(column_name)) for column_name in visible_columns]
            table.add_row(*rendered, key=str(row.row_index))

        if self._loaded_rows:
            cursor_row = min(self._view_row_position - self._window_start, len(self._loaded_rows) - 1)
            if current_column in visible_columns:
                cursor_col = visible_columns.index(current_column)
            else:
                cursor_col = min(table.cursor_column, max(len(visible_columns) - 1, 0))
            table.move_cursor(row=max(cursor_row, 0), column=max(cursor_col, 0), animate=False, scroll=False)
        self._refresh_subtitle(last_action=last_action)

    def _move_vertical(self, delta: int) -> None:
        if self._filtered_row_count <= 0:
            return
        new_position = min(
            max(self._view_row_position + delta, 0),
            self._filtered_row_count - 1,
        )
        if new_position == self._view_row_position:
            return
        self._view_row_position = new_position
        if self._window_start <= new_position < self._window_start + len(self._loaded_rows):
            table = self.query_one(DataTable)
            table.move_cursor(
                row=new_position - self._window_start,
                column=table.cursor_column,
                animate=False,
                scroll=True,
            )
            self._refresh_subtitle()
        else:
            self._refresh_grid()

    def action_move_down(self) -> None:
        self._move_vertical(1)

    def action_move_up(self) -> None:
        self._move_vertical(-1)

    def action_page_down(self) -> None:
        step = max(self.load_rows // 2, 1)
        self._move_vertical(step)

    def action_page_up(self) -> None:
        step = max(self.load_rows // 2, 1)
        self._move_vertical(-step)

    def action_go_top(self) -> None:
        self._view_row_position = 0
        self._refresh_grid(last_action="top")

    def action_go_bottom(self) -> None:
        if self._filtered_row_count <= 0:
            return
        self._view_row_position = self._filtered_row_count - 1
        self._refresh_grid(last_action="bottom")

    def action_move_left(self) -> None:
        table = self.query_one(DataTable)
        if table.cursor_column > 0:
            table.move_cursor(column=table.cursor_column - 1, animate=False, scroll=True)

    def action_move_right(self) -> None:
        table = self.query_one(DataTable)
        max_col = len(self._visible_column_names()) - 1
        if table.cursor_column < max_col:
            table.move_cursor(column=table.cursor_column + 1, animate=False, scroll=True)

    def action_toggle_sort_current_column(self) -> None:
        column = self._current_column_name()
        if column is None:
            return
        if self._sort_spec is None or self._sort_spec.column != column:
            self._sort_spec = SortSpec(column=column, descending=False)
        elif not self._sort_spec.descending:
            self._sort_spec = SortSpec(column=column, descending=True)
        else:
            self._sort_spec = None
        self._view_row_position = 0
        self._refresh_grid(last_action=f"sort {column}")

    def action_toggle_current_column_visibility(self) -> None:
        column = self._current_column_name()
        if column is None:
            return
        if column in self._hidden_columns:
            self._hidden_columns.remove(column)
            self._refresh_grid(last_action=f"show {column}")
            return
        visible = self._visible_column_names()
        if len(visible) <= 1:
            self.notify("Cannot hide the last visible column.", severity="warning")
            return
        self._hidden_columns.add(column)
        self._refresh_grid(last_action=f"hide {column}")

    def action_show_all_columns(self) -> None:
        self._hidden_columns.clear()
        self._refresh_grid(last_action="show all columns")

    def action_reset_view_state(self) -> None:
        self._filter_query = None
        self._sort_spec = None
        self._hidden_columns.clear()
        self._view_row_position = 0
        self._window_start = 0
        self._refresh_grid(last_action="view reset")

    def _apply_quick_label(self, key: str) -> None:
        if not self.annotation_store:
            self.notify("Annotation store is not configured.", severity="warning")
            return

        label = self.annotation_store.quick_label_for_key(key)
        if not label:
            self.notify(f"No quick label assigned to key {key}.", severity="warning")
            return

        row = self._current_row()
        if not row:
            self.notify("No active row is selected.", severity="warning")
            return

        self.annotation_store.append_annotation(row=row, label=label)
        self._refresh_subtitle(last_action=f"row {row.row_index} -> {label}")
        self.notify(f"Annotated row {row.row_index} as '{label}'.")

    def action_quick_label_1(self) -> None:
        self._apply_quick_label("1")

    def action_quick_label_2(self) -> None:
        self._apply_quick_label("2")

    def action_quick_label_3(self) -> None:
        self._apply_quick_label("3")

    def action_quick_label_4(self) -> None:
        self._apply_quick_label("4")

    def action_quick_label_5(self) -> None:
        self._apply_quick_label("5")

    def action_quick_label_6(self) -> None:
        self._apply_quick_label("6")

    def action_quick_label_7(self) -> None:
        self._apply_quick_label("7")

    def action_quick_label_8(self) -> None:
        self._apply_quick_label("8")

    def action_quick_label_9(self) -> None:
        self._apply_quick_label("9")
