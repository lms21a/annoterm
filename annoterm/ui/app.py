"""Textual app for data navigation, filtering, and quick annotations."""

from __future__ import annotations

import os
import shlex
import shutil
import subprocess
import sys
from typing import Any, Callable

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
Enter: inspect row (Tab / Shift+Tab to move columns)
In inspect modal, Ctrl+C copies current entry to clipboard.

[b]View Controls[/b]
/: open filter input
f: quick contains filter for current column
: open command input
s: sort current column (asc -> desc -> off)
c: hide current column
Shift+C: show all columns
r: reset filter/sort/columns

[b]Annotations[/b]
1..9: apply quick label to focused row
a: annotate focused row with typed label
t: open task label mode
In inspect modal, 1..9 also annotate current row
In task label mode: Enter add label, `task <name>` to add/switch task, Tab/Shift+Tab switch tasks

[b]Commands[/b]
row <index>
filter <expr>
annotate <label>
label <label>
labels
task <task_type>
tasks
sort <column> [asc|desc|none]
hide <column>
show <column>
show-all
cols
reset
help

Press Esc, q, Enter, or ? to close this help.
"""

HOME_COMMAND_TEXT = """[b]AnnoTerm Home[/b]

Type a command to launch an action.

Available commands:

1) /open <source> [options]
   Open a dataset in the annotation viewer.

2) /inspect <source> [options]
   Show dataset schema and sample rows.

3) /inspect-bundle <bundle-dir>
   Show bundle summary and samples.

4) /export <bundle-dir> <output> [--format dir|tar]
   Export a bundle.

5) /import <target-bundle> <source-bundle-or-tar>
   Merge another bundle into a target.

Start a command with / when you want quick visibility:
- /open path/to/data.csv
- /inspect-bundle .annoterm/my-bundle

Escape or q to quit.
"""


_PATH_AUTOCOMPLETE_COMMANDS = {
    "open",
    "inspect",
    "inspect-bundle",
    "export",
    "import",
}
_MAX_PATH_COMPLETION_PREVIEW = 6


def _complete_path_in_command(value: str, cursor_position: int) -> tuple[str, int, list[str]]:
    if cursor_position < 0:
        cursor_position = 0
    if cursor_position > len(value):
        cursor_position = len(value)

    before = value[:cursor_position]
    if not before.strip():
        return value, cursor_position, []

    tokens = before.split()
    if not tokens:
        return value, cursor_position, []

    command = tokens[0].lstrip("/").lower()
    if command not in _PATH_AUTOCOMPLETE_COMMANDS:
        return value, cursor_position, []

    if before[-1].isspace():
        token_index = len(tokens)
    else:
        token_index = len(tokens) - 1
    if token_index <= 0:
        return value, cursor_position, []

    if before[-1].isspace():
        token_start = cursor_position
        token_end = cursor_position
    else:
        token_start = cursor_position
        while token_start > 0 and not before[token_start - 1].isspace():
            token_start -= 1
        token_end = cursor_position
        while token_end < len(value) and not value[token_end].isspace():
            token_end += 1

    token = value[token_start:token_end]

    quote = ""
    if token and token[0] in {'"', "'"} and token.count(token[0]) % 2 == 1:
        quote = token[0]
        token = token[1:]

    expanded = os.path.expanduser(token)
    if expanded.endswith(os.sep):
        search_dir = expanded
        stem = ""
        raw_prefix = token
    else:
        search_dir = os.path.dirname(expanded)
        stem = os.path.basename(expanded)
        if not search_dir:
            search_dir = "."
        raw_prefix = token[: len(token) - len(stem)] if stem else token

    if not os.path.isdir(search_dir):
        return value, cursor_position, []

    dir_matches: list[str] = []
    file_matches: list[str] = []
    for entry in sorted(os.listdir(search_dir)):
        if not entry.startswith(stem):
            continue
        expanded_entry = os.path.join(search_dir, entry)
        candidate = f"{raw_prefix}{entry}"
        if os.path.isdir(expanded_entry):
            candidate += os.sep
            if quote:
                candidate = f"{quote}{candidate}"
            dir_matches.append(candidate)
            continue
        if quote:
            candidate = f"{quote}{candidate}"
        file_matches.append(candidate)

    matches = sorted(dir_matches) + sorted(file_matches)

    if not matches:
        return value, cursor_position, []

    if len(matches) > 1:
        return value, cursor_position, matches

    completion = matches[0]

    if completion == token and not quote:
        return value, cursor_position, matches

    new_value = value[:token_start] + completion + value[token_end:]
    new_cursor = token_start + len(completion)
    return new_value, new_cursor, matches


def _format_completion_matches(matches: list[str]) -> str:
    if not matches:
        return ""
    preview = matches[:_MAX_PATH_COMPLETION_PREVIEW]
    lines = [match for match in preview]
    remaining = len(matches) - len(preview)
    if remaining > 0:
        lines.append(f"... and {remaining} more")
    return "\n".join(lines)


class PathCompletionInput(Input):
    """Input with shell-like path completion triggered by Tab."""

    def __init__(
        self,
        *args: Any,
        on_completions: Callable[[list[str]], None] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._on_completions = on_completions

    BINDINGS = [Binding("tab", "complete_path", "Complete path", show=False)]

    def on_input_changed(self, event: Input.Changed) -> None:
        del event
        self._notify_completions([])

    def action_complete_path(self) -> None:
        new_value, new_cursor, matches = _complete_path_in_command(
            self.value,
            self.cursor_position,
        )
        if not matches:
            self._notify_completions([])
            return
        if new_value == self.value:
            self._notify_completions(matches)
            return
        self.value = new_value
        self.cursor_position = new_cursor
        self._notify_completions([])

    def _notify_completions(self, matches: list[str]) -> None:
        if self._on_completions is None:
            return
        self._on_completions(matches)


def _format_value_for_inspector(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, (dict, list, tuple)):
        try:
            return orjson.dumps(
                value,
                option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS,
            ).decode("utf-8")
        except TypeError:
            return str(value)
    return str(value)


def _run_clipboard_command(command: list[str], text: str) -> bool:
    try:
        process = subprocess.run(
            command,
            input=text,
            text=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
    except OSError:
        return False
    return process.returncode == 0


def _copy_text_to_clipboard(text: str) -> bool:
    """Try to copy text to the OS clipboard."""
    if sys.platform == "darwin":
        if _run_clipboard_command(["pbcopy"], text):
            return True
    elif sys.platform == "win32":
        if _run_clipboard_command(["clip"], text):
            return True
    else:
        for command in (
            ["wl-copy"],
            ["xclip", "-selection", "clipboard"],
            ["xsel", "--clipboard", "--input"],
        ):
            if shutil.which(command[0]) is None:
                continue
            if _run_clipboard_command(command, text):
                return True

    try:
        import tkinter
    except Exception:
        return False
    root = None
    try:
        root = tkinter.Tk()
        root.withdraw()
        root.clipboard_clear()
        root.clipboard_append(text)
        root.update()
        root.destroy()
        return True
    except Exception:
        try:
            if root is not None:
                root.destroy()
        except Exception:
            pass
        return False


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


class HomeLauncherApp(App[None]):
    """Home screen shown when no command is provided."""

    BINDINGS = [
        Binding("escape", "close", "Close"),
        Binding("q", "close", "Close"),
        Binding("question_mark", "close", "Close"),
    ]

    def __init__(self, status: str | None = None) -> None:
        super().__init__()
        self._status = status or ""
        self._requested_command: str | None = None

    @property
    def requested_command(self) -> str | None:
        return self._requested_command

    def compose(self) -> ComposeResult:
        yield Header(show_clock=False, id="home_header")
        with Container(id="home_screen"):
            yield Static(HOME_COMMAND_TEXT, id="home_command_text")
            if self._status:
                yield Static(self._status, id="home_status")
            yield PathCompletionInput(
                value="",
                placeholder="/open path/to/data.csv",
                on_completions=self._show_home_completions,
                id="home_command_input",
            )
            yield Static("", id="home_command_completions")
            yield Static(
                "Press Enter to run • Esc or q to quit",
                id="home_hint",
            )
        yield Footer()

    def on_mount(self) -> None:
        self.query_one("#home_command_input", Input).focus()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id != "home_command_input":
            return
        command = event.value.strip()
        if not command:
            self.notify("Enter a command to run.")
            return
        self._requested_command = command
        self.exit()

    def action_close(self) -> None:
        self._requested_command = None
        self.exit()

    def _show_home_completions(self, matches: list[str]) -> None:
        completion = self.query_one("#home_command_completions", Static)
        if not matches:
            completion.update("")
            return
        completion.update(_format_completion_matches(matches))

    CSS = """
    HomeLauncherApp {
        align: left top;
    }
    #home_screen {
        width: 100%;
        max-width: 100%;
        padding: 1 1 1 1;
        layout: vertical;
        min-height: 28;
    }
    #home_command_text {
        width: 100%;
        color: $text;
        padding: 0 1 1 1;
    }
    #home_status {
        width: 100%;
        color: $text;
        padding: 0 1 1 1;
        max-height: 6;
        overflow-y: auto;
    }
    #home_command_input {
        width: 100%;
        margin: 0 0 1 0;
    }
    #home_command_completions {
        width: 100%;
        color: $text-muted;
        padding: 0 1 1 1;
        max-height: 10;
        overflow-y: auto;
    }
    #home_hint {
        width: 100%;
        color: $text-muted;
        padding: 1 0 0 0;
    }
    """


class RowInspectModal(ModalScreen[None]):
    """Modal for row-level inspection with per-column value browsing."""

    BINDINGS = [
        Binding("escape", "close", "Close"),
        Binding("q", "close", "Close"),
        Binding("enter", "close", "Close"),
        Binding("tab,right", "next_column", "Next Column", show=False, priority=True),
        Binding("shift+tab,left", "previous_column", "Prev Column", show=False, priority=True),
        Binding("ctrl+c", "copy_entry", "Copy Value", show=False, priority=True),
        Binding("home", "first_column", "First Column", show=False, priority=True),
        Binding("end", "last_column", "Last Column", show=False, priority=True),
        Binding("1", "apply_label_1", "Label 1", show=False, priority=True),
        Binding("2", "apply_label_2", "Label 2", show=False, priority=True),
        Binding("3", "apply_label_3", "Label 3", show=False, priority=True),
        Binding("4", "apply_label_4", "Label 4", show=False, priority=True),
        Binding("5", "apply_label_5", "Label 5", show=False, priority=True),
        Binding("6", "apply_label_6", "Label 6", show=False, priority=True),
        Binding("7", "apply_label_7", "Label 7", show=False, priority=True),
        Binding("8", "apply_label_8", "Label 8", show=False, priority=True),
        Binding("9", "apply_label_9", "Label 9", show=False, priority=True),
    ]

    def __init__(
        self,
        row: RowRecord,
        columns: list[str],
        focused_column: str | None = None,
        quick_label_map: dict[str, str] | None = None,
        on_apply_label: Callable[[str], bool] | None = None,
    ) -> None:
        super().__init__()
        self._row = row
        self._quick_label_map = dict(quick_label_map or {})
        self._on_apply_label = on_apply_label
        deduped_columns: list[str] = []
        for column in columns:
            if column in self._row.row_data and column not in deduped_columns:
                deduped_columns.append(column)
        for column in self._row.row_data:
            if column not in deduped_columns:
                deduped_columns.append(column)
        self._columns = deduped_columns

        if focused_column in self._columns:
            self._column_index = self._columns.index(focused_column)
        else:
            self._column_index = 0

    def compose(self) -> ComposeResult:
        with Container(id="row_inspect_modal"):
            yield Static("", id="row_inspect_title")
            yield Static("", id="row_inspect_column_meta")
            yield TextArea(
                "",
                read_only=True,
                show_cursor=False,
                show_line_numbers=False,
                soft_wrap=True,
                id="row_inspect_text",
            )
            yield Static(
                "Tab/Shift+Tab: change column | Ctrl+C: copy value | Enter/Esc/q: close",
                id="row_inspect_hint",
            )

    @property
    def current_column_name(self) -> str | None:
        if not self._columns:
            return None
        return self._columns[self._column_index]

    def on_mount(self) -> None:
        self._refresh_content()
        self.query_one("#row_inspect_text", TextArea).focus()

    def action_close(self) -> None:
        self.dismiss(None)

    def action_next_column(self) -> None:
        if not self._columns:
            return
        self._column_index = (self._column_index + 1) % len(self._columns)
        self._refresh_content()

    def action_previous_column(self) -> None:
        if not self._columns:
            return
        self._column_index = (self._column_index - 1) % len(self._columns)
        self._refresh_content()

    def _current_entry_value(self) -> str:
        if not self._columns:
            return ""
        column = self._columns[self._column_index]
        return _format_value_for_inspector(self._row.row_data.get(column))

    def action_copy_entry(self) -> None:
        if not self._columns:
            return
        value_text = self._current_entry_value()
        if _copy_text_to_clipboard(value_text):
            self.notify(f"Copied {self.current_column_name} to clipboard.")
        else:
            self.notify("Clipboard not available.", severity="error")

    def action_first_column(self) -> None:
        if not self._columns:
            return
        self._column_index = 0
        self._refresh_content()

    def action_last_column(self) -> None:
        if not self._columns:
            return
        self._column_index = len(self._columns) - 1
        self._refresh_content()

    def _apply_label_key(self, key: str) -> None:
        label = self._quick_label_map.get(key)
        if not label:
            return
        if self._on_apply_label is None:
            return
        self._on_apply_label(label)

    def action_apply_label_1(self) -> None:
        self._apply_label_key("1")

    def action_apply_label_2(self) -> None:
        self._apply_label_key("2")

    def action_apply_label_3(self) -> None:
        self._apply_label_key("3")

    def action_apply_label_4(self) -> None:
        self._apply_label_key("4")

    def action_apply_label_5(self) -> None:
        self._apply_label_key("5")

    def action_apply_label_6(self) -> None:
        self._apply_label_key("6")

    def action_apply_label_7(self) -> None:
        self._apply_label_key("7")

    def action_apply_label_8(self) -> None:
        self._apply_label_key("8")

    def action_apply_label_9(self) -> None:
        self._apply_label_key("9")

    def _refresh_content(self) -> None:
        title = f"Row {self._row.row_index}"
        if self._row.row_id is not None:
            title = f"{title} | row_id {self._row.row_id}"

        if not self._columns:
            column_meta = "No columns in this row."
            value_text = "{}"
        else:
            column = self._columns[self._column_index]
            column_meta = f"Column {self._column_index + 1}/{len(self._columns)}: {column}"
            value_text = self._current_entry_value()

        quick_summary = ", ".join(
            f"{key}:{value}" for key, value in sorted(self._quick_label_map.items())
        )
        if quick_summary:
            hint = (
                "Tab/Shift+Tab: change column | Ctrl+C: copy value | 1..9: annotate row | "
                f"labels {quick_summary} | Enter/Esc/q: close"
            )
        else:
            hint = "Tab/Shift+Tab: change column | Ctrl+C: copy value | Enter/Esc/q: close"

        self.query_one("#row_inspect_title", Static).update(title)
        self.query_one("#row_inspect_column_meta", Static).update(column_meta)
        self.query_one("#row_inspect_text", TextArea).load_text(value_text)
        self.query_one("#row_inspect_hint", Static).update(hint)

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
    #row_inspect_column_meta {
        width: 100%;
        color: $text-muted;
        padding: 0 0 1 0;
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


class CommandInputModal(ModalScreen[str | None]):
    """Popup command/filter input modal."""

    BINDINGS = [
        Binding("escape", "close", "Cancel"),
        Binding("q", "close", "Cancel"),
    ]

    def __init__(
        self,
        mode: str,
        initial_value: str,
        placeholder: str,
    ) -> None:
        super().__init__()
        self._mode = mode
        self._initial_value = initial_value
        self._placeholder = placeholder

    def compose(self) -> ComposeResult:
        if self._mode == "filter":
            mode_label = "Filter"
            prefix = "/"
        elif self._mode == "command":
            mode_label = "Command"
            prefix = ":"
        elif self._mode == "annotate":
            mode_label = "Annotate"
            prefix = "a"
        elif self._mode == "task":
            mode_label = "Task"
            prefix = "t"
        else:
            mode_label = "Input"
            prefix = ""
        with Container(id="command_modal"):
            yield Static(f"{mode_label} ({prefix})", id="command_modal_title")
            yield PathCompletionInput(
                value=self._initial_value,
                placeholder=self._placeholder,
                on_completions=self._show_command_completions,
                id="command_modal_input",
            )
            yield Static("", id="command_modal_completions")
            yield Static(
                "Enter to apply, Esc to cancel.",
                id="command_modal_hint",
            )

    def on_mount(self) -> None:
        self.query_one("#command_modal_input", Input).focus()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "command_modal_input":
            self.dismiss(event.value)

    def action_close(self) -> None:
        self.dismiss(None)

    def _show_command_completions(self, matches: list[str]) -> None:
        completion = self.query_one("#command_modal_completions", Static)
        if not matches:
            completion.update("")
            return
        completion.update(_format_completion_matches(matches))

    CSS = """
    CommandInputModal {
        align: center middle;
    }
    #command_modal {
        width: 84%;
        max-width: 100;
        height: auto;
        border: tall $accent;
        padding: 1;
        background: $surface;
        layout: vertical;
    }
    #command_modal_title {
        width: 100%;
        text-style: bold;
        padding: 0 0 1 0;
    }
    #command_modal_input {
        width: 100%;
    }
    #command_modal_completions {
        width: 100%;
        color: $text-muted;
        padding: 1 0 0 0;
        max-height: 10;
        overflow-y: auto;
    }
    #command_modal_hint {
        width: 100%;
        color: $text-muted;
        padding: 1 0 0 0;
    }
    """


class TaskLabelModal(ModalScreen[None]):
    """Task-scoped label management modal."""

    BINDINGS = [
        Binding("escape", "close", "Close"),
        Binding("tab,right", "next_task", "Next Task"),
        Binding("shift+tab,left", "previous_task", "Prev Task"),
        Binding("ctrl+l", "focus_input", "Add Label"),
    ]

    def __init__(
        self,
        get_state: Callable[[], dict[str, Any]],
        on_add_label: Callable[[str], tuple[str, bool, str | None] | None],
        on_switch_task: Callable[[str], bool],
    ) -> None:
        super().__init__()
        self._get_state = get_state
        self._on_add_label = on_add_label
        self._on_switch_task = on_switch_task

    def compose(self) -> ComposeResult:
        with Container(id="task_mode_modal"):
            yield Static("", id="task_mode_title")
            yield Static("", id="task_mode_tasks")
            yield Static("", id="task_mode_labels")
            yield Input(
                value="",
                placeholder="Label or command: task <name>",
                id="task_mode_add_input",
            )
            yield Static(
                "Enter: add label | task <name>: create/switch task | Tab/Shift+Tab: switch task | Escape: close",
                id="task_mode_hint",
            )

    def on_mount(self) -> None:
        self._refresh_content()
        self.query_one("#task_mode_add_input", Input).focus()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id != "task_mode_add_input":
            return
        raw = event.value.strip()
        if not raw:
            return

        lowered = raw.casefold()
        if lowered.startswith("task "):
            next_task = raw[5:].strip()
            if not next_task:
                self.notify("Usage: task <name>")
                return
            if self._on_switch_task(next_task):
                event.input.value = ""
                self._refresh_content()
                self.notify(f"Active task set to '{next_task}'.")
            return

        result = self._on_add_label(raw)
        if result is None:
            return
        normalized_label, created, hotkey = result
        event.input.value = ""
        self._refresh_content()
        if created and hotkey:
            self.notify(f"Added '{normalized_label}' on key {hotkey}.")
        elif created:
            self.notify(f"Added '{normalized_label}' (no quick key).")
        else:
            self.notify(f"Label '{normalized_label}' already exists.")

    def action_close(self) -> None:
        self.dismiss(None)

    def action_focus_input(self) -> None:
        self.query_one("#task_mode_add_input", Input).focus()

    def action_next_task(self) -> None:
        self._switch_task(offset=1)

    def action_previous_task(self) -> None:
        self._switch_task(offset=-1)

    def _switch_task(self, offset: int) -> None:
        state = self._get_state()
        tasks = list(state.get("task_types") or [])
        active_task = str(state.get("active_task_type") or "")
        if not tasks:
            return
        if active_task in tasks:
            index = tasks.index(active_task)
        else:
            index = 0
        next_task = tasks[(index + offset) % len(tasks)]
        if self._on_switch_task(next_task):
            self._refresh_content()

    def _refresh_content(self) -> None:
        state = self._get_state()
        active_task = str(state.get("active_task_type") or "none")
        tasks = list(state.get("task_types") or [])
        labels = list(state.get("labels") or [])
        hotkeys = dict(state.get("hotkeys") or {})

        tasks_line = " | ".join(
            f"[{task}]" if task == active_task else task for task in tasks
        ) or "none"
        lines = []
        if not labels:
            lines.append("No labels yet for this task.")
        else:
            for index, label in enumerate(labels, start=1):
                assigned = next((key for key, value in hotkeys.items() if value == label), None)
                if assigned:
                    lines.append(f"{index:>2}. ({assigned}) {label}")
                else:
                    lines.append(f"{index:>2}. (.) {label}")
        if len(labels) > 9:
            lines.append("Labels without 1..9 quick keys can still be used via `a` or `:annotate`.")

        self.query_one("#task_mode_title", Static).update(f"Task Label Mode | active: {active_task}")
        self.query_one("#task_mode_tasks", Static).update(f"Tasks (Tab/Shift+Tab): {tasks_line}")
        self.query_one("#task_mode_labels", Static).update("\n".join(lines))

    CSS = """
    TaskLabelModal {
        align: center middle;
    }
    #task_mode_modal {
        width: 88%;
        max-width: 110;
        height: auto;
        max-height: 90%;
        border: tall $accent;
        padding: 1;
        background: $surface;
        layout: vertical;
    }
    #task_mode_title {
        width: 100%;
        text-style: bold;
        padding: 0 0 1 0;
    }
    #task_mode_tasks {
        width: 100%;
        color: $text-muted;
        padding: 0 0 1 0;
    }
    #task_mode_labels {
        width: 100%;
        max-height: 18;
        overflow-y: auto;
    }
    #task_mode_add_input {
        width: 100%;
        margin: 1 0 0 0;
    }
    #task_mode_hint {
        width: 100%;
        color: $text-muted;
        padding: 1 0 0 0;
    }
    """


class DataViewerApp(App[None]):
    """Viewer with virtual paging, filter/sort commands, and quick labels."""

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("question_mark", "show_help", "Help"),
        Binding("slash", "open_filter_bar", "Filter"),
        Binding("f", "contains_filter_current_column", "Find", show=False),
        Binding("colon", "open_command_bar", "Command Palette"),
        Binding("a", "open_annotation_input", "Annotate"),
        Binding("t", "open_task_input", "Task"),
        Binding("j,down", "move_down", "Down", show=False),
        Binding("k,up", "move_up", "Up", show=False),
        Binding("h,left", "move_left", "Left", show=False),
        Binding("l,right", "move_right", "Right", show=False),
        Binding("ctrl+d", "page_down", "Page Down", show=False),
        Binding("ctrl+u", "page_up", "Page Up", show=False),
        Binding("g", "go_top", "Top", show=False),
        Binding("G,shift+g,end", "go_bottom", "Bottom", show=False),
        Binding("s", "toggle_sort_current_column", "Sort", show=False),
        Binding("c", "toggle_current_column_visibility", "Hide Column", show=False),
        Binding("shift+c", "show_all_columns", "Show Columns", show=False),
        Binding("r", "reset_view_state", "Reset", show=False),
        Binding("1", "quick_label_1", "Label 1", show=False),
        Binding("2", "quick_label_2", "Label 2", show=False),
        Binding("3", "quick_label_3", "Label 3", show=False),
        Binding("4", "quick_label_4", "Label 4", show=False),
        Binding("5", "quick_label_5", "Label 5", show=False),
        Binding("6", "quick_label_6", "Label 6", show=False),
        Binding("7", "quick_label_7", "Label 7", show=False),
        Binding("8", "quick_label_8", "Label 8", show=False),
        Binding("9", "quick_label_9", "Label 9", show=False),
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
        yield Header(show_clock=False)
        yield DataTable(id="grid")
        yield Footer()

    def on_mount(self) -> None:
        table = self.query_one(DataTable)
        table.cursor_type = "cell"

        self._schema = self.adapter.schema()
        self._schema_by_name = {column.name: column for column in self._schema}

        self._refresh_grid(preserve_column=None)

    def on_data_table_cell_highlighted(self, event: DataTable.CellHighlighted) -> None:
        self._view_row_position = self._window_start + event.coordinate.row
        self._refresh_subtitle()

    def on_data_table_cell_selected(self, _: DataTable.CellSelected) -> None:
        self.action_inspect_current_row()

    def action_open_filter_bar(self) -> None:
        current_filter = self._filter_query.raw if self._filter_query else ""
        self._open_command_modal(
            mode="filter",
            value=current_filter,
            placeholder="column == value && category >= 5",
        )

    def action_contains_filter_current_column(self) -> None:
        column = self._current_column_name()
        if not column:
            self.notify("No active column is selected.", severity="warning")
            return
        prefix = f"{column} contains "
        if self._filter_query:
            value = f"{self._filter_query.raw} and {prefix}"
        else:
            value = prefix
        self._open_command_modal(
            mode="filter",
            value=value,
            placeholder=f"{column} contains substring",
        )

    def action_open_command_bar(self) -> None:
        self._open_command_modal(
            mode="command",
            value="",
            placeholder="row 1200",
        )

    def action_open_annotation_input(self) -> None:
        if not self.annotation_store:
            self.notify("Annotation store is not configured.", severity="warning")
            return
        if self._current_row() is None:
            self.notify("No active row is selected.", severity="warning")
            return
        self._open_command_modal(
            mode="annotate",
            value="",
            placeholder="high-quality",
        )

    def action_open_task_input(self) -> None:
        if not self.annotation_store:
            self.notify("Annotation store is not configured.", severity="warning")
            return
        self._open_task_mode()

    def action_show_help(self) -> None:
        self.push_screen(HelpModal())

    def action_inspect_current_row(self) -> None:
        if self._command_mode is not None:
            return
        row = self._current_row()
        if row is None:
            self.notify("No active row is selected.", severity="warning")
            return
        focused_column = self._current_column_name()
        self.push_screen(
            RowInspectModal(
                row=row,
                columns=self._ordered_row_columns(row),
                focused_column=focused_column,
                quick_label_map=(
                    self.annotation_store.active_hotkeys() if self.annotation_store else {}
                ),
                on_apply_label=lambda label: self._annotate_row(
                    row=row,
                    label=label,
                    source="inspect",
                ),
            )
        )

    def _refresh_subtitle(self, last_action: str | None = None) -> None:
        total_rows_display = str(self._filtered_row_count)
        annotations_count = self.annotation_store.annotation_count() if self.annotation_store else 0
        task_display = self.annotation_store.active_task_type() if self.annotation_store else "none"
        source_text = self.adapter.source_uri
        if len(source_text) > 48:
            source_text = f"...{source_text[-45:]}"

        parts = [
            f"src {source_text}",
            f"row {self._view_row_position}/{total_rows_display}",
            f"task {task_display}",
            f"annotations {annotations_count}",
        ]
        if self._filter_query:
            filter_text = self._filter_query.raw
            if len(filter_text) > 36:
                filter_text = f"{filter_text[:33]}..."
            parts.append(f"filter {filter_text}")
        if self._sort_spec:
            direction = "desc" if self._sort_spec.descending else "asc"
            parts.append(f"sort {self._sort_spec.column}:{direction}")

        subtitle = " | ".join(parts)
        mode_display = {
            "filter": "/",
            "command": ":",
            "annotate": "a",
            "task": "t",
        }
        if self._command_mode in mode_display:
            subtitle = f"{subtitle} | mode {mode_display[self._command_mode]}"
        if last_action:
            action_text = last_action if len(last_action) <= 32 else f"{last_action[:29]}..."
            subtitle = f"{subtitle} | last {action_text}"
        self.sub_title = subtitle

    def _format_cell(self, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, (dict, list, tuple)):
            dumped = orjson.dumps(value).decode("utf-8")
            return dumped if len(dumped) <= 120 else f"{dumped[:117]}..."
        stringified = str(value)
        return stringified if len(stringified) <= 120 else f"{stringified[:117]}..."

    def _ordered_row_columns(self, row: RowRecord) -> list[str]:
        schema_columns = [column.name for column in self._schema if column.name in row.row_data]
        extras = [column for column in row.row_data if column not in schema_columns]
        return schema_columns + extras

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

    def _open_command_modal(self, mode: str, value: str, placeholder: str) -> None:
        self._command_mode = mode
        self._refresh_subtitle()
        self.push_screen(
            CommandInputModal(mode=mode, initial_value=value, placeholder=placeholder),
            callback=lambda result: self._on_command_modal_dismiss(mode, result),
        )

    def _on_command_modal_dismiss(self, mode: str, value: str | None) -> None:
        self._command_mode = None
        self._refresh_subtitle()
        if value is None:
            return
        text = value.strip()
        if mode == "filter":
            self._handle_filter_submit(text)
        elif mode == "command":
            self._handle_command_submit(text)
        elif mode == "annotate":
            self._handle_annotate_submit(text)
        elif mode == "task":
            self._handle_task_submit(text)

    def _handle_filter_submit(self, text: str) -> None:
        if not text:
            self._filter_query = None
            self._view_row_position = 0
            self._refresh_grid(last_action="filter cleared")
            return
        try:
            query = parse_filter_expression(text)
        except ValueError as exc:
            self.notify(f"Invalid filter: {exc}", severity="error")
            return
        if query is not None:
            invalid_columns = [
                condition.column
                for condition in query.conditions
                if condition.column not in self._schema_by_name
            ]
            if invalid_columns:
                unknown_columns = ", ".join(dict.fromkeys(invalid_columns))
                self.notify(f"Unknown column(s): {unknown_columns}", severity="error")
                return
            self._filter_query = query
        else:
            self._filter_query = None
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
        if command in {"annotate", "ann"}:
            if not args:
                self.notify("Usage: annotate <label>", severity="warning")
                return
            self._handle_annotate_submit(" ".join(args))
            return
        if command in {"label", "label-add"}:
            if not args:
                self.notify("Usage: label <name>", severity="warning")
                return
            self._register_label(" ".join(args), source="label")
            return
        if command == "labels":
            self._notify_label_state()
            return
        if command in {"task", "profile"}:
            if not args:
                self._open_task_mode()
                return
            self._handle_task_submit(" ".join(args))
            return
        if command in {"tasks", "task-mode"}:
            self._open_task_mode()
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

    def _handle_annotate_submit(self, text: str) -> None:
        label = text.strip()
        if not label:
            return
        row = self._current_row()
        if row is None:
            self.notify("No active row is selected.", severity="warning")
            return
        self._annotate_row(row=row, label=label, source="annotate")

    def _handle_task_submit(self, text: str) -> None:
        if not self.annotation_store:
            self.notify("Annotation store is not configured.", severity="warning")
            return
        task_type = text.strip()
        if not task_type:
            return
        try:
            active = self.annotation_store.set_task_type(task_type)
        except ValueError as exc:
            self.notify(f"Invalid task type: {exc}", severity="warning")
            return
        self._refresh_subtitle(last_action=f"task {active}")
        self.notify(f"Active task set to '{active}'.")
        self._open_task_mode()

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

    def _notify_label_state(self) -> None:
        if not self.annotation_store:
            self.notify("Annotation store is not configured.", severity="warning")
            return
        labels = self.annotation_store.labels()
        quick_map = self.annotation_store.active_hotkeys()
        quick_summary = ", ".join(f"{key}:{value}" for key, value in sorted(quick_map.items()))
        quick_text = quick_summary if quick_summary else "none"
        preview = ", ".join(labels[:8])
        if len(labels) > 8:
            preview = f"{preview}, +{len(labels) - 8} more"
        self.notify(
            (
                f"Task '{self.annotation_store.active_task_type()}' | "
                f"labels ({len(labels)}): {preview or 'none'} | "
                f"quick {quick_text}"
            ),
            timeout=6,
        )

    def _open_task_mode(self) -> None:
        if not self.annotation_store:
            self.notify("Annotation store is not configured.", severity="warning")
            return
        self.push_screen(
            TaskLabelModal(
                get_state=self._task_mode_state,
                on_add_label=self._add_label_from_task_mode,
                on_switch_task=self._switch_task_from_task_mode,
            )
        )

    def _task_mode_state(self) -> dict[str, Any]:
        if not self.annotation_store:
            return {
                "active_task_type": "none",
                "task_types": [],
                "labels": [],
                "hotkeys": {},
            }
        return {
            "active_task_type": self.annotation_store.active_task_type(),
            "task_types": self.annotation_store.task_types(),
            "labels": self.annotation_store.labels(),
            "hotkeys": self.annotation_store.active_hotkeys(),
        }

    def _add_label_from_task_mode(self, label: str) -> tuple[str, bool, str | None] | None:
        result = self._register_label(label, source="task-mode")
        if result is not None:
            self._refresh_subtitle(last_action=f"task-label {result[0]}")
        return result

    def _switch_task_from_task_mode(self, task_type: str) -> bool:
        if not self.annotation_store:
            return False
        try:
            active = self.annotation_store.set_task_type(task_type)
        except ValueError:
            return False
        self._refresh_subtitle(last_action=f"task {active}")
        return True

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
                visible_columns=None,
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

        self._annotate_row(row=row, label=label, source="grid")

    def _annotate_row(self, row: RowRecord, label: str, source: str) -> bool:
        if not self.annotation_store:
            self.notify("Annotation store is not configured.", severity="warning")
            return False
        registered = self._register_label(label, source=source)
        if registered is None:
            return False
        normalized_label, added, assigned_hotkey = registered

        self.annotation_store.append_annotation(row=row, label=normalized_label)
        self._refresh_subtitle(last_action=f"{source} row {row.row_index} -> {normalized_label}")
        if added and assigned_hotkey:
            self.notify(
                (
                    f"Annotated row {row.row_index} as '{normalized_label}' "
                    f"(new label, key {assigned_hotkey})."
                )
            )
        elif added:
            self.notify(
                (
                    f"Annotated row {row.row_index} as '{normalized_label}' "
                    "(new label, no quick key available)."
                )
            )
        else:
            self.notify(f"Annotated row {row.row_index} as '{normalized_label}'.")
        return True

    def _register_label(self, label: str, source: str) -> tuple[str, bool, str | None] | None:
        if not self.annotation_store:
            self.notify("Annotation store is not configured.", severity="warning")
            return None
        try:
            normalized, assigned_hotkey, created = self.annotation_store.ensure_label(label)
        except ValueError as exc:
            self.notify(f"Invalid label: {exc}", severity="warning")
            return None

        if created and source == "label":
            if assigned_hotkey:
                self.notify(f"Added label '{normalized}' on key {assigned_hotkey}.")
            else:
                self.notify(f"Added label '{normalized}' (no quick key available).")
            self._refresh_subtitle(last_action=f"label {normalized}")
        elif not created and source == "label":
            self.notify(f"Label '{normalized}' already exists.")
            self._refresh_subtitle(last_action=f"label {normalized}")

        return normalized, created, assigned_hotkey

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
