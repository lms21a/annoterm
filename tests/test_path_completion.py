from __future__ import annotations

from pathlib import Path

from annoterm.ui.app import _complete_path_in_command, _format_completion_matches


def test_complete_path_in_command_fills_single_match(tmp_path: Path) -> None:
    target = tmp_path / "dataset.csv"
    target.write_text("")

    input_text = f"/open {tmp_path}/data"
    cursor = len(input_text)

    updated_text, updated_cursor, matches = _complete_path_in_command(input_text, cursor)

    assert updated_text == f"/open {target}"
    assert updated_cursor == len(updated_text)
    assert matches == [f"{tmp_path}/dataset.csv"]


def test_complete_path_in_command_returns_multiple_matches(tmp_path: Path) -> None:
    (tmp_path / "alpha.csv").write_text("")
    (tmp_path / "alpha.json").write_text("")

    input_text = f"/open {tmp_path}/alpha"
    cursor = len(input_text)

    updated_text, updated_cursor, matches = _complete_path_in_command(input_text, cursor)

    assert updated_text == input_text
    assert updated_cursor == cursor
    assert matches == [
        f"{tmp_path}/alpha.csv",
        f"{tmp_path}/alpha.json",
    ]


def test_format_completion_matches_shows_preview_and_more_count() -> None:
    matches = [f"/tmp/a{i}" for i in range(10)]
    assert _format_completion_matches(matches).endswith("and 4 more")


def test_format_completion_matches_shows_all_when_small() -> None:
    matches = ["/tmp/a", "/tmp/b"]
    assert _format_completion_matches(matches) == "/tmp/a\n/tmp/b"


def test_complete_path_in_command_ignores_non_path_commands(tmp_path: Path) -> None:
    input_text = f"/help {tmp_path}/"
    cursor = len(input_text)

    updated_text, updated_cursor, matches = _complete_path_in_command(input_text, cursor)

    assert updated_text == input_text
    assert updated_cursor == cursor
    assert matches == []


def test_complete_path_in_command_expands_after_space(tmp_path: Path) -> None:
    target = tmp_path / "bundle"
    target.mkdir()

    input_text = f"/open {tmp_path}/"
    cursor = len(input_text)

    updated_text, updated_cursor, matches = _complete_path_in_command(input_text, cursor)

    assert updated_text == f"/open {tmp_path}/bundle/"
    assert updated_text.endswith(f"{target.name}/")
    assert updated_cursor == len(updated_text)
    assert matches == [f"{tmp_path}/bundle/"]


def test_complete_path_in_command_lists_directories_along_files(tmp_path: Path) -> None:
    (tmp_path / "data.csv").write_text("")
    nested = tmp_path / "data_dir"
    nested.mkdir()

    input_text = f"/open {tmp_path}/data"
    cursor = len(input_text)

    _, _, matches = _complete_path_in_command(input_text, cursor)

    assert f"{tmp_path}/data.csv" in matches
    assert f"{tmp_path}/data_dir/" in matches
