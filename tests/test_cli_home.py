from __future__ import annotations

from argparse import Namespace
from unittest.mock import Mock

from annoterm import cli


def test_tokenize_home_command_strips_leading_slash_and_quotes() -> None:
    assert cli._tokenize_home_command('/open "my data.csv" --task-type classification') == [
        "open",
        "my data.csv",
        "--task-type",
        "classification",
    ]


def test_run_home_command_runs_open_without_stdout(monkeypatch) -> None:
    handle_open = Mock(return_value=0)
    called: dict[str, object] = {}

    def _capture(args: Namespace) -> int:
        called["source"] = args.source
        return handle_open(args)

    monkeypatch.setattr(cli, "_handle_open", _capture)
    result = cli._run_home_command("/open data.csv")

    assert result == "Returned from open."
    assert called["source"] == "data.csv"
    assert handle_open.call_count == 1


def test_run_home_command_unknown_command_returns_help() -> None:
    status = cli._run_home_command("/does-not-exist")
    assert status.startswith("usage:")
