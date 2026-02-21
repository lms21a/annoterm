"""Command line entrypoint for AnnoTerm."""

from __future__ import annotations

import argparse
import contextlib
import io
import shlex
import re
import tarfile
import tempfile
from dataclasses import asdict
from pathlib import Path
from typing import Sequence

import orjson

from annoterm.annotations.io import DEFAULT_QUICK_LABELS, AnnotationBundleStore
from annoterm.annotations.transfer import (
    REQUIRED_BUNDLE_FILES,
    export_bundle,
    import_bundle,
    summarize_bundle,
    validate_bundle_dir,
)
from annoterm.data.factory import create_adapter
from annoterm.models import DatasetMeta
from annoterm.ui.app import DataViewerApp, HomeLauncherApp
from contextlib import redirect_stderr, redirect_stdout


def _build_parser(require_command: bool = True) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="annoterm", description="View and annotate tabular data.")
    subparsers = parser.add_subparsers(dest="command", required=require_command)

    inspect_cmd = _add_common_source_args(
        subparsers.add_parser("inspect", help="Print schema and sample rows.")
    )
    inspect_cmd.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Number of rows to print in the sample section.",
    )
    inspect_cmd.set_defaults(handler=_handle_inspect)

    open_cmd = _add_common_source_args(
        subparsers.add_parser("open", help="Open source in the Textual viewer.")
    )
    open_cmd.add_argument(
        "--load-rows",
        type=int,
        default=200,
        help="Number of rows to materialize in the first viewer page.",
    )
    open_cmd.add_argument(
        "--bundle-dir",
        default=None,
        help=(
            "Optional explicit bundle directory. "
            "If omitted, a dataset-specific path under .annoterm/bundles is used."
        ),
    )
    open_cmd.add_argument(
        "--annotator",
        default=None,
        help="Annotator name written into annotation records (defaults to $USER).",
    )
    open_cmd.add_argument(
        "--task-type",
        default="preference",
        help="Annotation task type stored with each annotation record.",
    )
    open_cmd.add_argument(
        "--quick-label",
        dest="quick_labels",
        action="append",
        default=[],
        help="Repeatable quick label mapped to hotkeys 1..9 in order.",
    )
    open_cmd.set_defaults(handler=_handle_open)

    export_cmd = subparsers.add_parser("export", help="Export an annotation bundle for sharing.")
    export_cmd.add_argument("bundle_dir", help="Path to the source annotation bundle directory.")
    export_cmd.add_argument(
        "output",
        help="Output path. For --format dir this is a directory. For --format tar, a tarball path.",
    )
    export_cmd.add_argument(
        "--format",
        choices=["dir", "tar"],
        default="dir",
        help="Export as a directory copy or a .tar.gz archive.",
    )
    export_cmd.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite output path if it already exists.",
    )
    export_cmd.set_defaults(handler=_handle_export)

    inspect_bundle_cmd = subparsers.add_parser(
        "inspect-bundle",
        help="Inspect annotation bundle metadata, counts, and sample records.",
    )
    inspect_bundle_cmd.add_argument("bundle_dir", help="Path to annotation bundle directory.")
    inspect_bundle_cmd.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Maximum number of annotation records to include in sample output.",
    )
    inspect_bundle_cmd.add_argument(
        "--label",
        default=None,
        help="Optional label filter for summary counts and sample records.",
    )
    inspect_bundle_cmd.add_argument(
        "--annotator",
        default=None,
        help="Optional annotator filter for summary counts and sample records.",
    )
    inspect_bundle_cmd.add_argument(
        "--task-type",
        default=None,
        help="Optional task type filter for summary counts and sample records.",
    )
    inspect_bundle_cmd.set_defaults(handler=_handle_inspect_bundle)

    import_cmd = subparsers.add_parser("import", help="Import annotations from another bundle.")
    import_cmd.add_argument("target_bundle_dir", help="Target bundle directory to merge into.")
    import_cmd.add_argument(
        "source",
        help="Source bundle directory or .tar.gz export file.",
    )
    import_cmd.add_argument(
        "--allow-fingerprint-mismatch",
        action="store_true",
        help="Allow merge even when dataset fingerprints differ.",
    )
    import_cmd.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview merge result without writing target bundle files.",
    )
    import_cmd.set_defaults(handler=_handle_import)

    return parser


def _tokenize_home_command(raw_command: str) -> list[str]:
    value = (raw_command or "").strip()
    if not value:
        return []
    if value.startswith("/"):
        value = value[1:].strip()
    if not value:
        return []
    return shlex.split(value)


def _add_common_source_args(command: argparse.ArgumentParser) -> argparse.ArgumentParser:
    command.add_argument("source", help="Path to CSV/JSONL, or HF dataset name.")
    command.add_argument(
        "--type",
        dest="source_type",
        choices=["csv", "jsonl", "hf"],
        default=None,
        help="Optional explicit source type.",
    )
    command.add_argument(
        "--split",
        default=None,
        help="HF split name (defaults to train when source type is hf).",
    )
    command.add_argument("--config", default=None, help="HF dataset config/name.")
    command.add_argument(
        "--id-field",
        default=None,
        help="Preferred field name to use for stable row_id.",
    )
    command.add_argument(
        "--key-field",
        dest="key_fields",
        action="append",
        default=[],
        help="Repeatable key field used for row matching across refreshes.",
    )
    return command


def _create_adapter_from_args(args: argparse.Namespace):
    return create_adapter(
        source=args.source,
        source_type=args.source_type,
        split=args.split,
        config=args.config,
        row_id_field=args.id_field,
        key_fields=tuple(args.key_fields),
    )


def _print_json(payload: object) -> None:
    print(orjson.dumps(payload, option=orjson.OPT_INDENT_2).decode("utf-8"))


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", value).strip("_").lower()
    return slug or "dataset"


def _default_bundle_dir_for_meta(meta: DatasetMeta) -> Path:
    if meta.source_type in {"csv", "jsonl"}:
        source_name = Path(meta.source_uri).stem or Path(meta.source_uri).name
    else:
        source_name = meta.source_uri
    if meta.split:
        source_name = f"{source_name}_{meta.split}"

    fingerprint = meta.fingerprint.split(":", maxsplit=1)[-1][:12]
    slug = _slugify(source_name)
    return Path(".annoterm") / "bundles" / f"{slug}_{fingerprint}"


def _handle_inspect(args: argparse.Namespace) -> int:
    adapter = _create_adapter_from_args(args)
    meta = adapter.meta()
    schema = [asdict(column) for column in adapter.schema()]
    rows = [
        {
            "row_index": row.row_index,
            "row_id": row.row_id,
            "key_fields": row.key_fields,
            "row_hash": row.row_hash,
            "row_data": row.row_data,
        }
        for row in adapter.rows(offset=0, limit=max(args.limit, 0))
    ]
    _print_json({"meta": asdict(meta), "schema": schema, "sample_rows": rows})
    return 0


def _handle_open(args: argparse.Namespace) -> int:
    adapter = _create_adapter_from_args(args)
    meta = adapter.meta()
    bundle_dir = Path(args.bundle_dir) if args.bundle_dir else _default_bundle_dir_for_meta(meta)
    quick_labels = tuple(args.quick_labels) if args.quick_labels else DEFAULT_QUICK_LABELS
    store = AnnotationBundleStore(
        bundle_dir=bundle_dir,
        dataset_meta=meta,
        annotator=args.annotator,
        task_type=args.task_type,
        quick_labels=quick_labels,
    )
    try:
        store.ensure_initialized()
    except ValueError as exc:
        suggested = _default_bundle_dir_for_meta(meta)
        suggestion = (
            ""
            if args.bundle_dir is None
            else f" Try a different bundle path, e.g. `--bundle-dir {suggested}`."
        )
        raise SystemExit(f"Failed to initialize annotation bundle: {exc}.{suggestion}") from exc

    app = DataViewerApp(
        adapter=adapter,
        load_rows=max(args.load_rows, 1),
        annotation_store=store,
    )
    app.run()
    return 0


def _handle_export(args: argparse.Namespace) -> int:
    result = export_bundle(
        source_bundle_dir=args.bundle_dir,
        output_path=args.output,
        fmt=args.format,
        overwrite=bool(args.overwrite),
    )
    _print_json(result)
    return 0


def _handle_inspect_bundle(args: argparse.Namespace) -> int:
    result = summarize_bundle(
        bundle_dir=args.bundle_dir,
        limit=max(args.limit, 0),
        label=args.label,
        annotator=args.annotator,
        task_type=args.task_type,
    )
    _print_json(result)
    return 0


def _safe_extract_tar(archive_path: Path, destination: Path) -> None:
    root = destination.resolve()
    with tarfile.open(archive_path, "r:gz") as archive:
        for member in archive.getmembers():
            target = (destination / member.name).resolve()
            if target != root and root not in target.parents:
                raise ValueError(f"Blocked unsafe tar member path: {member.name}")
        archive.extractall(destination)


def _find_extracted_bundle_dir(root: Path) -> Path:
    candidates: list[Path] = []
    for manifest_path in root.rglob("manifest.json"):
        candidate = manifest_path.parent
        if all((candidate / name).exists() for name in REQUIRED_BUNDLE_FILES):
            candidates.append(candidate)
    if not candidates:
        raise ValueError("No valid annotation bundle found in extracted archive.")
    candidates.sort(key=lambda path: (len(path.parts), str(path)))
    return candidates[0]


def _resolve_source_bundle(source: str) -> tuple[Path, contextlib.ExitStack]:
    source_path = Path(source).expanduser().resolve()
    stack = contextlib.ExitStack()

    if source_path.is_dir():
        return validate_bundle_dir(source_path), stack

    if source_path.is_file() and source_path.suffixes[-2:] == [".tar", ".gz"]:
        temp_dir = Path(stack.enter_context(tempfile.TemporaryDirectory(prefix="annoterm_import_")))
        _safe_extract_tar(source_path, temp_dir)
        return validate_bundle_dir(_find_extracted_bundle_dir(temp_dir)), stack

    stack.close()
    raise ValueError(
        "Import source must be an annotation bundle directory or a .tar.gz export archive."
    )


def _handle_import(args: argparse.Namespace) -> int:
    with contextlib.ExitStack() as stack:
        source_bundle, source_cleanup = _resolve_source_bundle(args.source)
        stack.enter_context(source_cleanup)
        result = import_bundle(
            target_bundle_dir=args.target_bundle_dir,
            source_bundle_dir=source_bundle,
            allow_fingerprint_mismatch=bool(args.allow_fingerprint_mismatch),
            dry_run=bool(args.dry_run),
        )
    _print_json(result)
    return 0


def _run_home_command(raw_command: str) -> str:
    tokens = _tokenize_home_command(raw_command)
    if not tokens:
        return "No command entered."

    parser = _build_parser(require_command=False)
    parse_output = io.StringIO()
    try:
        with redirect_stdout(parse_output), redirect_stderr(parse_output):
            parsed = parser.parse_args(tokens)
    except SystemExit:
        message = parse_output.getvalue().strip()
        return message or "Invalid command."

    handler = getattr(parsed, "handler", None)
    if handler is None:
        return "Available commands: open, inspect, inspect-bundle, export, import."

    if parsed.command == "open":
        try:
            _handle_open(parsed)
        except SystemExit as exc:
            return str(exc)
        return "Returned from open."

    command_output = io.StringIO()
    try:
        with redirect_stdout(command_output), redirect_stderr(command_output):
            handler(parsed)
    except Exception as exc:
        return f"{type(exc).__name__}: {exc}"

    return command_output.getvalue().strip() or f"{parsed.command} completed."


def _run_home_mode() -> int:
    status = None
    while True:
        app = HomeLauncherApp(status=status)
        app.run()
        command = app.requested_command
        if command is None:
            return 0
        status = _run_home_command(command)


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser(require_command=False)
    args = parser.parse_args(argv)
    if args.command is None:
        return _run_home_mode()
    return int(args.handler(args))


if __name__ == "__main__":
    raise SystemExit(main())
