## AnnoTerm

Keyboard-first TUI for exploring and annotating data sources.

Current baseline:
- CSV adapter
- JSONL adapter
- Hugging Face dataset adapter
- Textual grid viewer with virtual paging + quick-label annotations (`1..9`)
- Filter, sort, row jump, and column visibility controls
- Row identity hashing
- JSONL-first annotation bundle output (`manifest.json`, `label_set.json`, `annotations.jsonl`)

## Quick start

```bash
source ../.venv/bin/activate
uv run --active annoterm inspect path/to/data.csv
uv run --active annoterm open path/to/data.csv
```

By default, `open` now creates/uses a dataset-specific bundle under:

```text
.annoterm/bundles/<dataset_slug>_<fingerprint12>/
```

This keeps annotations for different datasets separate automatically.

### Annotation bundle options

```bash
uv run --active annoterm open path/to/data.csv \
  --bundle-dir .annoterm/my-bundle \
  --annotator lukas928 \
  --quick-label high-quality \
  --quick-label low-quality \
  --quick-label needs-review
```

In the viewer:
- Press `1..9` to apply the configured quick label to the focused row.
- Records are appended to `<bundle-dir>/annotations.jsonl`.
- Each record stores `row_data` (a snapshot of the annotated row) so exports remain usable even if the original dataset path is gone.
- Press `?` for an on-screen help modal with shortcuts and command reference.
- Navigation keys: `j/k` (up/down), `ctrl+d/u` (page), `g/G` (top/bottom).
- Press `Enter` to inspect the row in a popup, then use `Tab` / `Shift+Tab` to cycle columns.
- In row inspect mode, `1..9` also applies quick-label annotations.
- Press `a` to annotate the current row with any label (new labels are added automatically).
- Press `t` to set/switch the active annotation task, then open task label mode.
- In task label mode, use `Enter` to add labels and `Ctrl+N` / `Ctrl+P` to switch tasks.
- Sorting: `s` toggles sort on the focused column.
- Columns: `c` hides current column, `C` restores all columns.
- Reset view state: `r`.
- Filter bar: `/` then enter expression like `score >= 0.6 and text contains "beta"`.
- Quick column search: `f` opens a `contains` filter pre-filled for the currently selected column.
- `/` and `:` open a centered popup input modal so typing stays visible even in tmux.
- Command bar: `:` then run commands such as:
  - `row 1200`
  - `annotate high-quality`
  - `label needs-review`
  - `labels`
  - `task classification`
  - `tasks`
  - `sort score desc`
  - `hide text`
  - `show text`
  - `show-all`
  - `filter score >= 0.9`
  - `reset`

Hotkey behavior:
- The first nine labels are mapped to quick keys `1..9`.
- Additional labels are still supported and can be applied through `a` or `:annotate <label>`.
- Labels/hotkeys are task-scoped (for example `preference` vs `classification` can have different sets).

### Share and merge bundles

Export bundle folder:

```bash
uv run --active annoterm export .annoterm/my-bundle ./shared-bundle --format dir
```

Export `.tar.gz` artifact:

```bash
uv run --active annoterm export .annoterm/my-bundle ./shared-bundle --format tar
```

Import another bundle (directory or `.tar.gz`) into your local bundle:

```bash
uv run --active annoterm import .annoterm/my-bundle ./shared-bundle.tar.gz
```

Preview merge impact without writing files:

```bash
uv run --active annoterm import .annoterm/my-bundle ./shared-bundle.tar.gz --dry-run
```

Inspect bundle metadata, stats, and sample records:

```bash
uv run --active annoterm inspect-bundle .annoterm/my-bundle --limit 10
```

Filter bundle inspection output:

```bash
uv run --active annoterm inspect-bundle .annoterm/my-bundle \
  --label high-quality \
  --annotator lukas928 \
  --task-type preference
```

The import command performs:
- dataset fingerprint validation (unless `--allow-fingerprint-mismatch`),
- annotation deduplication by `annotation_id`,
- label-set merge,
- conflict counting for rows with multiple labels.

### Hugging Face example

```bash
uv run --active annoterm inspect hf:imdb --type hf --split train --limit 3
```

## Development

```bash
source ../.venv/bin/activate
python -m pytest
```
