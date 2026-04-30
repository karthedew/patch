# patch-sft

`patch-sft` is a Python pipeline for building supervised fine-tuning (SFT) datasets from real GitHub engineering history.

It collects closed GitHub issues and linked merged pull requests, extracts unified diffs, applies quality filters, formats examples into `messages[]` chat records, and publishes train/test splits to Hugging Face.

## Using the published dataset

If you just want the pre-built dataset, no GitHub token or local pipeline needed:

```python
import patch

ds = patch.load("your-hf-username/patch-sft")
print(ds)
# DatasetDict({
#     train: Dataset({...}),
#     test:  Dataset({...}),
# })

df = ds["train"].to_pandas()
```

Or install and use it as a dependency in another project:

```toml
# pyproject.toml
dependencies = ["patch-sft>=0.1.0"]
```

```python
import patch

# Download from HuggingFace Hub
ds = patch.load("your-hf-username/patch-sft")

# Train split only
train_ds = patch.load("your-hf-username/patch-sft", split="train")
```

## Running your own collection

To collect from GitHub and build your own dataset:

### Requirements

- Python 3.11+
- [uv](https://docs.astral.sh/uv/)
- GitHub personal access token
- Hugging Face account/token (for pushing)

### Setup

```bash
git clone https://github.com/your-username/patch-sft
cd patch-sft
make sync
make init-env
```

Edit `.env` with your values:

```bash
GITHUB_TOKEN=ghp_...        # required for collection
HF_REPO_ID=yourname/patch-sft
HF_TOKEN=hf_...             # optional if using `huggingface-cli login`
DATA_DIR=./data             # optional, defaults to ./data
PYPI_API_TOKEN=pypi-...     # only needed for `make publish`
```

### Run the pipeline

```bash
# All repos
make collect
make process
make merge
make push

# Single repo
make collect-repo REPO=apache/arrow
make process-repo REPO=apache/arrow

# Full pipeline in one shot
make pipeline
```

### Programmatic collection

```python
import patch
from patch import RepoConfig

patch.collect(
    repos=[
        RepoConfig("apache", "arrow", "python", "data-engineering"),
        RepoConfig("fastapi", "fastapi", "python", "web"),
    ],
    token="ghp_...",      # or set GITHUB_TOKEN env var
    data_dir="./data",
)
```

### CLI

After installation, the `patch-sft` command is available:

```bash
patch-sft collect [--repo owner/repo]
patch-sft process [--repo owner/repo]
patch-sft merge
patch-sft push
patch-sft peek
```

### Peek mode

Preview GraphQL request structure and one sample record without writing any files:

```bash
make peek
# or
patch-sft peek
```

Hardcoded target is `pola-rs/polars`. Prints a request example and a normalized sample record to stdout.

## Project layout

```text
patch-sft/
├── pyproject.toml
├── .env.example
├── Makefile
├── src/
│   └── patch/
│       ├── __init__.py     # public API: collect(), load(), RepoConfig, REPOS
│       ├── cli.py          # patch-sft CLI entry point
│       ├── repos.py        # curated repo list and RepoConfig
│       ├── collect.py      # GitHub GraphQL + REST collection
│       ├── process.py      # quality filters and SFT formatting
│       ├── merge.py        # train/test split → Parquet
│       ├── push.py         # HuggingFace Hub upload
│       ├── peek.py         # single-sample GraphQL preview
│       ├── manifest.py     # per-repo progress tracking
│       └── filters.py      # record quality filters
└── data/
    ├── raw/                # per-repo JSONL (append-only)
    ├── processed/          # filtered and formatted JSONL
    └── hf_upload/          # train.parquet / test.parquet
```

## Output formats

### Raw record (`data/raw/*.jsonl`)

Each line is a JSON object:

- `repo`, `language`, `domain`, `license`, `collected_at`
- `issue_number`, `issue_title`, `issue_body`, `issue_labels`, `issue_created_at`
- `pr_number`, `pr_title`, `pr_body`, `pr_merged_at`, `base_branch`, `base_sha`, `merge_sha`, `closing_pr_confidence`
- `diff`, `review_count`, `changed_files`, `additions`, `deletions`, `has_tests`, `test_files_changed`

Failed diff fetches are written to `data/raw/*.errors.jsonl` and retried automatically on the next run.

### Processed record (`data/processed/*.jsonl`)

Formatted for instruction SFT:

- `messages`: `[{role: system}, {role: user}, {role: assistant}]`
- `metadata`: repo/language/domain identifiers plus issue/PR provenance fields

### HF upload (`data/hf_upload/`)

- `train.parquet`
- `test.parquet`

## Quality filters

Records are kept only if they pass all checks:

- Issue body length ≥ 100 characters
- Changed diff lines >= 5 (no upper limit)
- At least 1 PR review
- Changed files ≤ 10

## Resumability

- `data/raw/` is append-only; collection deduplicates by issue number
- `data/manifest.json` tracks per-repo progress and is flushed during collection
- Each pipeline stage is independent and reads from files on disk
- Re-running collection is safe — already-collected issues are skipped

## Publishing to PyPI

```bash
make publish
```

Requires `~/.pypirc` with a valid PyPI token (username `__token__`, password `pypi-...`), or `PYPI_API_TOKEN` set in your environment.
