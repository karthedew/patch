# patch

`patch` is a Python pipeline for building supervised fine-tuning (SFT) datasets from real GitHub engineering history.

It collects closed GitHub issues and linked merged pull requests, extracts unified diffs, applies quality filters, formats examples into `messages[]` chat records, and publishes train/test splits to Hugging Face.

## What this project does

- Collects issue -> merged PR pairs from curated open-source repositories
- Fetches review counts, changed files, and unified diffs
- Applies quality filters to keep focused, learnable examples
- Formats output for ChatML-style instruction fine-tuning
- Merges and splits into deterministic `train.jsonl` / `test.jsonl`
- Pushes a `DatasetDict` to Hugging Face Hub

## Project layout

```text
patch/
├── pyproject.toml
├── .env.example
├── Makefile
├── config/
│   └── repos.py
├── src/
│   └── patch/
│       ├── collect.py
│       ├── process.py
│       ├── merge.py
│       ├── push.py
│       ├── peek.py
│       ├── manifest.py
│       └── filters.py
├── data/
│   ├── raw/
│   ├── processed/
│   └── hf_upload/
└── scripts/
    ├── run_collect.py
    ├── run_process.py
    ├── run_merge.py
    ├── run_push.py
    └── run_peek.py
```

## Requirements

- Python 3.11+
- [uv](https://docs.astral.sh/uv/)
- GitHub token with API access
- Hugging Face account/token (or CLI login)

## Setup

```bash
make sync
make init-env
```

Then edit `.env` with your values:

- `GITHUB_TOKEN`
- `HF_REPO_ID`
- `HF_TOKEN` (optional if using `hf auth login`)
- `DATA_DIR` (optional, defaults to `./data`)

## Run the pipeline

### All repos

```bash
make collect
make process
make merge
make push
```

### Single repo

```bash
make collect-repo REPO=apache/arrow
make process-repo REPO=apache/arrow
```

### One-command full run

```bash
make pipeline
```

## Peek mode (GraphQL preview)

Use `peek` when you want to inspect GraphQL request structure and one sample issue/PR result without writing files.

```bash
make peek
```

- Hardcoded target: `pola-rs/polars`
- Prints a request example plus a sample normalized record to stdout
- Does not write dataset data to disk

## Output formats

### Raw record (`data/raw/*.jsonl`)

Each line is a JSON object with fields such as:

- `repo`, `language`, `domain`
- `issue_number`, `issue_title`, `issue_body`, `issue_labels`
- `pr_number`, `pr_title`, `pr_body`, `merged_at`
- `diff`, `review_count`, `changed_files`

### Processed record (`data/processed/*.jsonl`)

Each line is formatted for instruction SFT:

- `messages`: `system`, `user`, `assistant`
- `metadata`: repo/language/domain/issue/pr identifiers

### HF upload files (`data/hf_upload/`)

- `train.jsonl`
- `test.jsonl`

## Quality filters

Records are kept only if they pass all checks:

- issue body length >= 100 chars
- changed diff lines between 5 and 500
- review count >= 1
- changed files <= 10

## Resumability and invariants

- `data/raw/` is append-only
- collection is safe to re-run and deduplicates by issue number
- `data/manifest.json` is the source of truth for per-repo progress
- manifest writes are flushed during collection for crash safety
- each stage runs independently from files on disk

## Authentication notes

### GitHub

- `GITHUB_TOKEN` is required for collection and peek commands

### Hugging Face

Push supports either auth path:

1. Set `HF_TOKEN` in `.env`, or
2. Run `hf auth login` and use cached CLI credentials

## Helpful targets

```bash
make help
make check
```

- `make help`: list all available commands
- `make check`: compile Python files for quick syntax validation
