from __future__ import annotations

from pathlib import Path

from datasets import DatasetDict, load_dataset
from huggingface_hub import HfApi


def build_dataset_card() -> str:
    return """# patch dataset

GitHub issue to unified diff dataset for supervised fine-tuning.

## Splits

- `train`: `train.jsonl`
- `test`: `test.jsonl`

## Record schema

- `messages`: chat-style prompt/response tuple (`system`, `user`, `assistant`)
- `metadata`: repository and tracking fields (`repo`, `language`, `domain`, `issue_number`, `pr_number`, `issue_created_at`, `pr_merged_at`, `review_count`, `additions`, `deletions`, `base_sha`, `merge_sha`, `base_branch`, `closing_pr_confidence`, `has_tests`, `test_files_changed`, `license`, `collected_at`)

## System prompt

`You are an expert software engineer. Given a GitHub issue and repository context, produce the minimal unified diff that resolves the issue. Output only the diff.`

## Notes

- Raw sources are GitHub closed issue and merged PR pairs.
- Assistant output is the merged PR unified diff.
"""


def push_to_hub(hf_upload_dir: Path, repo_id: str, token: str | None = None) -> None:
    train_path = hf_upload_dir / "train.jsonl"
    test_path = hf_upload_dir / "test.jsonl"
    if not train_path.exists() or not test_path.exists():
        raise FileNotFoundError(
            "Missing train.jsonl or test.jsonl in hf_upload directory"
        )

    dataset = DatasetDict(
        {
            "train": load_dataset("json", data_files=str(train_path))["train"],
            "test": load_dataset("json", data_files=str(test_path))["train"],
        }
    )

    dataset.push_to_hub(repo_id, private=False, token=token)

    card = build_dataset_card()
    card_path = hf_upload_dir / "README.md"
    card_path.write_text(card, encoding="utf-8")

    api = HfApi(token=token)
    api.upload_file(
        path_or_fileobj=str(card_path),
        path_in_repo="README.md",
        repo_id=repo_id,
        repo_type="dataset",
    )
