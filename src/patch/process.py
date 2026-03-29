from __future__ import annotations

from pathlib import Path

import orjson

from config.repos import RepoConfig
from .filters import passes_filters

SYSTEM_PROMPT = (
    "You are an expert software engineer. Given a GitHub issue and repository "
    "context, produce the minimal unified diff that resolves the issue. "
    "Output only the diff."
)


def format_record(record: dict) -> dict:
    user_content = (
        f"Repository: {record['repo']}\n"
        f"Language: {record['language']}\n"
        f"Domain: {record['domain']}\n\n"
        f"Issue #{record['issue_number']}: {record['issue_title']}\n\n"
        f"{(record.get('issue_body') or '').strip()}"
    )

    return {
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_content},
            {"role": "assistant", "content": (record.get("diff") or "").strip()},
        ],
        "metadata": {
            "repo": record["repo"],
            "language": record["language"],
            "domain": record["domain"],
            "issue_number": record["issue_number"],
            "pr_number": record["pr_number"],
            "merged_at": record["merged_at"],
        },
    }


def _load_existing_issue_numbers(path: Path) -> set[int]:
    if not path.exists():
        return set()

    seen: set[int] = set()
    with open(path, "rb") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            obj = orjson.loads(line)
            issue_number = obj.get("metadata", {}).get("issue_number")
            if issue_number is not None:
                seen.add(int(issue_number))
    return seen


def process_repo(
    cfg: RepoConfig, raw_dir: Path, processed_dir: Path
) -> tuple[int, int]:
    raw_path = raw_dir / f"{cfg.owner}-{cfg.repo}.jsonl"
    processed_path = processed_dir / f"{cfg.owner}-{cfg.repo}.jsonl"
    processed_dir.mkdir(parents=True, exist_ok=True)

    if not raw_path.exists():
        return 0, 0

    seen_issues = _load_existing_issue_numbers(processed_path)
    total = 0
    kept = 0

    with open(raw_path, "rb") as in_handle, open(processed_path, "ab") as out_handle:
        for line in in_handle:
            line = line.strip()
            if not line:
                continue
            total += 1
            record = orjson.loads(line)

            issue_number = int(record.get("issue_number") or 0)
            if issue_number in seen_issues:
                continue
            if not passes_filters(record):
                continue

            formatted = format_record(record)
            out_handle.write(orjson.dumps(formatted) + b"\n")
            seen_issues.add(issue_number)
            kept += 1

    return total, kept
