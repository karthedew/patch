from __future__ import annotations

from pathlib import Path

import orjson

from patch.repos import RepoConfig
from patch.filters import passes_filters

SYSTEM_PROMPT = (
    "You are an expert software engineer. Given a GitHub issue and repository "
    "context, produce the minimal unified diff that resolves the issue. "
    "Output only the diff."
)


def format_record(record: dict) -> dict:
    pr_merged_at = record.get("pr_merged_at") or record.get("merged_at") or ""
    user_content = (
        f"Repository: {record.get('repo', '')}\n"
        f"Language: {record.get('language', '')}\n"
        f"Domain: {record.get('domain', '')}\n\n"
        f"Issue #{record.get('issue_number', '')}: {record.get('issue_title', '')}\n\n"
        f"{(record.get('issue_body') or '').strip()}"
    )

    return {
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_content},
            {"role": "assistant", "content": (record.get("diff") or "").strip()},
        ],
        "metadata": {
            "repo": record.get("repo") or "",
            "language": record.get("language") or "",
            "domain": record.get("domain") or "",
            "issue_number": record.get("issue_number") or 0,
            "pr_number": record.get("pr_number") or 0,
            "issue_created_at": record.get("issue_created_at")
            or record.get("issue_created")
            or "",
            "pr_merged_at": pr_merged_at,
            "review_count": int(record.get("review_count") or 0),
            "additions": int(record.get("additions") or 0),
            "deletions": int(record.get("deletions") or 0),
            "base_sha": record.get("base_sha") or "",
            "merge_sha": record.get("merge_sha") or "",
            "merge_parent_shas": record.get("merge_parent_shas") or [],
            "base_branch": record.get("base_branch") or "",
            "closing_pr_confidence": record.get("closing_pr_confidence")
            or "unknown",
            "has_tests": bool(record.get("has_tests") or False),
            "test_files_changed": record.get("test_files_changed") or [],
            "license": record.get("license") or "UNKNOWN",
            "collected_at": record.get("collected_at") or "",
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
    raw_path = raw_dir / f"{cfg.owner}-{cfg.repo}-{cfg.language}.jsonl"
    processed_path = processed_dir / f"{cfg.owner}-{cfg.repo}-{cfg.language}.jsonl"
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
