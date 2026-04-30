from __future__ import annotations


def count_changed_lines(diff: str) -> int:
    count = 0
    for line in diff.splitlines():
        if line.startswith("+++") or line.startswith("---"):
            continue
        if line.startswith("+") or line.startswith("-"):
            count += 1
    return count


def passes_filters(record: dict) -> bool:
    issue_body = (record.get("issue_body") or "").strip()
    diff = record.get("diff") or ""
    review_count = int(record.get("review_count") or 0)
    changed_files = record.get("changed_files") or []

    changed_lines = count_changed_lines(diff)

    if len(issue_body) < 100:
        return False
    if changed_lines < 5:
        return False
    if review_count < 1:
        return False
    if len(changed_files) > 10:
        return False
    return True
