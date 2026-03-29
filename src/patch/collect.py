from __future__ import annotations

import asyncio
from collections.abc import Iterable
from pathlib import Path
import time
from typing import Any

import aiofiles
import aiohttp
import orjson
from tenacity import retry, stop_after_attempt, wait_exponential

from config.repos import RepoConfig
from .manifest import Manifest

GITHUB_API = "https://api.github.com"
WORKER_COUNT = 8


class RetryableGitHubError(RuntimeError):
    pass


def _headers(
    token: str, *, accept: str = "application/vnd.github+json"
) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": accept,
        "X-GitHub-Api-Version": "2022-11-28",
    }


async def respect_rate_limit(response: aiohttp.ClientResponse) -> None:
    remaining = int(response.headers.get("X-RateLimit-Remaining", "1"))
    if remaining >= 10:
        return

    reset_at = int(response.headers.get("X-RateLimit-Reset", "0"))
    sleep_for = max(0, reset_at - int(time.time())) + 2
    if sleep_for > 0:
        await asyncio.sleep(sleep_for)


@retry(wait=wait_exponential(min=1, max=60), stop=stop_after_attempt(5), reraise=True)
async def get_json(
    session: aiohttp.ClientSession,
    url: str,
    *,
    token: str,
    params: dict[str, Any] | None = None,
    accept: str = "application/vnd.github+json",
) -> Any:
    async with session.get(
        url, params=params, headers=_headers(token, accept=accept)
    ) as response:
        await respect_rate_limit(response)
        if response.status in {403, 429, 500, 502, 503, 504}:
            raise RetryableGitHubError(f"Retryable status {response.status} for {url}")
        if response.status >= 400:
            text = await response.text()
            raise RuntimeError(
                f"GitHub API error {response.status} for {url}: {text[:200]}"
            )
        return await response.json()


@retry(wait=wait_exponential(min=1, max=60), stop=stop_after_attempt(5), reraise=True)
async def get_text(
    session: aiohttp.ClientSession,
    url: str,
    *,
    token: str,
    accept: str,
) -> str:
    async with session.get(url, headers=_headers(token, accept=accept)) as response:
        await respect_rate_limit(response)
        if response.status in {403, 429, 500, 502, 503, 504}:
            raise RetryableGitHubError(f"Retryable status {response.status} for {url}")
        if response.status >= 400:
            text = await response.text()
            raise RuntimeError(
                f"GitHub API error {response.status} for {url}: {text[:200]}"
            )
        return await response.text()


async def _load_existing_issue_numbers(path: Path) -> set[int]:
    if not path.exists():
        return set()

    seen: set[int] = set()
    async with aiofiles.open(path, "rb") as handle:
        async for line in handle:
            line = line.strip()
            if not line:
                continue
            obj = orjson.loads(line)
            issue_number = obj.get("issue_number")
            if issue_number is not None:
                seen.add(int(issue_number))
    return seen


async def _append_jsonl(path: Path, record: dict, write_lock: asyncio.Lock) -> None:
    async with write_lock:
        async with aiofiles.open(path, "ab") as handle:
            await handle.write(orjson.dumps(record) + b"\n")


async def pr_from_commit(
    session: aiohttp.ClientSession, cfg: RepoConfig, token: str, commit_sha: str
) -> dict[str, Any] | None:
    url = f"{GITHUB_API}/repos/{cfg.owner}/{cfg.repo}/commits/{commit_sha}/pulls"
    pulls = await get_json(session, url, token=token)
    for pr in pulls:
        if pr.get("merged_at"):
            return pr
    return None


async def fetch_pr(
    session: aiohttp.ClientSession, cfg: RepoConfig, token: str, pr_number: int
) -> dict[str, Any]:
    url = f"{GITHUB_API}/repos/{cfg.owner}/{cfg.repo}/pulls/{pr_number}"
    return await get_json(session, url, token=token)


async def fetch_diff(
    session: aiohttp.ClientSession, cfg: RepoConfig, token: str, pr_number: int
) -> str:
    url = f"{GITHUB_API}/repos/{cfg.owner}/{cfg.repo}/pulls/{pr_number}"
    return await get_text(
        session, url, token=token, accept="application/vnd.github.v3.diff"
    )


async def fetch_review_count(
    session: aiohttp.ClientSession,
    cfg: RepoConfig,
    token: str,
    pr_number: int,
) -> int:
    url = f"{GITHUB_API}/repos/{cfg.owner}/{cfg.repo}/pulls/{pr_number}/reviews"
    reviews = await get_json(session, url, token=token, params={"per_page": 100})
    return len(reviews)


async def fetch_changed_files(
    session: aiohttp.ClientSession,
    cfg: RepoConfig,
    token: str,
    pr_number: int,
) -> list[str]:
    files: list[str] = []
    page = 1
    while True:
        url = f"{GITHUB_API}/repos/{cfg.owner}/{cfg.repo}/pulls/{pr_number}/files"
        payload = await get_json(
            session, url, token=token, params={"per_page": 100, "page": page}
        )
        if not payload:
            break
        files.extend(
            item.get("filename", "") for item in payload if item.get("filename")
        )
        page += 1
    return files


async def find_closing_pr(
    session: aiohttp.ClientSession,
    cfg: RepoConfig,
    token: str,
    issue_number: int,
) -> dict[str, Any] | None:
    url = f"{GITHUB_API}/repos/{cfg.owner}/{cfg.repo}/issues/{issue_number}/timeline"
    events = await get_json(
        session,
        url,
        token=token,
        accept="application/vnd.github.mockingbird-preview+json",
    )

    for event in reversed(events):
        event_type = event.get("event")

        if event_type == "cross-referenced":
            source_issue = event.get("source", {}).get("issue", {})
            if source_issue.get("pull_request") and source_issue.get("number"):
                pr = await fetch_pr(session, cfg, token, int(source_issue["number"]))
                if pr.get("merged_at"):
                    return pr

        if event_type == "closed" and event.get("commit_id"):
            pr = await pr_from_commit(session, cfg, token, event["commit_id"])
            if pr and pr.get("number"):
                full_pr = await fetch_pr(session, cfg, token, int(pr["number"]))
                if full_pr.get("merged_at"):
                    return full_pr

    return None


def _issue_labels(issue: dict[str, Any]) -> list[str]:
    labels = issue.get("labels") or []
    return [label.get("name", "") for label in labels if label.get("name")]


async def _issue_record(
    session: aiohttp.ClientSession,
    cfg: RepoConfig,
    token: str,
    issue: dict[str, Any],
) -> dict[str, Any] | None:
    issue_number = int(issue["number"])
    pr = await find_closing_pr(session, cfg, token, issue_number)
    if not pr:
        return None

    pr_number = int(pr["number"])
    merged_at = pr.get("merged_at")
    if not merged_at:
        return None

    diff, review_count, changed_files = await asyncio.gather(
        fetch_diff(session, cfg, token, pr_number),
        fetch_review_count(session, cfg, token, pr_number),
        fetch_changed_files(session, cfg, token, pr_number),
    )

    return {
        "repo": cfg.full_name,
        "language": cfg.language,
        "domain": cfg.domain,
        "issue_number": issue_number,
        "issue_title": issue.get("title") or "",
        "issue_body": issue.get("body") or "",
        "issue_labels": _issue_labels(issue),
        "issue_created": issue.get("created_at") or "",
        "pr_number": pr_number,
        "pr_title": pr.get("title") or "",
        "pr_body": pr.get("body") or "",
        "diff": diff,
        "merged_at": merged_at,
        "review_count": review_count,
        "changed_files": changed_files,
    }


async def _worker(
    name: str,
    session: aiohttp.ClientSession,
    cfg: RepoConfig,
    token: str,
    queue: asyncio.Queue,
    out_path: Path,
    write_lock: asyncio.Lock,
    seen_issues: set[int],
    stats: dict[str, int],
) -> None:
    while True:
        issue = await queue.get()
        try:
            if issue is None:
                return

            issue_number = int(issue["number"])
            if issue_number in seen_issues:
                continue

            try:
                record = await _issue_record(session, cfg, token, issue)
            except Exception as exc:
                print(f"[{cfg.full_name}][{name}] issue {issue_number} failed: {exc}")
                stats["failed"] += 1
                continue

            if not record:
                stats["skipped"] += 1
                continue

            await _append_jsonl(out_path, record, write_lock)
            seen_issues.add(issue_number)
            stats["written"] += 1
        finally:
            queue.task_done()


def _iter_non_pr_issues(issues: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    return [issue for issue in issues if "pull_request" not in issue]


async def collect_repo(
    session: aiohttp.ClientSession,
    cfg: RepoConfig,
    manifest: Manifest,
    output_dir: Path,
    token: str,
) -> dict[str, int]:
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"{cfg.owner}-{cfg.repo}.jsonl"
    state = manifest.get_repo_state(cfg)
    since_issue = manifest.get_last_issue(cfg) if state.get("complete", False) else 0
    seen_issues = await _load_existing_issue_numbers(out_path)

    queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
    write_lock = asyncio.Lock()
    stats = {"written": 0, "skipped": 0, "failed": 0}

    workers = [
        asyncio.create_task(
            _worker(
                f"w{i + 1}",
                session,
                cfg,
                token,
                queue,
                out_path,
                write_lock,
                seen_issues,
                stats,
            )
        )
        for i in range(WORKER_COUNT)
    ]

    page = 1
    done = False
    while True:
        url = f"{GITHUB_API}/repos/{cfg.owner}/{cfg.repo}/issues"
        issues = await get_json(
            session,
            url,
            token=token,
            params={"state": "closed", "per_page": 100, "page": page},
        )
        if not issues:
            break

        non_pr_issues = _iter_non_pr_issues(issues)
        eligible: list[dict[str, Any]] = []
        page_min: int | None = None

        for issue in non_pr_issues:
            issue_number = int(issue["number"])
            if issue_number <= since_issue:
                done = True
                break
            eligible.append(issue)
            page_min = issue_number if page_min is None else min(page_min, issue_number)

        for issue in eligible:
            await queue.put(issue)

        await queue.join()

        if page_min is not None:
            manifest.update(
                cfg,
                last_issue_number=page_min,
                raw_count=len(seen_issues),
                complete=False,
            )

        if done:
            break
        page += 1

    for _ in workers:
        await queue.put(None)
    await queue.join()
    await asyncio.gather(*workers)

    max_seen = max(seen_issues) if seen_issues else 0
    manifest.update(
        cfg, raw_count=len(seen_issues), complete=True, last_issue_number=max_seen
    )

    print(
        f"[{cfg.full_name}] complete: +{stats['written']} written, "
        f"{stats['skipped']} skipped, {stats['failed']} failed"
    )
    return stats
