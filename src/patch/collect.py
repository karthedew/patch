from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path
import sys
import time
from typing import Any

import aiofiles
import aiohttp
import orjson
from tenacity import retry, stop_after_attempt, wait_exponential

from config.repos import RepoConfig
from .manifest import Manifest

GITHUB_API = "https://api.github.com"
GITHUB_GRAPHQL = "https://api.github.com/graphql"

COLLECT_QUERY = """
query CollectIssues($owner: String!, $repo: String!, $first: Int!, $after: String) {
  rateLimit {
    cost
    remaining
    resetAt
  }
  repository(owner: $owner, name: $repo) {
    licenseInfo { spdxId }
    issues(first: $first, after: $after, states: CLOSED, orderBy: { field: CREATED_AT, direction: DESC }) {
      pageInfo { hasNextPage endCursor }
      nodes {
        number
        title
        body
        createdAt
        labels(first: 20) { nodes { name } }
        timelineItems(first: 100, itemTypes: [CLOSED_EVENT, CROSS_REFERENCED_EVENT]) {
          nodes {
            __typename
            ... on ClosedEvent {
              closer {
                __typename
                ... on PullRequest {
                  number
                  title
                  body
                  mergedAt
                  additions
                  deletions
                  baseRefName
                  baseRefOid
                  mergeCommit { oid }
                  reviews(first: 1) { totalCount }
                  files(first: 100) { nodes { path } }
                }
              }
            }
            ... on CrossReferencedEvent {
              source {
                __typename
                ... on PullRequest {
                  number
                  title
                  body
                  mergedAt
                  additions
                  deletions
                  baseRefName
                  baseRefOid
                  mergeCommit { oid }
                  reviews(first: 1) { totalCount }
                  files(first: 100) { nodes { path } }
                }
              }
            }
          }
        }
      }
    }
  }
}
"""


class RetryableGitHubError(RuntimeError):
    pass


def _is_rate_limited_error(errors: list[dict[str, Any]]) -> bool:
    for error in errors:
        if error.get("type") == "RATE_LIMITED":
            return True
        message = str(error.get("message") or "").lower()
        if "rate limit" in message:
            return True
    return False


def _parse_reset_at(value: str | None) -> int | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return int(dt.timestamp())


async def _sleep_with_state(
    sleep_for: int, reason: str, stats: dict | None = None
) -> None:
    if sleep_for <= 0:
        return
    if stats is not None:
        stats["state"] = f"sleeping {sleep_for}s ({reason})"
    await asyncio.sleep(sleep_for)
    if stats is not None:
        stats["state"] = "calling"


def _headers(
    token: str, *, accept: str = "application/vnd.github+json"
) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": accept,
        "X-GitHub-Api-Version": "2022-11-28",
    }


async def respect_rate_limit(
    response: aiohttp.ClientResponse, stats: dict | None = None
) -> None:
    retry_after = response.headers.get("Retry-After")
    if retry_after is not None:
        sleep_for = int(retry_after) + 1
        await _sleep_with_state(sleep_for, "secondary limit", stats)
        return

    remaining = int(response.headers.get("X-RateLimit-Remaining", "100"))
    if remaining > 0:
        return

    reset_at = int(response.headers.get("X-RateLimit-Reset", "0"))
    sleep_for = max(0, reset_at - int(time.time())) + 2
    await _sleep_with_state(sleep_for, "rate limit", stats)


async def _respect_graphql_budget(
    data: dict[str, Any], stats: dict | None = None
) -> None:
    rate_limit = data.get("rateLimit")
    if not isinstance(rate_limit, dict):
        return

    cost = int(rate_limit.get("cost") or 0)
    remaining = int(rate_limit.get("remaining") or 0)
    if remaining > max(1, cost):
        return

    reset_at = _parse_reset_at(rate_limit.get("resetAt"))
    sleep_for = 60 if reset_at is None else max(0, reset_at - int(time.time())) + 2
    await _sleep_with_state(sleep_for, "graphql budget", stats)


@retry(wait=wait_exponential(min=1, max=60), stop=stop_after_attempt(5), reraise=True)
async def _graphql(
    session: aiohttp.ClientSession,
    token: str,
    query: str,
    variables: dict[str, Any],
    stats: dict | None = None,
) -> dict[str, Any]:
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    payload = {"query": query, "variables": variables}
    async with session.post(
        GITHUB_GRAPHQL, headers=headers, data=orjson.dumps(payload)
    ) as response:
        await respect_rate_limit(response, stats)
        if stats is not None:
            stats["api_calls"] += 1
        text = await response.text()
        if response.status in {403, 429, 500, 502, 503, 504}:
            raise RetryableGitHubError(
                f"GraphQL request failed ({response.status}): {text[:300]}"
            )
        if response.status >= 400:
            raise RuntimeError(
                f"GraphQL request failed ({response.status}): {text[:300]}"
            )
        data = orjson.loads(text)
        if "errors" in data:
            errors = data["errors"]
            if _is_rate_limited_error(errors):
                retry_after = response.headers.get("Retry-After")
                if retry_after is not None:
                    sleep_for = int(retry_after) + 1
                else:
                    reset_at = int(response.headers.get("X-RateLimit-Reset", "0") or 0)
                    sleep_for = (
                        max(0, reset_at - int(time.time())) + 2 if reset_at else 60
                    )
                await _sleep_with_state(sleep_for, "graphql rate limit", stats)
            raise RetryableGitHubError(f"GraphQL errors: {data['errors']}")
        await _respect_graphql_budget(data.get("data") or {}, stats)
        await asyncio.sleep(1)
        return data["data"]


@retry(wait=wait_exponential(min=1, max=60), stop=stop_after_attempt(5), reraise=True)
async def get_text(
    session: aiohttp.ClientSession,
    url: str,
    *,
    token: str,
    accept: str,
    stats: dict | None = None,
) -> str:
    async with session.get(url, headers=_headers(token, accept=accept)) as response:
        await respect_rate_limit(response, stats)
        if stats is not None:
            stats["api_calls"] += 1
        if response.status in {403, 429, 500, 502, 503, 504}:
            raise RetryableGitHubError(f"Retryable status {response.status} for {url}")
        if response.status >= 400:
            text = await response.text()
            raise RuntimeError(
                f"GitHub API error {response.status} for {url}: {text[:200]}"
            )
        text = await response.text()
        await asyncio.sleep(1)
        return text


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


def _extract_pr_from_issue(issue: dict[str, Any]) -> tuple[dict[str, Any], str] | None:
    """Return the first merged PR that closes this issue, plus confidence label."""
    timeline_nodes = issue.get("timelineItems", {}).get("nodes", [])
    for node in timeline_nodes:
        typename = node.get("__typename")
        if typename == "ClosedEvent":
            closer = node.get("closer")
            if (
                closer
                and closer.get("__typename") == "PullRequest"
                and closer.get("mergedAt")
            ):
                return closer, "direct_close"
        if typename == "CrossReferencedEvent":
            source = node.get("source")
            if (
                source
                and source.get("__typename") == "PullRequest"
                and source.get("mergedAt")
            ):
                return source, "cross_reference"
    return None


def _build_record(
    cfg: RepoConfig,
    issue: dict[str, Any],
    pr: dict[str, Any],
    confidence: str,
    diff: str,
    license_spdx_id: str,
) -> dict[str, Any]:
    labels = [
        node["name"]
        for node in issue.get("labels", {}).get("nodes", [])
        if node.get("name")
    ]
    changed_files = [
        node["path"]
        for node in pr.get("files", {}).get("nodes", [])
        if node.get("path")
    ]
    test_files_changed = [path for path in changed_files if "test" in path.lower()]

    return {
        "repo": cfg.full_name,
        "language": cfg.language,
        "domain": cfg.domain,
        "issue_number": int(issue["number"]),
        "issue_title": issue.get("title") or "",
        "issue_body": issue.get("body") or "",
        "issue_labels": labels,
        "issue_created_at": issue.get("createdAt") or "",
        "pr_number": int(pr["number"]),
        "pr_title": pr.get("title") or "",
        "pr_body": pr.get("body") or "",
        "diff": diff,
        "pr_merged_at": pr.get("mergedAt") or "",
        "review_count": int((pr.get("reviews") or {}).get("totalCount") or 0),
        "changed_files": changed_files,
        "additions": int(pr.get("additions") or 0),
        "deletions": int(pr.get("deletions") or 0),
        "base_sha": pr.get("baseRefOid") or "",
        "merge_sha": (pr.get("mergeCommit") or {}).get("oid") or "",
        "base_branch": pr.get("baseRefName") or "",
        "closing_pr_confidence": confidence,
        "has_tests": len(test_files_changed) > 0,
        "test_files_changed": test_files_changed,
        "license": license_spdx_id,
        "collected_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    }


# ── terminal display ──────────────────────────────────────────────────────────

_D_W = 60                 # inner box width
_D_C = (16, 14, 14, 13)  # column widths; sum + 3 separators == _D_W
_D_LINES = 9              # total lines in the rendered block
_RST = "\033[0m"


def _ansi(code: str, s: str) -> str:
    return f"\033[{code}m{s}{_RST}"


def _state_color(state: str) -> str:
    if state.startswith("sleeping"):
        return "33"   # yellow
    if state == "writing":
        return "34"   # blue
    if state == "calling":
        return "32"   # green
    return "36"       # cyan


def _render_status(cfg: RepoConfig, stats: dict) -> list[str]:
    c1, c2, c3, c4 = _D_C
    W = _D_W

    top = _ansi("2", f"┌{'─' * W}┐")

    repo_vis = f"  patch  ›  {cfg.full_name}"
    repo_line = (
        _ansi("2", "│")
        + "  "
        + _ansi("1;96", "patch")
        + "  ›  "
        + _ansi("1;97", cfg.full_name)
        + " " * max(0, W - len(repo_vis))
        + _ansi("2", "│")
    )

    meta_vis = f"  {cfg.language}  ·  {cfg.domain}"
    meta_line = (
        _ansi("2", "│")
        + _ansi("2;37", f"  {cfg.language}  ·  {cfg.domain}")
        + " " * max(0, W - len(meta_vis))
        + _ansi("2", "│")
    )

    col_border = _ansi("2", f"├{'─'*c1}┬{'─'*c2}┬{'─'*c3}┬{'─'*c4}┤")

    labels = ("API CALLS", "WRITTEN", "SKIPPED", "FAILED")
    lbl_cells = [f"  {lbl:<{w - 2}}" for lbl, w in zip(labels, _D_C)]
    lbl_row = (
        "".join(_ansi("2", "│") + _ansi("2;37", cell) for cell in lbl_cells)
        + _ansi("2", "│")
    )

    vals = (
        f"{stats['api_calls']:,}",
        f"{stats['written']:,}",
        f"{stats['skipped']:,}",
        f"{stats['failed']:,}",
    )
    val_cells = [f"  {v:<{w - 2}}" for v, w in zip(vals, _D_C)]
    val_row = (
        "".join(_ansi("2", "│") + _ansi("1;97", cell) for cell in val_cells)
        + _ansi("2", "│")
    )

    state_border = _ansi("2", f"├{'─'*c1}┴{'─'*c2}┴{'─'*c3}┴{'─'*c4}┤")

    state = stats["state"]
    sc = _state_color(state)
    state_text = state[: W - 4]  # "  ● " = 4 chars
    state_line = (
        _ansi("2", "│")
        + "  "
        + _ansi(sc, "●")
        + " "
        + _ansi(sc, state_text)
        + " " * max(0, W - 4 - len(state_text))
        + _ansi("2", "│")
    )

    bottom = _ansi("2", f"└{'─' * W}┘")

    return [top, repo_line, meta_line, col_border, lbl_row, val_row, state_border, state_line, bottom]


async def _status_loop(cfg: RepoConfig, stats: dict, stop: asyncio.Event) -> None:
    first = True
    while not stop.is_set():
        lines = _render_status(cfg, stats)
        if not first:
            sys.stdout.write(f"\033[{_D_LINES}A")
        for line in lines:
            sys.stdout.write(f"\r{line}\n")
        sys.stdout.flush()
        first = False
        await asyncio.sleep(0.5)
    print(flush=True)


async def collect_repo(
    session: aiohttp.ClientSession,
    cfg: RepoConfig,
    manifest: Manifest,
    output_dir: Path,
    token: str,
) -> dict[str, int]:
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"{cfg.owner}-{cfg.repo}-{cfg.language}.jsonl"

    write_lock = asyncio.Lock()
    stats: dict = {
        "written": 0, "skipped": 0, "failed": 0, "api_calls": 0, "state": "starting"
    }

    stop_display = asyncio.Event()
    display_task = asyncio.create_task(_status_loop(cfg, stats, stop_display))

    repo_state = manifest.get_repo_state(cfg)
    since_issue = manifest.get_last_issue(cfg) if repo_state.get("complete", False) else 0
    seen_issues = await _load_existing_issue_numbers(out_path)

    license_spdx_id = "UNKNOWN"
    cursor: str | None = None
    done = False

    while not done:
        stats["state"] = "calling"
        variables: dict[str, Any] = {
            "owner": cfg.owner,
            "repo": cfg.repo,
            "first": 25,
            "after": cursor,
        }
        data = await _graphql(session, token, COLLECT_QUERY, variables, stats)

        repo_data = data["repository"]
        license_info = repo_data.get("licenseInfo") or {}
        license_spdx_id = license_info.get("spdxId") or "UNKNOWN"

        issues_page = repo_data["issues"]
        page_info = issues_page["pageInfo"]
        nodes = issues_page["nodes"]

        if not nodes:
            break

        page_min: int | None = None

        for issue in nodes:
            issue_number = int(issue["number"])

            if issue_number in seen_issues:
                continue

            if issue_number <= since_issue:
                done = True
                break

            pr_match = _extract_pr_from_issue(issue)
            if not pr_match:
                stats["skipped"] += 1
                continue

            pr, confidence = pr_match
            pr_number = int(pr["number"])

            stats["state"] = f"fetching diff #{pr_number}"
            try:
                diff = await get_text(
                    session,
                    f"{GITHUB_API}/repos/{cfg.owner}/{cfg.repo}/pulls/{pr_number}",
                    token=token,
                    accept="application/vnd.github.v3.diff",
                    stats=stats,
                )
            except Exception as exc:
                print(
                    f"\n[{cfg.full_name}] issue {issue_number} diff failed: {exc}",
                    flush=True,
                )
                stats["failed"] += 1
                continue

            record = _build_record(cfg, issue, pr, confidence, diff, license_spdx_id)

            stats["state"] = "writing"
            await _append_jsonl(out_path, record, write_lock)
            seen_issues.add(issue_number)
            stats["written"] += 1
            page_min = issue_number if page_min is None else min(page_min, issue_number)

        if page_min is not None:
            manifest.update(
                cfg,
                last_issue_number=page_min,
                raw_count=len(seen_issues),
                complete=False,
            )

        if not page_info.get("hasNextPage") or done:
            break

        cursor = page_info.get("endCursor")

    stop_display.set()
    await display_task

    max_seen = max(seen_issues) if seen_issues else 0
    manifest.update(cfg, raw_count=len(seen_issues), complete=True, last_issue_number=max_seen)

    c = _ansi("1;32", f"✓ {cfg.full_name}")
    print(
        f"{c}  "
        f"{_ansi('1;97', str(stats['written']))} written  "
        f"{_ansi('2;37', str(stats['skipped']))} skipped  "
        f"{_ansi('2;37', str(stats['failed']))} failed  "
        f"{_ansi('2;37', str(stats['api_calls']))} calls"
    )
    return stats
