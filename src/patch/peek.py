from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime
from typing import Any

import aiohttp
import orjson
from tenacity import retry, stop_after_attempt, wait_exponential

GITHUB_GRAPHQL = "https://api.github.com/graphql"
OWNER = "pola-rs"
REPO = "polars"

ISSUE_WITH_CLOSER_QUERY = """
query PeekIssues($owner: String!, $repo: String!, $count: Int!) {
  repository(owner: $owner, name: $repo) {
    issues(first: $count, states: CLOSED, orderBy: {field: UPDATED_AT, direction: DESC}) {
      nodes {
        number
        title
        body
        createdAt
        labels(first: 20) {
          nodes { name }
        }
        timelineItems(last: 100, itemTypes: [CLOSED_EVENT, CROSS_REFERENCED_EVENT]) {
          totalCount
          nodes {
            __typename
            ... on ClosedEvent {
              closer {
                __typename
                ... on PullRequest {
                  number
                  title
                  mergedAt
                }
                ... on Commit {
                  associatedPullRequests(first: 5) {
                    nodes {
                      number
                      title
                      mergedAt
                    }
                  }
                }
              }
            }
            ... on CrossReferencedEvent {
              willCloseTarget
              source {
                __typename
                ... on PullRequest {
                  number
                  title
                  mergedAt
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

PR_DETAILS_QUERY = """
query PeekPullRequest($owner: String!, $repo: String!, $number: Int!) {
  repository(owner: $owner, name: $repo) {
    licenseInfo {
      spdxId
    }
    pullRequest(number: $number) {
      number
      title
      body
      mergedAt
      additions
      deletions
      baseRefName
      baseRefOid
      mergeCommit {
        oid
        parents(first: 5) { nodes { oid } }
      }
      reviews {
        totalCount
      }
      files(first: 100) {
        totalCount
        nodes {
          path
        }
      }
    }
  }
}
"""


def _extract_merged_pr(issue: dict[str, Any]) -> tuple[dict[str, Any], str] | None:
    timeline_nodes = issue.get("timelineItems", {}).get("nodes", [])
    candidates: list[tuple[int, int, dict[str, Any], str]] = []

    for node in timeline_nodes:
        typename = node.get("__typename")
        if typename == "ClosedEvent":
            closer = node.get("closer")
            if not closer:
                continue
            closer_type = closer.get("__typename")
            if closer_type == "PullRequest" and closer.get("mergedAt"):
                candidates.append((0, int(closer.get("number") or 0), closer, "direct_close"))
            elif closer_type == "Commit":
                linked_prs = (
                    (closer.get("associatedPullRequests") or {}).get("nodes") or []
                )
                for pr in linked_prs:
                    if pr.get("mergedAt"):
                        candidates.append(
                            (
                                1,
                                int(pr.get("number") or 0),
                                pr,
                                "closed_by_commit_associated_pr",
                            )
                        )
        if typename == "CrossReferencedEvent":
            will_close_target = node.get("willCloseTarget")
            if will_close_target is False:
                continue
            source = node.get("source")
            if (
                source
                and source.get("__typename") == "PullRequest"
                and source.get("mergedAt")
            ):
                confidence = (
                    "cross_reference_closing"
                    if will_close_target is True
                    else "cross_reference"
                )
                candidates.append((2, int(source.get("number") or 0), source, confidence))

    if not candidates:
        return None

    candidates.sort(key=lambda item: (item[0], -item[1]))
    _, _, pr, confidence = candidates[0]
    return pr, confidence


async def _respect_rate_limit(response: aiohttp.ClientResponse) -> None:
    remaining = int(response.headers.get("X-RateLimit-Remaining", "1"))
    if remaining >= 10:
        return
    reset_at = int(response.headers.get("X-RateLimit-Reset", "0"))
    sleep_for = max(0, reset_at - int(time.time())) + 2
    await asyncio.sleep(sleep_for)


@retry(wait=wait_exponential(min=1, max=60), stop=stop_after_attempt(5), reraise=True)
async def _graphql(
    session: aiohttp.ClientSession, token: str, query: str, variables: dict[str, Any]
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
        await _respect_rate_limit(response)
        text = await response.text()
        if response.status in {403, 429, 500, 502, 503, 504}:
            raise RuntimeError(
                f"GraphQL request failed ({response.status}): {text[:300]}"
            )
        if response.status >= 400:
            raise RuntimeError(
                f"GraphQL request failed ({response.status}): {text[:300]}"
            )
        data = orjson.loads(text)
        if "errors" in data:
            raise RuntimeError(f"GraphQL errors: {data['errors']}")
        return data["data"]


@retry(wait=wait_exponential(min=1, max=60), stop=stop_after_attempt(5), reraise=True)
async def _fetch_diff(
    session: aiohttp.ClientSession, token: str, owner: str, repo: str, pr_number: int
) -> str:
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github.v3.diff",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    url = f"https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}"
    async with session.get(url, headers=headers) as response:
        await _respect_rate_limit(response)
        text = await response.text()
        if response.status in {403, 429, 500, 502, 503, 504}:
            raise RuntimeError(
                f"Diff request failed ({response.status}): {text[:300]}"
            )
        if response.status >= 400:
            raise RuntimeError(
                f"Diff request failed ({response.status}): {text[:300]}"
            )
        return text


async def collect_peek(token: str) -> dict[str, Any]:
    issue_vars = {"owner": OWNER, "repo": REPO, "count": 25}

    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=60)
    ) as session:
        issue_data = await _graphql(session, token, ISSUE_WITH_CLOSER_QUERY, issue_vars)
        issues = issue_data["repository"]["issues"]["nodes"]

        selected_issue = None
        selected_pr = None
        closing_pr_confidence = "unknown"
        for issue in issues:
            match = _extract_merged_pr(issue)
            if match:
                pr, confidence = match
                selected_issue = issue
                selected_pr = pr
                closing_pr_confidence = confidence
                break

        if not selected_issue or not selected_pr:
            raise RuntimeError(
                "No closed issue linked to a merged PR found in peek sample."
            )

        pr_number = int(selected_pr["number"])
        pr_vars = {"owner": OWNER, "repo": REPO, "number": pr_number}
        pr_data, diff = await asyncio.gather(
            _graphql(session, token, PR_DETAILS_QUERY, pr_vars),
            _fetch_diff(session, token, OWNER, REPO, pr_number),
        )
        pr_details = pr_data["repository"]["pullRequest"]

        labels = [
            node["name"]
            for node in selected_issue.get("labels", {}).get("nodes", [])
            if node.get("name")
        ]
        changed_files = [
            node["path"]
            for node in pr_details.get("files", {}).get("nodes", [])
            if node.get("path")
        ]
        test_files_changed = [path for path in changed_files if "test" in path.lower()]
        license_spdx_id = (
            pr_data["repository"].get("licenseInfo", {}) or {}
        ).get("spdxId") or "UNKNOWN"

        return {
            "request_example": {
                "endpoint": GITHUB_GRAPHQL,
                "query": ISSUE_WITH_CLOSER_QUERY.strip(),
                "variables": issue_vars,
            },
            "sample_record": {
                "repo": f"{OWNER}/{REPO}",
                "issue_number": selected_issue["number"],
                "issue_title": selected_issue.get("title", ""),
                "issue_body": selected_issue.get("body") or "",
                "issue_labels": labels,
                "issue_created_at": selected_issue.get("createdAt", ""),
                "pr_number": pr_details["number"],
                "pr_title": pr_details.get("title", ""),
                "pr_body": pr_details.get("body") or "",
                "pr_merged_at": pr_details.get("mergedAt", ""),
                "review_count": pr_details.get("reviews", {}).get("totalCount", 0),
                "additions": pr_details.get("additions", 0),
                "deletions": pr_details.get("deletions", 0),
                "base_sha": pr_details.get("baseRefOid") or "",
                "merge_sha": (pr_details.get("mergeCommit") or {}).get("oid") or "",
                "merge_parent_shas": [
                    node["oid"]
                    for node in (pr_details.get("mergeCommit") or {})
                    .get("parents", {})
                    .get("nodes", [])
                    if node.get("oid")
                ],
                "base_branch": pr_details.get("baseRefName") or "",
                "closing_pr_confidence": closing_pr_confidence,
                "diff": diff,
                "has_tests": len(test_files_changed) > 0,
                "test_files_changed": test_files_changed,
                "changed_files": changed_files,
                "license": license_spdx_id,
                "collected_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            },
        }
