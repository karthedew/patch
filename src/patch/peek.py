from __future__ import annotations

import json
from typing import Any

import aiohttp

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
        timelineItems(first: 50, itemTypes: [CLOSED_EVENT, CROSS_REFERENCED_EVENT]) {
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
              }
            }
            ... on CrossReferencedEvent {
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
    pullRequest(number: $number) {
      number
      title
      body
      mergedAt
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


def _extract_merged_pr(issue: dict[str, Any]) -> dict[str, Any] | None:
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
                return closer
        if typename == "CrossReferencedEvent":
            source = node.get("source")
            if (
                source
                and source.get("__typename") == "PullRequest"
                and source.get("mergedAt")
            ):
                return source
    return None


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
        GITHUB_GRAPHQL, headers=headers, data=json.dumps(payload)
    ) as response:
        text = await response.text()
        if response.status >= 400:
            raise RuntimeError(
                f"GraphQL request failed ({response.status}): {text[:300]}"
            )
        data = json.loads(text)
        if "errors" in data:
            raise RuntimeError(f"GraphQL errors: {data['errors']}")
        return data["data"]


async def collect_peek(token: str) -> dict[str, Any]:
    issue_vars = {"owner": OWNER, "repo": REPO, "count": 25}

    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=60)
    ) as session:
        issue_data = await _graphql(session, token, ISSUE_WITH_CLOSER_QUERY, issue_vars)
        issues = issue_data["repository"]["issues"]["nodes"]

        selected_issue = None
        selected_pr = None
        for issue in issues:
            pr = _extract_merged_pr(issue)
            if pr:
                selected_issue = issue
                selected_pr = pr
                break

        if not selected_issue or not selected_pr:
            raise RuntimeError(
                "No closed issue linked to a merged PR found in peek sample."
            )

        pr_number = int(selected_pr["number"])
        pr_vars = {"owner": OWNER, "repo": REPO, "number": pr_number}
        pr_data = await _graphql(session, token, PR_DETAILS_QUERY, pr_vars)
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
                "issue_body_preview": (selected_issue.get("body") or "")[:400],
                "issue_labels": labels,
                "issue_created": selected_issue.get("createdAt", ""),
                "pr_number": pr_details["number"],
                "pr_title": pr_details.get("title", ""),
                "pr_body_preview": (pr_details.get("body") or "")[:400],
                "merged_at": pr_details.get("mergedAt", ""),
                "review_count": pr_details.get("reviews", {}).get("totalCount", 0),
                "changed_files_count": pr_details.get("files", {}).get("totalCount", 0),
                "changed_files_sample": changed_files[:20],
                "note": "Peek mode does not fetch or store diff text.",
            },
        }
