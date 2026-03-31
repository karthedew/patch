from __future__ import annotations

__all__ = ["__version__", "RepoConfig", "REPOS", "collect", "load"]

__version__ = "0.1.0"

from patch.repos import RepoConfig, REPOS


def collect(
    repos: list[RepoConfig] | None = None,
    *,
    token: str | None = None,
    data_dir: str = "./data",
) -> None:
    """Collect raw issue/PR diffs from GitHub into JSONL files.

    Requires a GitHub personal access token with ``repo`` scope (or set the
    ``GITHUB_TOKEN`` environment variable).

    Example::

        import patch
        from patch import RepoConfig

        patch.collect(
            repos=[RepoConfig("owner", "repo", "python", "web")],
            token="ghp_...",
        )
    """
    import asyncio
    import os
    from pathlib import Path

    import aiohttp

    from patch.collect import collect_repo
    from patch.manifest import Manifest

    resolved_token = token or os.getenv("GITHUB_TOKEN")
    if not resolved_token:
        raise ValueError(
            "A GitHub token is required. Pass token= or set the GITHUB_TOKEN env var."
        )

    targets = repos if repos is not None else REPOS
    root = Path(data_dir)
    manifest = Manifest(root / "manifest.json")

    async def _run() -> None:
        timeout = aiohttp.ClientTimeout(total=120)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            for cfg in targets:
                await collect_repo(session, cfg, manifest, root / "raw", resolved_token)

    asyncio.run(_run())


def load(repo_id: str, split: str | None = None):
    """Load the patch-sft dataset from HuggingFace Hub.

    Returns a :class:`datasets.DatasetDict` (or a :class:`datasets.Dataset`
    when *split* is specified). No GitHub token required.

    Example::

        import patch

        ds = patch.load("your-hf-username/patch-sft")
        df = ds["train"].to_pandas()
    """
    from datasets import load_dataset as _hf_load

    return _hf_load(repo_id, split=split)
