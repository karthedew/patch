from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path
import sys

import aiohttp
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

from config.repos import REPO_INDEX, REPOS
from patch.collect import collect_repo
from patch.manifest import Manifest


def _targets(repo_arg: str | None):
    if not repo_arg:
        return REPOS
    if repo_arg not in REPO_INDEX:
        known = ", ".join(sorted(REPO_INDEX))
        raise SystemExit(f"Unknown repo '{repo_arg}'. Known repos: {known}")
    return [REPO_INDEX[repo_arg]]


async def _run(repo_arg: str | None) -> None:
    token = os.getenv("GITHUB_TOKEN")
    if not token:
        raise SystemExit("GITHUB_TOKEN is required")

    data_dir = Path(os.getenv("DATA_DIR", "./data"))
    raw_dir = data_dir / "raw"
    manifest = Manifest(data_dir / "manifest.json")

    targets = _targets(repo_arg)
    print("Manifest: ", manifest)
    print("Targets: ", targets)
    timeout = aiohttp.ClientTimeout(total=120)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for cfg in targets:
            await collect_repo(session, cfg, manifest, raw_dir, token)


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Collect GitHub issue/PR diff records")
    parser.add_argument("--repo", help="Single repo in owner/repo form", default=None)
    args = parser.parse_args()
    asyncio.run(_run(args.repo))


if __name__ == "__main__":
    main()
