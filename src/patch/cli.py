from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path

import aiohttp
from dotenv import load_dotenv


def _data_dir() -> Path:
    return Path(os.getenv("DATA_DIR", "./data"))


def _targets(repo_arg: str | None):
    from patch.repos import REPO_INDEX, REPOS
    if not repo_arg:
        return REPOS
    if repo_arg not in REPO_INDEX:
        known = ", ".join(sorted(REPO_INDEX))
        raise SystemExit(f"Unknown repo '{repo_arg}'. Known repos:\n  {known}")
    return [REPO_INDEX[repo_arg]]


# ── subcommand handlers ────────────────────────────────────────────────────────

async def _collect(args: argparse.Namespace) -> None:
    from patch.collect import collect_repo
    from patch.manifest import Manifest

    token = os.getenv("GITHUB_TOKEN")
    if not token:
        raise SystemExit("GITHUB_TOKEN is required")

    data_dir = _data_dir()
    raw_dir = data_dir / "raw"
    manifest = Manifest(data_dir / "manifest.json")
    targets = _targets(args.repo)

    print("Manifest:", manifest.path)
    print("Targets:", [cfg.full_name for cfg in targets])

    timeout = aiohttp.ClientTimeout(total=120)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for cfg in targets:
            await collect_repo(session, cfg, manifest, raw_dir, token)


def _process(args: argparse.Namespace) -> None:
    from patch.manifest import Manifest
    from patch.process import process_repo

    data_dir = _data_dir()
    raw_dir = data_dir / "raw"
    processed_dir = data_dir / "processed"
    manifest = Manifest(data_dir / "manifest.json")

    for cfg in _targets(args.repo):
        total, kept = process_repo(cfg, raw_dir, processed_dir)
        out_path = processed_dir / f"{cfg.owner}-{cfg.repo}-{cfg.language}.jsonl"
        existing = sum(1 for _ in open(out_path, "rb")) if out_path.exists() else 0
        manifest.update(cfg, processed_count=existing)
        print(f"[{cfg.full_name}] scanned={total} new_kept={kept} total_kept={existing}")


def _merge(_args: argparse.Namespace) -> None:
    from patch.merge import merge_and_split

    data_dir = _data_dir()
    summary = merge_and_split(data_dir / "processed", data_dir / "hf_upload")
    print(f"total={summary['total']} train={summary['train']} test={summary['test']}")
    print(f"languages={summary['languages']}")
    print(f"domains={summary['domains']}")


def _push(_args: argparse.Namespace) -> None:
    from huggingface_hub import get_token
    from patch.push import push_to_hub

    token = os.getenv("HF_TOKEN") or get_token()
    repo_id = os.getenv("HF_REPO_ID")
    if not token:
        raise SystemExit(
            "HF auth required. Set HF_TOKEN in .env or run `huggingface-cli login`."
        )
    if not repo_id:
        raise SystemExit("HF_REPO_ID is required")

    data_dir = _data_dir()
    push_to_hub(data_dir / "hf_upload", repo_id, token)
    print(f"Pushed dataset to {repo_id}")


async def _peek(_args: argparse.Namespace) -> None:
    from patch.peek import collect_peek

    token = os.getenv("GITHUB_TOKEN")
    if not token:
        raise SystemExit("GITHUB_TOKEN is required")

    result = await collect_peek(token)
    print(json.dumps(result, indent=2))


# ── main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    load_dotenv()

    parser = argparse.ArgumentParser(
        prog="patch-sft",
        description="patch-sft: GitHub issue/PR diff dataset pipeline",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_collect = sub.add_parser("collect", help="Collect raw issue/PR diffs from GitHub")
    p_collect.add_argument("--repo", help="Single repo in owner/repo form", default=None)

    p_process = sub.add_parser("process", help="Filter and format raw records into SFT messages")
    p_process.add_argument("--repo", help="Single repo in owner/repo form", default=None)

    sub.add_parser("merge", help="Merge processed records and write train/test split")
    sub.add_parser("push", help="Push dataset to HuggingFace Hub")
    sub.add_parser("peek", help="Preview a sample record from pola-rs/polars")

    args = parser.parse_args()

    if args.command == "collect":
        asyncio.run(_collect(args))
    elif args.command == "process":
        _process(args)
    elif args.command == "merge":
        _merge(args)
    elif args.command == "push":
        _push(args)
    elif args.command == "peek":
        asyncio.run(_peek(args))


if __name__ == "__main__":
    main()
