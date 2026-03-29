from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

from config.repos import REPO_INDEX, REPOS
from patch.manifest import Manifest
from patch.process import process_repo


def _targets(repo_arg: str | None):
    if not repo_arg:
        return REPOS
    if repo_arg not in REPO_INDEX:
        known = ", ".join(sorted(REPO_INDEX))
        raise SystemExit(f"Unknown repo '{repo_arg}'. Known repos: {known}")
    return [REPO_INDEX[repo_arg]]


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Process raw records into SFT messages[]"
    )
    parser.add_argument("--repo", help="Single repo in owner/repo form", default=None)
    args = parser.parse_args()

    data_dir = Path(os.getenv("DATA_DIR", "./data"))
    raw_dir = data_dir / "raw"
    processed_dir = data_dir / "processed"
    manifest = Manifest(data_dir / "manifest.json")

    for cfg in _targets(args.repo):
        total, kept = process_repo(cfg, raw_dir, processed_dir)
        out_path = processed_dir / f"{cfg.owner}-{cfg.repo}.jsonl"
        existing = 0
        if out_path.exists():
            with open(out_path, "rb") as handle:
                for _ in handle:
                    existing += 1
        manifest.update(cfg, processed_count=existing)
        print(
            f"[{cfg.full_name}] processed: scanned={total}, new_kept={kept}, total_kept={existing}"
        )


if __name__ == "__main__":
    main()
