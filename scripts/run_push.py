from __future__ import annotations

import os
from pathlib import Path
import sys

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from patch.push import push_to_hub


def main() -> None:
    load_dotenv()
    token = os.getenv("HF_TOKEN")
    repo_id = os.getenv("HF_REPO_ID")
    if not token:
        raise SystemExit("HF_TOKEN is required")
    if not repo_id:
        raise SystemExit("HF_REPO_ID is required")

    data_dir = Path(os.getenv("DATA_DIR", "./data"))
    push_to_hub(data_dir / "hf_upload", repo_id, token)
    print(f"Pushed dataset to {repo_id}")


if __name__ == "__main__":
    main()
