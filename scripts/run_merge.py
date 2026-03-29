from __future__ import annotations

import os
from pathlib import Path
import sys

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from patch.merge import merge_and_split


def main() -> None:
    load_dotenv()
    data_dir = Path(os.getenv("DATA_DIR", "./data"))
    summary = merge_and_split(data_dir / "processed", data_dir / "hf_upload")

    print(f"total={summary['total']} train={summary['train']} test={summary['test']}")
    print(f"languages={summary['languages']}")
    print(f"domains={summary['domains']}")


if __name__ == "__main__":
    main()
