from __future__ import annotations

from collections import Counter
from pathlib import Path
import random

import orjson


def _append_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "ab") as handle:
        for record in records:
            handle.write(orjson.dumps(record) + b"\n")


def merge_and_split(
    processed_dir: Path,
    output_dir: Path,
    test_ratio: float = 0.05,
    seed: int = 42,
) -> dict:
    all_records: list[dict] = []

    for path in sorted(processed_dir.glob("*.jsonl")):
        with open(path, "rb") as handle:
            for line in handle:
                line = line.strip()
                if line:
                    all_records.append(orjson.loads(line))

    random.seed(seed)
    random.shuffle(all_records)

    split = int(len(all_records) * (1.0 - test_ratio))
    train_records = all_records[:split]
    test_records = all_records[split:]

    output_dir.mkdir(parents=True, exist_ok=True)
    train_path = output_dir / "train.jsonl"
    test_path = output_dir / "test.jsonl"
    if train_path.exists():
        train_path.unlink()
    if test_path.exists():
        test_path.unlink()

    _append_jsonl(train_path, train_records)
    _append_jsonl(test_path, test_records)

    language_counts = Counter()
    domain_counts = Counter()
    for record in all_records:
        meta = record.get("metadata", {})
        language_counts[meta.get("language", "unknown")] += 1
        domain_counts[meta.get("domain", "unknown")] += 1

    return {
        "total": len(all_records),
        "train": len(train_records),
        "test": len(test_records),
        "languages": dict(sorted(language_counts.items())),
        "domains": dict(sorted(domain_counts.items())),
    }
