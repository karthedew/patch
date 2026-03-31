from __future__ import annotations

from collections import Counter
from pathlib import Path
import random

import orjson
import pyarrow as pa
import pyarrow.parquet as pq


def _records_to_table(records: list[dict]) -> pa.Table:
    if not records:
        return pa.table({})

    # Flatten: top-level keys + metadata sub-keys become columns
    rows: dict[str, list] = {}
    for record in records:
        meta = record.get("metadata", {})
        flat = {**{k: v for k, v in record.items() if k != "metadata"}, **meta}
        for key, val in flat.items():
            rows.setdefault(key, []).append(val)

    return pa.Table.from_pydict(rows)


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

    for name, records in [("train", train_records), ("test", test_records)]:
        path = output_dir / f"{name}.parquet"
        if path.exists():
            path.unlink()
        pq.write_table(
            _records_to_table(records),
            path,
            compression="zstd",
        )

    language_counts: Counter = Counter()
    domain_counts: Counter = Counter()
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
