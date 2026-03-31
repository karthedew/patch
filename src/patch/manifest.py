from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
import json
from pathlib import Path
import threading
from typing import Any

import fcntl

from patch.repos import RepoConfig


class Manifest:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock_path = self.path.with_suffix(self.path.suffix + ".lock")
        self._thread_lock = threading.RLock()
        self._data: dict[str, dict[str, Any]] = {}
        self.reload()

    @contextmanager
    def _file_lock(self):
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.lock_path, "a", encoding="utf-8") as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

    def reload(self) -> None:
        with self._thread_lock:
            with self._file_lock():
                if not self.path.exists():
                    self._data = {}
                    return
                with open(self.path, "r", encoding="utf-8") as handle:
                    self._data = json.load(handle)

    def flush(self) -> None:
        with self._thread_lock:
            with self._file_lock():
                tmp = self.path.with_suffix(self.path.suffix + ".tmp")
                with open(tmp, "w", encoding="utf-8") as handle:
                    json.dump(self._data, handle, indent=2, sort_keys=True)
                tmp.replace(self.path)

    @staticmethod
    def _repo_key(cfg: RepoConfig) -> str:
        return f"{cfg.owner}/{cfg.repo}"

    def get_repo_state(self, cfg: RepoConfig) -> dict[str, Any]:
        key = self._repo_key(cfg)
        with self._thread_lock:
            return dict(self._data.get(key, {}))

    def get_last_issue(self, cfg: RepoConfig) -> int:
        with self._thread_lock:
            state = self._data.get(self._repo_key(cfg), {})
            value = state.get("last_issue_number", 0)
            return int(value or 0)

    def update(
        self,
        cfg: RepoConfig,
        *,
        last_issue_number: int | None = None,
        raw_count: int | None = None,
        processed_count: int | None = None,
        complete: bool | None = None,
    ) -> None:
        with self._thread_lock:
            key = self._repo_key(cfg)
            state = self._data.setdefault(
                key,
                {
                    "last_issue_number": 0,
                    "last_run": "",
                    "raw_count": 0,
                    "processed_count": 0,
                    "complete": False,
                },
            )

            if last_issue_number is not None:
                state["last_issue_number"] = int(
                    max(last_issue_number, state["last_issue_number"])
                )
            if raw_count is not None:
                state["raw_count"] = int(raw_count)
            if processed_count is not None:
                state["processed_count"] = int(processed_count)
            if complete is not None:
                state["complete"] = bool(complete)

            state["last_run"] = datetime.now(UTC).isoformat().replace("+00:00", "Z")
            self.flush()
