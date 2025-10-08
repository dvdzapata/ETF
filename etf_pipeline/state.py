"""Persistent state tracking for incremental downloads."""
from __future__ import annotations

import json
import logging
from pathlib import Path
from threading import Lock
from typing import Any, Dict
import time

logger = logging.getLogger(__name__)


class DownloadState:
    def __init__(
        self,
        path: Path,
        *,
        flush_interval: float = 30.0,
        flush_threshold: int = 100,
    ) -> None:
        self.path = path
        self._lock = Lock()
        self._data: Dict[str, Any] = {}
        self._flush_interval = max(1.0, float(flush_interval))
        self._flush_threshold = max(1, int(flush_threshold))
        self._pending_writes = 0
        self._dirty = False
        self._last_flush = time.monotonic()
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            self._data = {}
            return
        try:
            self._data = json.loads(self.path.read_text())
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("Unable to load state file %s: %s", self.path, exc)
            self._data = {}

    def _write_locked(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self.path.with_suffix(".tmp")
        temp_path.write_text(json.dumps(self._data, indent=2, sort_keys=True))
        temp_path.replace(self.path)
        self._pending_writes = 0
        self._dirty = False
        self._last_flush = time.monotonic()

    def _should_flush_locked(self) -> bool:
        if not self._dirty:
            return False
        if self._pending_writes >= self._flush_threshold:
            return True
        return (time.monotonic() - self._last_flush) >= self._flush_interval

    def get(self, key: str, default: Any = None) -> Any:
        with self._lock:
            return self._data.get(key, default)

    def update(self, key: str, value: Any) -> None:
        with self._lock:
            self._data[key] = value
            self._pending_writes += 1
            self._dirty = True
            if self._should_flush_locked():
                self._write_locked()

    def get_symbol_state(self, symbol: str) -> Dict[str, Any]:
        with self._lock:
            return dict(self._data.setdefault(symbol, {}))

    def update_symbol_state(self, symbol: str, payload: Dict[str, Any]) -> None:
        with self._lock:
            state = self._data.setdefault(symbol, {})
            state.update(payload)
            self._pending_writes += 1
            self._dirty = True
            if self._should_flush_locked():
                self._write_locked()

    def flush(self) -> None:
        with self._lock:
            if self._dirty:
                self._write_locked()

    def save(self) -> None:  # backward compatibility helper
        self.flush()

    def close(self) -> None:
        self.flush()


__all__ = ["DownloadState"]
