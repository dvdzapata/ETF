"""Persistent state tracking for incremental downloads."""
from __future__ import annotations

import json
import logging
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class DownloadState:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = Lock()
        self._data: Dict[str, Any] = {}
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

    def save(self) -> None:
        with self._lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            temp_path = self.path.with_suffix(".tmp")
            temp_path.write_text(json.dumps(self._data, indent=2, sort_keys=True))
            temp_path.replace(self.path)

    def get(self, key: str, default: Any = None) -> Any:
        with self._lock:
            return self._data.get(key, default)

    def update(self, key: str, value: Any) -> None:
        with self._lock:
            self._data[key] = value
            self.save()

    def get_symbol_state(self, symbol: str) -> Dict[str, Any]:
        with self._lock:
            return dict(self._data.setdefault(symbol, {}))

    def update_symbol_state(self, symbol: str, payload: Dict[str, Any]) -> None:
        with self._lock:
            state = self._data.setdefault(symbol, {})
            state.update(payload)
            self.save()


__all__ = ["DownloadState"]
