"""HTTP client with rate limiting and retry support."""
from __future__ import annotations

import logging
import threading
import time
from typing import Any, Dict, Optional

import requests
from requests import Response, Session

from .config import APISettings

logger = logging.getLogger(__name__)


class RateLimiter:
    """Simple thread-safe rate limiter (sliding window)."""

    def __init__(self, max_calls: int, period: float) -> None:
        self.max_calls = max_calls
        self.period = period
        self.lock = threading.Lock()
        self.calls: list[float] = []

    def acquire(self) -> None:
        with self.lock:
            now = time.monotonic()
            # remove timestamps outside window
            self.calls = [t for t in self.calls if now - t < self.period]
            if len(self.calls) >= self.max_calls:
                sleep_time = self.period - (now - self.calls[0])
                if sleep_time > 0:
                    time.sleep(sleep_time)
                now = time.monotonic()
                self.calls = [t for t in self.calls if now - t < self.period]
            self.calls.append(time.monotonic())


class FMPClient:
    def __init__(self, settings: APISettings, timeout: int = 30, retries: int = 3) -> None:
        self.settings = settings
        self.timeout = timeout
        self.retries = retries
        self._thread_local = threading.local()
        self._sessions: set[Session] = set()
        self._sessions_lock = threading.Lock()
        # keep 90% of allowed throughput for safety
        max_calls = max(1, int(settings.max_requests_per_minute * 0.9))
        self.rate_limiter = RateLimiter(max_calls=max_calls, period=60.0)

    def _get_session(self) -> Session:
        session = getattr(self._thread_local, "session", None)
        if session is None:
            session = requests.Session()
            setattr(self._thread_local, "session", session)
            with self._sessions_lock:
                self._sessions.add(session)
        return session

    def _request(self, method: str, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Response:
        url = f"{self.settings.base_url}/{endpoint.lstrip('/') }"
        params = params or {}
        params.setdefault("apikey", self.settings.api_key)

        for attempt in range(1, self.retries + 1):
            try:
                self.rate_limiter.acquire()
                session = self._get_session()
                response = session.request(method, url, params=params, timeout=self.timeout)
                if response.status_code >= 500:
                    raise requests.HTTPError(f"Server error {response.status_code}")
                response.raise_for_status()
                return response
            except Exception as exc:  # pylint: disable=broad-except
                logger.warning("Request to %s failed on attempt %s/%s: %s", url, attempt, self.retries, exc)
                if attempt == self.retries:
                    logger.error("Exceeded retries for %s", url, exc_info=exc)
                    raise
                backoff = min(2 ** attempt, 30)
                time.sleep(backoff)
        raise RuntimeError("Unreachable")

    def get_json(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Any:
        response = self._request("GET", endpoint, params)
        try:
            return response.json()
        except ValueError as exc:
            logger.error("Invalid JSON response from %s", response.url, exc_info=exc)
            raise

    def close(self) -> None:
        with self._sessions_lock:
            sessions = list(self._sessions)
            self._sessions.clear()
        for session in sessions:
            session.close()

    def __enter__(self) -> "FMPClient":
        return self

    def __exit__(self, exc_type, exc, exc_tb) -> None:
        self.close()


__all__ = ["FMPClient"]
