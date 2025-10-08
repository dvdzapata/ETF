"""Configuration management for the ETF data pipeline."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict


DEFAULT_STATE_PATH = Path("state.json")
DEFAULT_LOG_DIR = Path("logs")


@dataclass(frozen=True)
class APISettings:
    base_url: str
    api_key: str
    max_requests_per_minute: int = 3000


@dataclass(frozen=True)
class DatabaseSettings:
    host: str
    port: int
    user: str
    password: str
    dbname: str
    sslmode: str | None = None


@dataclass(frozen=True)
class PipelineSettings:
    api: APISettings
    database: DatabaseSettings
    state_file: Path = DEFAULT_STATE_PATH
    log_dir: Path = DEFAULT_LOG_DIR
    max_workers: int = 8
    request_timeout: int = 30
    request_retries: int = 3
    state_flush_interval_seconds: int = 30
    state_flush_threshold: int = 100
    progress_log_interval: int = 25


def _parse_env_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def load_env_file(path: Path) -> Dict[str, str]:
    data: Dict[str, str] = {}
    if not path.exists():
        return data
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        data[key.strip()] = value.strip().strip('"').strip("'")
    return data


def build_settings(env_path: Path | None = None) -> PipelineSettings:
    env_path = env_path or Path(".env")
    env_values = dict(load_env_file(env_path))
    env_values.update({k: v for k, v in os.environ.items() if k not in env_values})

    api_key = env_values.get("FMP_API_KEY")
    if not api_key:
        raise ValueError("FMP_API_KEY is required in environment or .env file")

    base_url = env_values.get("FMP_BASE_URL", "https://financialmodelingprep.com/stable")

    db_settings = DatabaseSettings(
        host=env_values.get("DB_HOST", "localhost"),
        port=int(env_values.get("DB_PORT", "5432")),
        user=env_values.get("DB_USER", "postgres"),
        password=env_values.get("DB_PASSWORD", ""),
        dbname=env_values.get("DB_NAME", "etf_momentum"),
        sslmode=env_values.get("DB_SSLMODE") or None,
    )

    api_settings = APISettings(
        base_url=base_url.rstrip("/"),
        api_key=api_key,
        max_requests_per_minute=int(env_values.get("FMP_MAX_REQUESTS_PER_MINUTE", "3000")),
    )

    state_file = Path(env_values.get("PIPELINE_STATE_FILE", DEFAULT_STATE_PATH.as_posix()))
    log_dir = Path(env_values.get("PIPELINE_LOG_DIR", DEFAULT_LOG_DIR.as_posix()))
    max_workers = int(env_values.get("PIPELINE_MAX_WORKERS", "8"))
    request_timeout = int(env_values.get("PIPELINE_REQUEST_TIMEOUT", "30"))
    request_retries = int(env_values.get("PIPELINE_REQUEST_RETRIES", "3"))

    state_flush_interval = int(env_values.get("PIPELINE_STATE_FLUSH_INTERVAL", "30"))
    state_flush_threshold = int(env_values.get("PIPELINE_STATE_FLUSH_THRESHOLD", "100"))
    progress_log_interval = int(env_values.get("PIPELINE_PROGRESS_INTERVAL", "25"))

    return PipelineSettings(
        api=api_settings,
        database=db_settings,
        state_file=state_file,
        log_dir=log_dir,
        max_workers=max_workers,
        request_timeout=request_timeout,
        request_retries=request_retries,
        state_flush_interval_seconds=state_flush_interval,
        state_flush_threshold=state_flush_threshold,
        progress_log_interval=progress_log_interval,
    )


import os

__all__ = [
    "APISettings",
    "DatabaseSettings",
    "PipelineSettings",
    "build_settings",
    "load_env_file",
    "DEFAULT_STATE_PATH",
    "DEFAULT_LOG_DIR",
]
