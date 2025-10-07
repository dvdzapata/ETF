"""Logging utilities for the ETF pipeline."""
from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path


def setup_logging(log_dir: Path, *, console_level: int = logging.INFO) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "pipeline.log"
    error_log_file = log_dir / "errors.log"
    rejection_log_file = log_dir / "rejections.log"

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    # Console handler for real-time progress.
    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # General rotating file handler.
    file_handler = RotatingFileHandler(log_file, maxBytes=5_000_000, backupCount=5)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    # Dedicated error log file handler.
    error_handler = RotatingFileHandler(error_log_file, maxBytes=5_000_000, backupCount=5)
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    root_logger.addHandler(error_handler)

    # Dedicated data rejection log.
    rejection_handler = RotatingFileHandler(rejection_log_file, maxBytes=5_000_000, backupCount=5)
    rejection_handler.setLevel(logging.WARNING)
    rejection_handler.setFormatter(formatter)
    logging.getLogger("rejections").addHandler(rejection_handler)
    logging.getLogger("rejections").setLevel(logging.WARNING)


__all__ = ["setup_logging"]
