"""Utilities for centralised logging configuration."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

DEFAULT_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"


def _level_from_name(level_name: str | int | None) -> int:
    if isinstance(level_name, int):
        return level_name
    if not level_name:
        return logging.INFO
    level_str = str(level_name).upper()
    return getattr(logging, level_str, logging.INFO)


def configure_logging(level: str | int = "INFO", log_file: Optional[str] = None) -> None:
    """Configure root logging handlers for CLI or web usage."""

    numeric_level = _level_from_name(level)
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)
    for handler in root_logger.handlers:
        handler.setLevel(numeric_level)

    if not root_logger.handlers:
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(numeric_level)
        stream_handler.setFormatter(logging.Formatter(DEFAULT_FORMAT))
        root_logger.addHandler(stream_handler)

    if log_file:
        path = Path(log_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        existing = [
            handler
            for handler in root_logger.handlers
            if isinstance(handler, logging.FileHandler)
            and getattr(handler, "baseFilename", None) == str(path)
        ]
        if not existing:
            file_handler = logging.FileHandler(path)
            file_handler.setLevel(numeric_level)
            file_handler.setFormatter(logging.Formatter(DEFAULT_FORMAT))
            root_logger.addHandler(file_handler)
