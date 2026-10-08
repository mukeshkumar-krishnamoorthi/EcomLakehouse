"""
Logging utilities for EcomLakehouse.

Airflow manages logging handlers, formatting, and log destinations.
This module provides convenient access to Python loggers.
"""

from __future__ import annotations

import logging
from typing import Any


def get_logger(name: str | None = None) -> logging.Logger:
    """
    Return a logger for the given module.

    Args:
        name: Logger name. Usually ``__name__``.

    Returns:
        Python logger instance.
    """
    return logging.getLogger(name or "ecomlakehouse")


def get_task_logger(
    dag_id: str,
    task_id: str,
    run_id: str | None = None,
) -> logging.LoggerAdapter[logging.Logger]:
    """
    Return a logger with Airflow task context.
    """
    logger = get_logger(__name__)

    context: dict[str, Any] = {
        "dag_id": dag_id,
        "task_id": task_id,
    }

    if run_id:
        context["run_id"] = run_id

    return logging.LoggerAdapter(logger, context)


def log_exception(
    logger: logging.Logger,
    message: str,
    *args: Any,
) -> None:
    """
    Log an exception with its full traceback.

    Should be called inside an ``except`` block.
    """
    logger.exception(message, *args)
