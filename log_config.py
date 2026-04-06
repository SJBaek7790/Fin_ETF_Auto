"""
Centralized logging configuration for Fin_ETF_Auto.

Usage (in each entry-point script):
    from log_config import setup_logging, get_logger, get_log_filepath
    setup_logging("screening")          # call once at startup
    logger = get_logger(__name__)       # one per module
    # ... at end of run ...
    send_telegram_document(get_log_filepath(), caption="...")
"""

import os
import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime

_LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
_current_log_path: str | None = None


def setup_logging(script_name: str, console_level=logging.INFO, file_level=logging.DEBUG):
    """Configure root logger with daily-rotating file handler + console handler.

    Args:
        script_name: Prefix for log filename, e.g. 'screening' → logs/screening_2026-03-16.log
        console_level: Minimum level printed to stdout (default INFO).
        file_level: Minimum level written to file (default DEBUG).
    """
    global _current_log_path
    os.makedirs(_LOG_DIR, exist_ok=True)

    today_str = datetime.now().strftime("%Y-%m-%d")
    log_filename = f"{script_name}_{today_str}.txt"
    _current_log_path = os.path.join(_LOG_DIR, log_filename)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # --- File handler (daily rotation, 30-day retention) ---
    file_handler = TimedRotatingFileHandler(
        filename=_current_log_path,
        when="midnight",
        interval=1,
        backupCount=30,
        encoding="utf-8",
    )
    file_handler.setLevel(file_level)
    file_handler.setFormatter(formatter)

    # --- Console handler ---
    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_level)
    console_handler.setFormatter(formatter)

    # --- Root logger ---
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    # Prevent duplicate handlers on repeated calls
    root.handlers.clear()
    root.addHandler(file_handler)
    root.addHandler(console_handler)


def get_logger(name: str) -> logging.Logger:
    """Return a named logger (thin wrapper for consistency)."""
    return logging.getLogger(name)


def get_log_filepath() -> str | None:
    """Return the absolute path to today's log file (set by setup_logging)."""
    return _current_log_path
