"""
Centralized Enterprise Logging Infrastructure.

Provides formatted logging output to both standard console stream and file appender (logs/app.log).
"""

import os
import sys
import logging
from pathlib import Path

# Base Directory Setup
BASE_DIR = Path(__file__).resolve().parent.parent
LOGS_DIR = BASE_DIR / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE_PATH = LOGS_DIR / "app.log"


def get_logger(module_name: str = "SmartCityPlatform", log_level: int = logging.INFO) -> logging.Logger:
    """
    Creates and returns a configured logger instance.
    
    Args:
        module_name (str): Name of the calling module/file.
        log_level (int): Logging level (default: logging.INFO).
        
    Returns:
        logging.Logger: Fully configured Python logger instance.
    """
    logger = logging.getLogger(module_name)
    logger.setLevel(log_level)

    # Avoid duplicate handlers if logger is already configured
    if logger.hasHandlers():
        return logger

    # Log Formatter
    log_format = logging.Formatter(
        fmt="[%(asctime)s] [%(levelname)s] [%(name)s:%(lineno)d] - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # 1. Console Stream Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(log_format)
    logger.addHandler(console_handler)

    # 2. File Handler (Appends to logs/app.log)
    file_handler = logging.FileHandler(LOG_FILE_PATH, encoding="utf-8")
    file_handler.setLevel(log_level)
    file_handler.setFormatter(log_format)
    logger.addHandler(file_handler)

    return logger
