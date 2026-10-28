# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

"""Logging with per-database context and secret masking on every handler."""

import json
import logging
import sys
from collections.abc import Iterable
from pathlib import Path

LOGGER_NAME = "sqdb"
MASK = "***"
FORMAT = "%(asctime)s %(levelname)-7s [%(db)s] %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


class SecretMasker:
    """Replaces every known secret value; longer secrets first, so overlaps mask fully."""

    def __init__(self, secrets: Iterable[str] = ()):
        self.secrets: list[str] = []
        self.update(secrets)

    def update(self, secrets: Iterable[str]) -> None:
        merged = set(self.secrets) | {secret for secret in secrets if secret}
        for secret in list(merged):
            merged.update(part for part in secret.splitlines() if part)
            merged.add(json.dumps(secret, ensure_ascii=False)[1:-1])
        self.secrets = sorted(merged, key=len, reverse=True)

    def __call__(self, text: str) -> str:
        for secret in self.secrets:
            if secret in text:
                text = text.replace(secret, MASK)
        return text


MASKER = SecretMasker()


class _MaskingFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        if not hasattr(record, "db"):
            record.db = "-"
        return MASKER(super().format(record))


def setup_logging(verbose: bool = False, log_file: Path | None = None) -> logging.Logger:
    logger = logging.getLogger(LOGGER_NAME)
    for handler in logger.handlers:
        handler.close()
    logger.handlers.clear()
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    formatter = _MaskingFormatter(FORMAT, DATE_FORMAT)
    console = logging.StreamHandler(sys.stderr)
    console.setLevel(logging.DEBUG if verbose else logging.INFO)
    console.setFormatter(formatter)
    logger.addHandler(console)
    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handler = logging.FileHandler(log_file, encoding="utf-8")
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger


def db_logger(name: str) -> logging.LoggerAdapter:
    return logging.LoggerAdapter(logging.getLogger(LOGGER_NAME), {"db": name})
