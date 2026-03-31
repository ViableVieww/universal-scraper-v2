from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pipeline.config import PipelineConfig


class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if hasattr(record, "extra") and record.extra:
            entry["extra"] = record.extra
        if record.exc_info and record.exc_info[1]:
            entry["error"] = {
                "type": type(record.exc_info[1]).__name__,
                "message": str(record.exc_info[1]),
            }
        return json.dumps(entry)


def setup_logging(config: PipelineConfig) -> None:
    log_dir = Path(config.log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    json_fmt = JSONFormatter()
    console_fmt = logging.Formatter("%(asctime)s %(levelname)-8s [%(name)s] %(message)s")

    root = logging.getLogger("pipeline")
    root.setLevel(logging.DEBUG)
    root.handlers.clear()

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(console_fmt)
    root.addHandler(console)

    for name in ("producer", "consumer"):
        logger = logging.getLogger(f"pipeline.{name}")
        fh = logging.FileHandler(log_dir / f"{name}.log", encoding="utf-8")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(json_fmt)
        logger.addHandler(fh)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
